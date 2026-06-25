# dags/grid_cold_storage_archival.py
# Airflow DAG to archive old telemetry to Azure Blob cold tier

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.microsoft.azure.operators.asb import AzureBlobStorageDeleteOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import datetime, timedelta
import logging

# Configuration
COLD_STORAGE_CONN = 'azure_blob_cold_storage'
DELTA_TABLES = {
    'raw_readings': 'grid.raw_meter_readings',
    'zip_aggregations': 'grid.zip_code_loads', 
    'predictions': 'grid.overload_predictions'
}
RETENTION_DAYS_HOT = 7      # Keep in Delta hot storage
RETENTION_DAYS_WARM = 30    # Keep in Delta warm storage  
ARCHIVE_AFTER_DAYS = 30     # Move to Azure cold blob after this

default_args = {
    'owner': 'grid-ops',
    'depends_on_past': False,
    'email': ['grid-ops@utility.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=4)
}

dag = DAG(
    'grid_telemetry_cold_archival',
    default_args=default_args,
    description='Archive old smart grid telemetry to Azure Blob cold storage',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['grid', 'storage', 'cost-optimization', 'azure'],
    max_active_runs=1
)

def get_partition_date(**context):
    """Calculate the partition date to archive (30 days ago)"""
    execution_date = context['execution_date']
    archive_date = execution_date - timedelta(days=ARCHIVE_AFTER_DAYS)
    return archive_date.strftime('%Y-%m-%d')

# ============================================
# TASK 1: Verify data quality before archival
# ============================================
def validate_data_quality(archive_date, **context):
    """Ensure data is complete before moving to cold storage"""
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.getOrCreate()
    
    for table_name, delta_path in DELTA_TABLES.items():
        # Check record count for archive date
        count = spark.sql(f"""
            SELECT COUNT(*) as cnt 
            FROM {delta_path}
            WHERE DATE(window_start) = '{archive_date}'
        """).collect()[0]['cnt']
        
        # Alert if suspiciously low (data loss check)
        expected_min = 1000 if 'raw' in table_name else 100
        if count < expected_min:
            raise ValueError(
                f"Data quality alert: {table_name} only has {count} records "
                f"for {archive_date}. Expected at least {expected_min}."
            )
        
        logging.info(f"✅ {table_name}: {count:,} records validated for {archive_date}")

validate_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    op_kwargs={'archive_date': '{{ ti.xcom_pull(task_ids="get_archive_date") }}'},
    provide_context=True,
    dag=dag
)

# ============================================
# TASK 2: Compact Delta files before export
# ============================================
def optimize_delta_for_export(archive_date, **context):
    """Optimize and Z-ORDER Delta table for efficient export"""
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.getOrCreate()
    
    for table_name, delta_path in DELTA_TABLES.items():
        # Optimize files for the archive partition
        spark.sql(f"""
            OPTIMIZE {delta_path}
            WHERE DATE(window_start) = '{archive_date}'
            ZORDER BY (zip_code)
        """)
        
        # Vacuum old versions (keep only latest for archive)
        spark.sql(f"""
            SET spark.databricks.delta.retentionDurationCheck.enabled = false;
            VACUUM {delta_path} RETAIN 0 HOURS;
        """)
        
        logging.info(f"🔧 Optimized {table_name} for export")

optimize_task = PythonOperator(
    task_id='optimize_delta_tables',
    python_callable=optimize_delta_for_export,
    op_kwargs={'archive_date': '{{ ti.xcom_pull(task_ids="get_archive_date") }}'},
    provide_context=True,
    dag=dag
)

# ============================================
# TASK 3: Export to Parquet and upload to Azure Blob Cold Tier
# ============================================
def export_to_cold_storage(archive_date, **context):
    """Export Delta data to compressed Parquet and upload to Azure Blob"""
    from pyspark.sql import SparkSession
    from azure.storage.blob import BlobServiceClient
    import tempfile
    import os
    
    spark = SparkSession.builder.getOrCreate()
    
    # Azure Blob configuration
    blob_conn_str = Variable.get('azure_blob_connection_string')
    container_name = 'grid-cold-storage'
    
    blob_service = BlobServiceClient.from_connection_string(blob_conn_str)
    container = blob_service.get_container_client(container_name)
    
    for table_name, delta_path in DELTA_TABLES.items():
        # Read archive partition
        df = spark.sql(f"""
            SELECT * FROM {delta_path}
            WHERE DATE(window_start) = '{archive_date}'
        """)
        
        # Write to temporary compressed Parquet
        temp_dir = tempfile.mkdtemp()
        parquet_path = f"{temp_dir}/{table_name}"
        
        df.coalesce(10).write \
            .mode('overwrite') \
            .parquet(parquet_path, compression='zstd')  # ZSTD for best compression
        
        # Upload to Azure Blob with cold tier
        for root, dirs, files in os.walk(parquet_path):
            for file in files:
                if file.endswith('.parquet'):
                    local_path = os.path.join(root, file)
                    blob_path = f"{archive_date}/{table_name}/{file}"
                    
                    blob_client = container.get_blob_client(blob_path)
                    
                    with open(local_path, 'rb') as data:
                        blob_client.upload_blob(
                            data, 
                            overwrite=True,
                            standard_blob_tier='Archive'  # Cheapest tier
                        )
                    
                    logging.info(f"📦 Uploaded {blob_path} to Archive tier")
        
        # Cleanup temp
        import shutil
        shutil.rmtree(temp_dir)
        
        # Record metadata
        record_count = df.count()
        context['ti'].xcom_push(
            key=f'{table_name}_records',
            value=record_count
        )

export_task = PythonOperator(
    task_id='export_to_azure_blob',
    python_callable=export_to_cold_storage,
    op_kwargs={'archive_date': '{{ ti.xcom_pull(task_ids="get_archive_date") }}'},
    provide_context=True,
    dag=dag
)

# ============================================
# TASK 4: Delete from Delta hot storage
# ============================================
def purge_hot_storage(archive_date, **context):
    """Remove archived data from expensive hot Delta storage"""
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.getOrCreate()
    
    for table_name, delta_path in DELTA_TABLES.items():
        # Delete archived partition
        spark.sql(f"""
            DELETE FROM {delta_path}
            WHERE DATE(window_start) = '{archive_date}'
        """)
        
        # Log savings
        records_deleted = context['ti'].xcom_pull(
            task_ids='export_to_azure_blob',
            key=f'{table_name}_records'
        )
        
        logging.info(
            f"🗑️ Purged {records_deleted:,} records from {table_name} "
            f"hot storage for {archive_date}"
        )

purge_task = PythonOperator(
    task_id='purge_hot_storage',
    python_callable=purge_hot_storage,
    op_kwargs={'archive_date': '{{ ti.xcom_pull(task_ids="get_archive_date") }}'},
    provide_context=True,
    dag=dag
)

# ============================================
# TASK 5: Cost reporting
# ============================================
def generate_cost_report(**context):
    """Calculate storage cost savings"""
    archive_date = context['ti'].xcom_pull(task_ids='get_archive_date')
    
    # Azure Hot vs Archive pricing (simplified)
    # Hot: $0.0184/GB/month | Archive: $0.00099/GB/month
    hot_cost_per_gb = 0.0184
    archive_cost_per_gb = 0.00099
    
    # Estimate based on typical compression ratios
    estimated_gb_saved = 500  # Adjust based on actual metrics
    
    monthly_savings = estimated_gb_saved * (hot_cost_per_gb - archive_cost_per_gb)
    annual_savings = monthly_savings * 12
    
    report = f"""
    ╔══════════════════════════════════════════════════════╗
    ║     GRID COLD STORAGE ARCHIVAL REPORT                ║
    ╠══════════════════════════════════════════════════════╣
    ║  Archive Date: {archive_date}                        ║
    ║  Data Moved:  ~{estimated_gb_saved} GB               ║
    ║  Monthly Savings: ${monthly_savings:.2f}             ║
    ║  Annual Savings:  ${annual_savings:.2f}              ║
    ║  Storage Tier: Azure Blob Archive (lowest cost)      ║
    ╚══════════════════════════════════════════════════════╝
    """
    
    logging.info(report)
    return report

cost_report_task = PythonOperator(
    task_id='generate_cost_report',
    python_callable=generate_cost_report,
    provide_context=True,
    dag=dag
)

# ============================================
# TASK DEPENDENCIES
# ============================================
get_date_task = PythonOperator(
    task_id='get_archive_date',
    python_callable=lambda **ctx: (ctx['execution_date'] - timedelta(days=ARCHIVE_AFTER_DAYS)).strftime('%Y-%m-%d'),
    provide_context=True,
    dag=dag
)

get_date_task >> validate_task >> optimize_task >> export_task >> purge_task >> cost_report_task
 
