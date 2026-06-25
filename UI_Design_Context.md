# Smart-Grid Energy Demand Balancer — UI Design Context

> **Use this document to build the web application UI. All backend logic is Python. Keep the UI aligned with these data structures, workflows, and real-time requirements.**

---

## 1. PROBLEM STATEMENT (What the UI must communicate)

### The Core Problem
Electric power grids collapse under **simultaneous demand surges**. When millions of homes turn on air conditioning during a heatwave, total load can exceed grid capacity within minutes. Without advance warning, operators cannot:
- Spin up reserve power plants (takes 15-30 minutes)
- Activate demand response programs (smart thermostat adjustments)
- Reroute surplus renewable energy from low-demand zones

**Result:** Rolling blackouts, grid instability, equipment damage, and massive financial losses.

### The Solution (What the UI visualizes)
A **real-time predictive system** that:
1. **Collects** power readings from millions of smart meters every 10 seconds
2. **Aggregates** load by postal zip code in real-time
3. **Predicts** grid overloads **15 minutes before they happen** using acceleration trends
4. **Alerts** operators with specific actions (load shedding, reserve activation, renewable reallocation)
5. **Archives** old data nightly to cheap cold storage to control cloud costs

**Key Value Proposition for UI:** The dashboard doesn't just show current load — it shows **"minutes until overload"** and **recommended emergency actions**.

---

## 2. SYSTEM ARCHITECTURE (What the UI connects to)

```
┌─────────────────────────────────────────────────────────────────┐
│                        USER INTERFACE (Web App)                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │  Live Grid   │  │  Alert Panel │  │  Zone Control Map    │  │
│  │  Load Chart  │  │  (Actions)   │  │  (Zip Code Status)   │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │  Prediction  │  │  Renewable   │  │  Historical /        │  │
│  │  Timeline    │  │  Reallocation│  │  Cost Analytics      │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼ WebSocket / REST API
┌─────────────────────────────────────────────────────────────────┐
│                    PYTHON BACKEND (Your Code)                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │  Kafka       │  │  Spark       │  │  SQL Predictive      │  │
│  │  Producer    │  │  Streaming   │  │  Engine              │  │
│  │  (Simulator) │  │  (Aggregations)│  │  (Window Functions) │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │  Airflow     │  │  Azure Blob  │  │  Alert WebSocket     │  │
│  │  DAG (Nightly)│  │  Cold Storage│  │  Server (Python)     │  │
│  │  Archival    │  │  (Parquet)   │  │  (FastAPI/Flask)     │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

**UI connects to Python backend via:**
- **WebSocket** for real-time alerts and grid metrics (10-second refresh)
- **REST API** for historical data, predictions, and manual actions (load shedding triggers)

---

## 3. KEY DATA STRUCTURES (What the UI displays)

### 3.1 Smart Meter Reading (Raw Event — every 10 seconds)
```python
{
    "meter_id": "meter_0000001234",
    "zip_code": "90210",
    "timestamp": "2026-06-25T14:30:00Z",
    "power_kw": 4.523,           # Current power draw in kilowatts
    "voltage": 240.1,            # Line voltage
    "frequency": 60.02,          # Grid frequency (Hz)
    "temperature_f": 102.5,      # Local temperature (heatwave indicator)
    "grid_stress_indicator": 0.85,  # 0-1 scale, higher = more stress
    "reading_interval_sec": 10
}
```

### 3.2 Zip Code Aggregation (10-second window)
```python
{
    "window_start": "2026-06-25T14:30:00Z",
    "window_end": "2026-06-25T14:30:10Z",
    "zip_code": "90210",
    "load_mw": 245.8,            # Total load in megawatts for this zip
    "avg_load_kw": 2.45,         # Average per meter
    "meter_count": 100320,       # Active meters in this zip
    "peak_load_kw": 18.7,        # Highest single meter reading
    "load_stddev": 1.23,         # Load variance (instability indicator)
    "avg_voltage": 239.8,
    "avg_frequency": 59.98,
    "avg_temp_f": 101.2,
    "avg_stress": 0.82
}
```

### 3.3 Overload Prediction (Critical for UI — 15-minute forecast)
```python
{
    "zip_code": "90210",
    "window_start": "2026-06-25T14:30:00Z",
    "current_load_kw": 245800,   # Current total load in kW
    "load_acceleration_mw_per_min": 12.5,  # How fast load is increasing
    "projected_15min_load": 433000,       # Predicted load in 15 minutes (kW)
    "capacity_utilization_pct": 87.3,       # % of zip capacity used
    "growth_regime": "EXPLOSIVE_GROWTH",    # STABLE / MODERATE / RAPID / EXPLOSIVE
    "estimated_minutes_to_overload": 8.5,  # ⭐ KEY UI METRIC
    "overload_risk_score": 87.5,            # 0-100 risk score
    "alert_level": "CRITICAL",              # LOW / MEDIUM / HIGH / CRITICAL
    "recommended_action": "INITIATE_LOAD_SHEDDING: Reduce non-essential loads immediately"
}
```

### 3.4 Grid-Wide Metrics (Dashboard Header)
```python
{
    "total_grid_load_mw": 12450.5,    # Entire grid load
    "active_meters": 4982301,         # Meters reporting
    "single_peak_kw": 24.7,           # Highest single meter
    "avg_temp_f": 98.4,               # Grid-wide average temp
    "heatwave_active": True,          # Boolean flag
    "capacity_pct": 78.5,             # Overall grid capacity used
    "renewable_surplus_mw": 450.2,    # Excess solar/wind available
    "renewable_deficit_zones": 3      # Zones needing power
}
```

### 3.5 Alert Object (WebSocket Push)
```python
{
    "type": "ALERT",
    "timestamp": "2026-06-25T14:30:05Z",
    "payload": {
        "zip_code": "90210",
        "alert_level": "CRITICAL",
        "overload_risk_score": 87.5,
        "estimated_minutes_to_overload": 8.5,
        "recommended_action": "INITIATE_LOAD_SHEDDING: Reduce non-essential loads immediately",
        "predicted_15min": 433000,
        "current_load_kw": 245800
    }
}
```

### 3.6 Renewable Reallocation Command (UI Action Result)
```python
{
    "zip_code": "90210",
    "grid_role": "IMPORT_ZONE",       # EXPORT_ZONE / IMPORT_ZONE / BALANCED_ZONE
    "current_load": 245.8,
    "predicted_load": 433.0,
    "risk_score": 87.5,
    "surplus_mw": 0.0,
    "deficit_mw": 187.2,
    "priority_score": 145.6,
    "grid_command": "REQUEST 187.2 MW FROM NEAREST EXPORT ZONE"
}
```

---

## 4. UI SCREEN REQUIREMENTS (What pages/views you need)

### 4.1 MAIN DASHBOARD (Primary View — Real-Time Operations)

**Layout:** Single-page dashboard with 4-6 widget panels, auto-refreshing every 10 seconds.

**Required Widgets:**

| Widget | Data Source | Refresh Rate | Key Visual |
|--------|-------------|--------------|------------|
| **Grid Load Chart** | Grid-wide metrics stream | 10s | Area chart: load (blue) vs capacity limit (red dashed) |
| **Alert Panel** | WebSocket alert stream | Real-time | Scrollable list, color-coded by severity, action buttons |
| **Zone Map** | Zip code aggregations | 10s | Grid of zip code cards, color by capacity % |
| **Prediction Timeline** | Predictions table | 10s | Horizontal bar: "8.5 min until overload" countdown |
| **Weather Correlation** | Grid-wide temp | 10s | Temperature gauge, heatwave indicator |
| **Renewable Balance** | Reallocation optimizer | 10s | Surplus vs deficit zones, flow arrows |

**Header Bar:**
- Total Grid Load (MW) — large number
- Heatwave Status (🔥 indicator)
- System Health (green/yellow/red)
- Operator Name / Shift Info
- Timestamp of last data refresh

**Alert Panel Design:**
```
┌─────────────────────────────────────────────┐
│ 🔴 CRITICAL  ZIP 90210    8.5 min to overload│
│ Load: 245.8 MW → 433.0 MW (predicted)       │
│ Action: INITIATE LOAD SHEDDING              │
│ [EXECUTE SHEDDING] [SPIN UP RESERVES]       │
├─────────────────────────────────────────────┤
│ 🟠 HIGH      ZIP 77001    14 min to overload│
│ Load: 189.2 MW → 298.5 MW (predicted)       │
│ Action: ACTIVATE DEMAND RESPONSE            │
│ [SEND DR SIGNAL] [MORE INFO]                │
└─────────────────────────────────────────────┘
```

**Zone Map Card Design:**
```
┌──────────────┐
│ ZIP 90210    │
│ 245.8 MW     │
│ ████████░░   │  ← capacity bar (87% = red)
│ 101.2°F ↑    │  ← temp + trend arrow
│ 🔴 CRITICAL  │
│ 8.5 min left │
└──────────────┘
```

---

### 4.2 PREDICTIVE ANALYTICS VIEW (Deep Dive)

**Purpose:** Show how the 15-minute prediction is calculated. For operators and engineers.

**Required Elements:**
- **Time-series chart** with 3 lines:
  - Actual load (solid blue)
  - Predicted load (dashed orange)
  - Capacity limit (red horizontal line)
- **Acceleration gauge:** Current rate of load increase (MW/min)
- **Prediction confidence:** Based on historical accuracy
- **Window function breakdown:** Show the LAG/LEAD calculations visually
- **Heatwave overlay:** Temperature correlation on secondary Y-axis

**Data Query (SQL behind this view):**
```sql
SELECT 
    window_start,
    minute_load_kw,
    LAG(minute_load_kw, 1) OVER (PARTITION BY zip_code ORDER BY window_start) as prev_load,
    LAG(minute_load_kw, 5) OVER (PARTITION BY zip_code ORDER BY window_start) as load_5min_ago,
    SUM(minute_load_kw) OVER (PARTITION BY zip_code ORDER BY window_start ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) as cumulative_15min,
    (minute_load_kw - LAG(minute_load_kw, 5) OVER (...)) / 5.0 as trend_slope,
    minute_load_kw + ((minute_load_kw - LAG(minute_load_kw, 5) OVER (...)) / 5.0 * 15) as predicted_15min
FROM grid.minute_loads
WHERE zip_code = '90210'
ORDER BY window_start DESC
LIMIT 100;
```

---

### 4.3 RENEWABLE REALLOCATION CONTROL (Action Center)

**Purpose:** Allow operators to manually approve or override automatic renewable energy routing.

**Required Elements:**
- **Map/Grid view** of all zones with color coding:
  - 🟢 EXPORT zones (surplus renewable generation)
  - 🔴 IMPORT zones (deficit, need power)
  - ⚪ BALANCED zones
- **Flow arrows** showing recommended power transfers
- **Transfer amount editor:** Adjust MW to transfer between zones
- **Execute button:** Confirm reallocation command
- **Impact preview:** Show predicted effect on receiving zone's minutes-to-overload

**Data Structure for Transfer:**
```python
{
    "source_zip": "98101",      # Export zone (Seattle, surplus wind)
    "target_zip": "90210",      # Import zone (LA, heatwave deficit)
    "transfer_mw": 150.0,
    "predicted_impact": {
        "target_new_load_mw": 295.8,      # 245.8 + 150 transferred away
        "target_new_minutes_to_overload": 22.5,  # Was 8.5, now 22.5
        "target_new_risk_score": 45.2       # Was 87.5, now 45.2
    }
}
```

---

### 4.4 HISTORICAL ANALYTICS & REPORTING (Batch View)

**Purpose:** Post-event analysis, regulatory reporting, cost tracking.

**Required Elements:**
- **Date range picker** for historical data
- **Event replay:** Step through a past heatwave event minute-by-minute
- **Cost savings chart:** Hot vs cold storage costs over time
- **Alert response time:** How fast operators acted on predictions
- **Prediction accuracy report:** Actual vs predicted load scatter plot
- **Export to PDF/CSV** for regulatory compliance

**Data Source:** Delta Lake historical tables + Azure Blob cold storage (queried via Spark)

---

### 4.5 ADMIN / CONFIGURATION PANEL

**Purpose:** System configuration, user management, alert thresholds.

**Required Elements:**
- **Alert threshold settings:**
  - CRITICAL: minutes_to_overload < 5
  - HIGH: minutes_to_overload < 15
  - MEDIUM: minutes_to_overload < 30
- **Zip code capacity limits:** Edit MW capacity per zone
- **Simulation controls:**
  - Trigger heatwave (for testing)
  - Adjust number of simulated meters
  - Pause/resume simulator
- **User roles:** Operator (view + basic actions), Engineer (view + config), Admin (full access)
- **API key management** for external integrations

---

## 5. WEBSOCKET API SPEC (UI ↔ Python Backend)

### Connection
```
wss://grid-api.utility.com/ws/alerts?token={JWT_TOKEN}
```

### Incoming Messages (Server → UI)

**Type: METRICS (every 10 seconds)**
```json
{
    "type": "METRICS",
    "timestamp": "2026-06-25T14:30:10Z",
    "payload": {
        "total_grid_load_mw": 12450.5,
        "active_meters": 4982301,
        "avg_temp_f": 98.4,
        "heatwave_active": true,
        "capacity_pct": 78.5,
        "renewable_surplus_mw": 450.2,
        "renewable_deficit_zones": 3
    }
}
```

**Type: ALERT (pushed immediately when triggered)**
```json
{
    "type": "ALERT",
    "timestamp": "2026-06-25T14:30:05Z",
    "payload": {
        "zip_code": "90210",
        "alert_level": "CRITICAL",
        "overload_risk_score": 87.5,
        "estimated_minutes_to_overload": 8.5,
        "recommended_action": "INITIATE_LOAD_SHEDDING",
        "predicted_15min": 433000,
        "current_load_kw": 245800
    }
}
```

**Type: ZONE_UPDATE (every 10 seconds)**
```json
{
    "type": "ZONE_UPDATE",
    "timestamp": "2026-06-25T14:30:10Z",
    "payload": [
        {
            "zip_code": "90210",
            "current_load_mw": 245.8,
            "capacity_utilization_pct": 87.3,
            "avg_temp_f": 101.2,
            "meter_count": 100320,
            "trend": "UP",
            "alert_level": "CRITICAL"
        },
        ...
    ]
}
```

### Outgoing Messages (UI → Server)

**Type: EXECUTE_SHEDDING**
```json
{
    "type": "EXECUTE_SHEDDING",
    "zip_code": "90210",
    "priority": "CRITICAL",
    "estimated_recovery_minutes": 30,
    "operator_id": "op_12345",
    "timestamp": "2026-06-25T14:30:15Z"
}
```

**Type: REALLOCATE_POWER**
```json
{
    "type": "REALLOCATE_POWER",
    "source_zip": "98101",
    "target_zip": "90210",
    "transfer_mw": 150.0,
    "operator_id": "op_12345",
    "timestamp": "2026-06-25T14:30:15Z"
}
```

**Type: TRIGGER_HEATWAVE (Admin only)**
```json
{
    "type": "TRIGGER_HEATWAVE",
    "duration_hours": 4,
    "operator_id": "admin_001",
    "timestamp": "2026-06-25T14:30:15Z"
}
```

---

## 6. REST API ENDPOINTS (UI ↔ Python Backend)

### Historical Data
```
GET /api/v1/historical/load?zip_code={zip}&start={ISO}&end={ISO}&granularity=1min
GET /api/v1/historical/predictions?zip_code={zip}&start={ISO}&end={ISO}
GET /api/v1/historical/alerts?start={ISO}&end={ISO}&severity={CRITICAL|HIGH|MEDIUM}
GET /api/v1/historical/events?event_id={heatwave_2026_06_25}  # Replay event
```

### Analytics
```
GET /api/v1/analytics/prediction-accuracy?days=30
GET /api/v1/analytics/cost-savings?month=2026-06
GET /api/v1/analytics/response-times?days=7
GET /api/v1/analytics/renewable-utilization?start={ISO}&end={ISO}
```

### Configuration
```
GET /api/v1/config/alert-thresholds
PUT /api/v1/config/alert-thresholds
GET /api/v1/config/zip-capacities
PUT /api/v1/config/zip-capacities/{zip_code}
```

### Simulation Control
```
POST /api/v1/sim/heatwave/trigger
POST /api/v1/sim/heatwave/cancel
POST /api/v1/sim/meters/adjust-count
GET /api/v1/sim/status
```

---

## 7. PYTHON BACKEND STACK (What powers the UI)

| Layer | Technology | Role |
|-------|-----------|------|
| **Web Framework** | FastAPI or Flask | REST API endpoints |
| **WebSocket Server** | FastAPI WebSocket or Flask-SocketIO | Real-time push to UI |
| **Database** | Delta Lake (Databricks) | Hot data (last 30 days) |
| **Cold Storage** | Azure Blob Storage (Parquet) | Historical data |
| **Query Engine** | Spark SQL (via Databricks Connect) | Query Delta tables |
| **Caching** | Redis | Cache recent aggregations for UI speed |
| **Authentication** | JWT tokens | Operator login & role-based access |
| **Task Queue** | Celery + Redis | Async background jobs |

**Python Dependencies:**
```
fastapi==0.111.0
uvicorn[standard]==0.30.0
websockets==12.0
pyspark==3.5.0
delta-spark==3.1.0
azure-storage-blob==12.20.0
redis==5.0.0
celery==5.4.0
python-jose[cryptography]==3.3.0
pydantic==2.7.0
sqlalchemy==2.0.0
```

---

## 8. UI DESIGN PRINCIPLES (Critical for accuracy)

### 8.1 Real-Time Indicators
- **Every data element must show "last updated" timestamp**
- **Stale data (>30 seconds old) must be visually flagged** (grayed out, warning icon)
- **WebSocket disconnect must show prominent reconnection banner**

### 8.2 Alert Hierarchy
```
CRITICAL  → Red background, pulsing animation, sound alarm, requires acknowledgment
HIGH      → Orange background, solid, desktop notification
MEDIUM    → Yellow background, no animation, log only
LOW       → Green/gray, informational
```

### 8.3 Action Buttons
- **CRITICAL alerts must have ONE-CLICK action buttons** (no confirmation dialog — speed matters)
- **Actions must show loading state** while Python backend processes
- **Success/failure must be confirmed** with toast notification

### 8.4 Color Coding
```
Capacity Utilization:
  0-60%   → Green  (#22c55e)
  60-80%  → Yellow (#eab308)
  80-95%  → Orange (#f97316)
  95-100% → Red    (#dc2626) + pulse animation

Trend Arrows:
  ↑       → Red (load increasing)
  ↓       → Green (load decreasing)
  →       → Gray (stable)

Temperature:
  <85°F   → Blue
  85-95°F → Yellow
  >95°F   → Red (heatwave indicator)
```

### 8.5 Mobile Responsiveness
- **Alert panel must be accessible on mobile** (operators may be in the field)
- **Critical alerts must trigger push notifications** on mobile
- **Zone map can simplify to list view on small screens**

---

## 9. USER PERSONAS & WORKFLOWS

### Persona 1: Grid Control Room Operator
**Goal:** Prevent blackouts in real-time
**Primary View:** Main Dashboard
**Workflow:**
1. Monitor grid load chart and zone map
2. Receive CRITICAL alert (sound + visual)
3. Click "EXECUTE LOAD SHEDDING" on alert panel
4. Confirm action was successful (toast notification)
5. Monitor zone status until it returns to green

### Persona 2: Grid Operations Engineer
**Goal:** Optimize prediction accuracy and response protocols
**Primary View:** Predictive Analytics + Historical Reporting
**Workflow:**
1. Review prediction accuracy report
2. Adjust alert thresholds in Admin panel
3. Analyze past heatwave event replay
4. Modify SQL window function parameters
5. Test changes with simulator heatwave trigger

### Persona 3: Utility Executive / Regulator
**Goal:** Compliance reporting and cost analysis
**Primary View:** Historical Analytics + Cost Dashboard
**Workflow:**
1. Generate monthly cost savings report
2. Export PDF for board meeting
3. Review alert response time compliance
4. Verify renewable reallocation effectiveness

---

## 10. TESTING SCENARIOS (For UI validation)

### Scenario 1: Normal Operations
- Load: 60-70% capacity
- Temperature: 75°F
- Expected UI: All green, no alerts, steady charts

### Scenario 2: Heatwave Event (Simulated)
- Trigger: Admin clicks "Trigger Heatwave" or simulator auto-triggers
- Load: Rapid increase to 85-95% capacity
- Temperature: 100-110°F
- Expected UI:
  - Heatwave banner appears (🔥 icon)
  - Zone cards turn yellow then orange
  - Alert panel populates with HIGH alerts
  - Grid load chart shows steep upward curve
  - Prediction timeline shows decreasing minutes-to-overload

### Scenario 3: Critical Overload Imminent
- Trigger: Load acceleration continues for 10+ minutes
- Load: 95%+ capacity, minutes-to-overload < 5
- Expected UI:
  - Zone cards turn red with pulse animation
  - CRITICAL alerts with sound alarm
  - One-click "EXECUTE LOAD SHEDDING" button
  - Prediction timeline shows "< 5 MIN" in large red text
  - Renewable reallocation panel shows available surplus

### Scenario 4: Load Shedding Executed
- Trigger: Operator clicks "EXECUTE LOAD SHEDDING"
- Result: Load drops by 15-20% within 2 minutes
- Expected UI:
  - Zone card shows load dropping (animated bar)
  - Alert downgrades from CRITICAL to HIGH
  - Minutes-to-overload increases
  - Historical log records the action with operator ID

### Scenario 5: Renewable Reallocation
- Trigger: Operator approves power transfer from export zone to import zone
- Result: Import zone load decreases, export zone surplus decreases
- Expected UI:
  - Flow arrow animation between zones
  - Impact preview shows before/after metrics
  - Both zone cards update with new load values
  - Success toast: "Transfer of 150 MW from 98101 to 90210 complete"

---

## 11. PERFORMANCE REQUIREMENTS (UI-Side)

| Requirement | Target | Why |
|-------------|--------|-----|
| **Initial Load** | < 3 seconds | Operators need immediate situational awareness |
| **WebSocket Connect** | < 1 second | Real-time alerts must not be delayed |
| **Chart Render** | < 500ms for 1000 data points | Smooth scrolling during events |
| **Alert Animation** | < 100ms | Visual urgency for critical alerts |
| **Action Response** | < 2 seconds (button click → backend ack) | Emergency actions need fast feedback |
| **Mobile Load** | < 5 seconds | Field operators may have slower connections |
| **Concurrent Users** | 100+ operators | Large utility with multiple shifts |

---

## 12. SECURITY REQUIREMENTS

- **JWT Authentication** for all WebSocket and REST connections
- **Role-Based Access Control (RBAC):**
  - `operator`: View dashboard, execute shedding, approve reallocations
  - `engineer`: + View analytics, modify thresholds, trigger simulations
  - `admin`: + Full config access, user management, API keys
- **Audit Logging:** Every action (shedding, reallocation, config change) logged with operator ID, timestamp, and result
- **HTTPS/WSS only** — no unencrypted connections
- **Rate limiting:** Max 10 actions per minute per operator (prevent accidental double-clicks)

---

## 13. DEPLOYMENT NOTES (For UI Developer)

### Recommended UI Stack
| Layer | Recommendation | Alternative |
|-------|---------------|-------------|
| **Framework** | React 18 + TypeScript | Vue 3 + TypeScript |
| **State Management** | Zustand or React Query | Redux Toolkit |
| **Charts** | Recharts or Apache ECharts | D3.js (custom) |
| **Maps** | Leaflet (zip code choropleth) | Mapbox GL |
| **WebSocket** | Native WebSocket API | Socket.io client |
| **Styling** | Tailwind CSS | Styled Components |
| **Build Tool** | Vite | Next.js (if SSR needed) |
| **Testing** | Vitest + React Testing Library | Jest |

### Environment Variables
```env
VITE_API_BASE_URL=https://grid-api.utility.com
VITE_WS_URL=wss://grid-api.utility.com/ws/alerts
VITE_REFRESH_INTERVAL=10000
VITE_MAPBOX_TOKEN=pk.xxx
```

### Build & Deploy
```bash
npm install
npm run build
# Deploy dist/ to Azure Static Web Apps or AWS S3 + CloudFront
```

---

## 14. SUMMARY CHECKLIST FOR UI DEVELOPER

Before building, confirm you have:

- [ ] **Understood the problem:** Grid overloads during heatwaves, need 15-min prediction
- [ ] **Mapped data structures:** Meter readings → Zip aggregations → Predictions → Alerts
- [ ] **Designed 5 views:** Dashboard, Analytics, Reallocation, Historical, Admin
- [ ] **Implemented WebSocket:** Real-time alerts, metrics, zone updates
- [ ] **Implemented REST API:** Historical queries, config, simulation control
- [ ] **Applied color coding:** Green/Yellow/Orange/Red for capacity, trends, temperature
- [ ] **Added one-click actions:** Load shedding, reallocation approval (no dialogs for critical)
- [ ] **Handled edge cases:** WebSocket disconnect, stale data, mobile responsiveness
- [ ] **Tested scenarios:** Normal, heatwave, critical, shedding, reallocation
- [ ] **Secured access:** JWT auth, RBAC, audit logging, rate limiting

---

> **Document Version:** 1.0
> **Last Updated:** 2026-06-25
> **Backend Language:** Python (FastAPI/Flask + Spark + Kafka + Airflow)
> **UI Framework:** Your Choice (React/Vue recommended)
> **Primary Data Source:** Databricks Delta Lake + Azure Blob Storage
