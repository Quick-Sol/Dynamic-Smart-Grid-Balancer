// GridControlDashboard.tsx
// Real-time grid operations center

import React, { useState, useEffect, useCallback } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, AreaChart, Area } from 'recharts';
import { AlertTriangle, Zap, Thermometer, Activity, MapPin } from 'lucide-react';
import { Kafka } from 'kafkajs';

interface GridAlert {
  zip_code: string;
  alert_level: 'CRITICAL' | 'HIGH' | 'MEDIUM' | 'LOW';
  overload_risk_score: number;
  estimated_minutes_to_overload: number;
  recommended_action: string;
  predicted_15min: number;
}

interface ZipMetrics {
  zip_code: string;
  current_load_mw: number;
  capacity_utilization_pct: number;
  avg_temp_f: number;
  meter_count: number;
  trend: 'UP' | 'DOWN' | 'STABLE';
}

const GridControlDashboard: React.FC = () => {
  const [alerts, setAlerts] = useState<GridAlert[]>([]);
  const [zipMetrics, setZipMetrics] = useState<ZipMetrics[]>([]);
  const [gridWideLoad, setGridWideLoad] = useState<number>(0);
  const [heatwaveActive, setHeatwaveActive] = useState(false);
  const [historicalData, setHistoricalData] = useState<any[]>([]);

  // WebSocket connection to Kafka consumer
  useEffect(() => {
    const ws = new WebSocket('wss://grid-api.utility.com/ws/alerts');
    
    ws.onmessage = (event) => {
      const data = JSON.parse(event.data);
      
      if (data.type === 'ALERT') {
        setAlerts(prev => [data.payload, ...prev].slice(0, 50));
      } else if (data.type === 'METRICS') {
        setZipMetrics(data.payload);
      } else if (data.type === 'GRID_WIDE') {
        setGridWideLoad(data.payload.total_load_mw);
        setHeatwaveActive(data.payload.avg_temp_f > 95);
        
        setHistoricalData(prev => [
          ...prev.slice(-100),
          {
            time: new Date().toLocaleTimeString(),
            load: data.payload.total_load_mw,
            temp: data.payload.avg_temp_f,
            capacity: data.payload.capacity_pct
          }
        ]);
      }
    };

    return () => ws.close();
  }, []);

  const getAlertColor = (level: string) => {
    switch (level) {
      case 'CRITICAL': return 'bg-red-600 animate-pulse';
      case 'HIGH': return 'bg-orange-500';
      case 'MEDIUM': return 'bg-yellow-500';
      default: return 'bg-green-500';
    }
  };

  const initiateLoadShedding = useCallback(async (zipCode: string) => {
    // API call to grid control system
    await fetch('/api/grid/emergency-shed', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        zip_code: zipCode,
        action: 'IMMEDIATE_SHED',
        priority: 'CRITICAL',
        estimated_recovery_minutes: 30
      })
    });
  }, []);

  return (
    <div className="min-h-screen bg-gray-900 text-white p-6">
      {/* Header */}
      <div className="mb-8 flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold flex items-center gap-3">
            <Zap className="text-yellow-400" />
            Smart Grid Demand Balancer
          </h1>
          <p className="text-gray-400 mt-1">Real-time Load Management & Predictive Analytics</p>
        </div>
        
        <div className="flex items-center gap-4">
          <div className={`px-4 py-2 rounded-lg font-bold ${heatwaveActive ? 'bg-red-600' : 'bg-green-600'}`}>
            <Thermometer className="inline mr-2" />
            {heatwaveActive ? '🔥 HEATWAVE ACTIVE' : 'Normal Weather'}
          </div>
          <div className="text-right">
            <div className="text-2xl font-mono font-bold">{gridWideLoad.toFixed(1)} MW</div>
            <div className="text-sm text-gray-400">Total Grid Load</div>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-12 gap-6">
        {/* Live Grid Load Chart */}
        <div className="col-span-8 bg-gray-800 rounded-xl p-6">
          <h2 className="text-xl font-bold mb-4 flex items-center gap-2">
            <Activity className="text-blue-400" />
            Real-Time Grid Load & Capacity
          </h2>
          <ResponsiveContainer width="100%" height={300}>
            <AreaChart data={historicalData}>
              <defs>
                <linearGradient id="loadGradient" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.8}/>
                  <stop offset="95%" stopColor="#3b82f6" stopOpacity={0}/>
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="time" stroke="#9ca3af" />
              <YAxis stroke="#9ca3af" />
              <Tooltip 
                contentStyle={{ backgroundColor: '#1f2937', border: 'none', borderRadius: '8px' }}
              />
              <Legend />
              <Area 
                type="monotone" 
                dataKey="load" 
                stroke="#3b82f6" 
                fillOpacity={1} 
                fill="url(#loadGradient)" 
                name="Load (MW)"
              />
              <Area 
                type="monotone" 
                dataKey="capacity" 
                stroke="#ef4444" 
                fill="none" 
                strokeDasharray="5 5"
                name="Capacity Limit"
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>

        {/* Critical Alerts Panel */}
        <div className="col-span-4 bg-gray-800 rounded-xl p-6">
          <h2 className="text-xl font-bold mb-4 flex items-center gap-2">
            <AlertTriangle className="text-red-400" />
            Active Alerts
          </h2>
          <div className="space-y-3 max-h-[300px] overflow-y-auto">
            {alerts.length === 0 && (
              <div className="text-gray-500 text-center py-8">No active alerts</div>
            )}
            {alerts.map((alert, idx) => (
              <div 
                key={idx}
                className={`p-4 rounded-lg border-l-4 ${getAlertColor(alert.alert_level)} bg-opacity-20 bg-gray-700`}
              >
                <div className="flex justify-between items-start">
                  <div>
                    <div className="font-bold flex items-center gap-2">
                      <MapPin size={16} />
                      ZIP {alert.zip_code}
                    </div>
                    <div className="text-sm mt-1">{alert.recommended_action}</div>
                  </div>
                  <div className="text-right">
                    <div className="text-2xl font-bold">{alert.estimated_minutes_to_overload}m</div>
                    <div className="text-xs text-gray-400">to overload</div>
                  </div>
                </div>
                
                {alert.alert_level === 'CRITICAL' && (
                  <button
                    onClick={() => initiateLoadShedding(alert.zip_code)}
                    className="mt-3 w-full bg-red-600 hover:bg-red-700 text-white py-2 rounded font-bold transition-colors"
                  >
                    INITIATE LOAD SHEDDING
                  </button>
                )}
              </div>
            ))}
          </div>
        </div>

        {/* Zip Code Grid Map */}
        <div className="col-span-12 bg-gray-800 rounded-xl p-6">
          <h2 className="text-xl font-bold mb-4">Zone Status Map</h2>
          <div className="grid grid-cols-6 gap-4">
            {zipMetrics.map(zip => (
              <div 
                key={zip.zip_code}
                className={`p-4 rounded-lg border-2 transition-all hover:scale-105 cursor-pointer
                  ${zip.capacity_utilization_pct > 80 ? 'border-red-500 bg-red-900/30' :
                    zip.capacity_utilization_pct > 60 ? 'border-yellow-500 bg-yellow-900/30' :
                    'border-green-500 bg-green-900/30'}
                `}
              >
                <div className="font-bold text-lg">{zip.zip_code}</div>
                <div className="text-2xl font-mono">{zip.current_load_mw.toFixed(1)} MW</div>
                <div className="text-sm text-gray-400">{zip.capacity_utilization_pct.toFixed(1)}% capacity</div>
                <div className="text-sm mt-1 flex items-center gap-1">
                  <Thermometer size={14} />
                  {zip.avg_temp_f}°F
                </div>
                <div className={`text-xs font-bold mt-2 ${
                  zip.trend === 'UP' ? 'text-red-400' : 
                  zip.trend === 'DOWN' ? 'text-green-400' : 'text-gray-400'
                }`}>
                  {zip.trend === 'UP' ? '▲ RISING' : zip.trend === 'DOWN' ? '▼ FALLING' : '→ STABLE'}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
};

export default GridControlDashboard;
 
