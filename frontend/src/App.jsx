import React, { useState, useEffect } from 'react';

const API_BASE = '/api';

export default function App() {
  const [activeTab, setActiveTab] = useState('pipeline');
  const [health, setHealth] = useState(null);
  const [pipelineStatus, setPipelineStatus] = useState(null);
  const [runningStep, setRunningStep] = useState(null);
  const [pipelineLogs, setPipelineLogs] = useState([]);
  
  // Data States
  const [kpis, setKpis] = useState([]);
  const [sectors, setSectors] = useState([]);
  const [airQuality, setAirQuality] = useState([]);
  const [trend, setTrend] = useState([]);
  const [customSql, setCustomSql] = useState('SELECT anio, sector_macro_nombre, emisiones_brutas_kt FROM mart_consolidado_ambiental LIMIT 10;');
  const [sqlResult, setSqlResult] = useState(null);
  const [sqlLoading, setSqlLoading] = useState(false);

  // Chat State
  const [chatMessages, setChatMessages] = useState([
    {
      role: 'ai',
      text: '¡Hola! Soy tu asistente de inteligencia ambiental impulsado por Ollama. Puedo analizar el inventario de GEI de Colombia, las estaciones de calidad del aire y generar reportes ejecutivos. ¿Qué te gustaría consultar hoy?'
    }
  ]);
  const [chatInput, setChatInput] = useState('');
  const [chatLoading, setChatLoading] = useState(false);
  const [ollamaStatus, setOllamaStatus] = useState(null);

  // Quality Audit
  const [auditResult, setAuditResult] = useState(null);
  const [auditLoading, setAuditLoading] = useState(false);

  useEffect(() => {
    fetchHealth();
    fetchStatus();
    fetchOllamaStatus();
    loadDashboardData();
  }, []);

  useEffect(() => {
    if (activeTab === 'dashboard') {
      loadDashboardData();
    }
  }, [activeTab]);

  const fetchHealth = async () => {
    try {
      const res = await fetch(`${API_BASE}/health`);
      if (res.ok) setHealth(await res.json());
    } catch {
      setHealth({ status: 'offline' });
    }
  };

  const fetchStatus = async () => {
    try {
      const res = await fetch(`${API_BASE}/pipeline/status`);
      if (res.ok) setPipelineStatus(await res.json());
    } catch (err) {
      console.error(err);
    }
  };

  const fetchOllamaStatus = async () => {
    try {
      const res = await fetch(`${API_BASE}/chat/status`);
      if (res.ok) setOllamaStatus(await res.json());
    } catch {
      setOllamaStatus({ ollama_running: false });
    }
  };

  const loadDashboardData = async () => {
    try {
      const kRes = await fetch(`${API_BASE}/data/kpis`);
      if (kRes.ok) setKpis(await kRes.json());
    } catch (err) {
      console.error('Error KPIs:', err);
    }

    try {
      const sRes = await fetch(`${API_BASE}/data/sectors`);
      if (sRes.ok) setSectors(await sRes.json());
    } catch (err) {
      console.error('Error Sectores:', err);
    }

    try {
      const aRes = await fetch(`${API_BASE}/data/air-quality`);
      if (aRes.ok) setAirQuality(await aRes.json());
    } catch (err) {
      console.error('Error Calidad Aire:', err);
    }

    try {
      const tRes = await fetch(`${API_BASE}/data/trend`);
      if (tRes.ok) setTrend(await tRes.json());
    } catch (err) {
      console.error('Error Tendencias:', err);
    }
  };

  const runStep = async (step) => {
    setRunningStep(step);
    setPipelineLogs((prev) => [...prev, `[${new Date().toLocaleTimeString()}] Iniciando fase ${step.toUpperCase()}...`]);
    try {
      const res = await fetch(`${API_BASE}/pipeline/execute`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ step })
      });
      const data = await res.json();
      if (res.ok) {
        setPipelineLogs((prev) => [
          ...prev,
          `✅ [${new Date().toLocaleTimeString()}] ${data.details?.message || 'Fase completada exitosamente'} (${data.elapsed_seconds}s)`
        ]);
        fetchStatus();
        fetchHealth();
        if (step === 'gold') loadDashboardData();
      } else {
        setPipelineLogs((prev) => [
          ...prev,
          `❌ [${new Date().toLocaleTimeString()}] Error en ${step}: ${data.detail || 'Fallo desconocido'}`
        ]);
      }
    } catch (err) {
      setPipelineLogs((prev) => [...prev, `❌ [${new Date().toLocaleTimeString()}] Error de red: ${err.message}`]);
    } finally {
      setRunningStep(null);
    }
  };

  const executeSql = async () => {
    setSqlLoading(true);
    try {
      const res = await fetch(`${API_BASE}/data/sql`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query: customSql })
      });
      const data = await res.json();
      if (res.ok) {
        setSqlResult(data);
      } else {
        alert(data.detail || 'Error en consulta');
      }
    } catch (err) {
      alert('Error ejecutando SQL: ' + err.message);
    } finally {
      setSqlLoading(false);
    }
  };

  const handleSendMessage = async (textToSend) => {
    const msg = textToSend || chatInput;
    if (!msg.trim()) return;

    const newMessages = [...chatMessages, { role: 'user', text: msg }];
    setChatMessages(newMessages);
    setChatInput('');
    setChatLoading(true);

    try {
      const res = await fetch(`${API_BASE}/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: msg })
      });
      const data = await res.json();
      if (res.ok) {
        setChatMessages([...newMessages, { role: 'ai', text: data.response }]);
      } else {
        setChatMessages([
          ...newMessages,
          { role: 'ai', text: `⚠️ Error de Ollama (${res.status}): ${data.detail || 'No se pudo conectar con el modelo local. Verifica el modelo configurado en Ollama.'}` }
        ]);
      }
    } catch (err) {
      setChatMessages([
        ...newMessages,
        { role: 'ai', text: `⚠️ Error de conexión: ${err.message}. Verifica que el backend y Ollama estén activos.` }
      ]);
    } finally {
      setChatLoading(false);
    }
  };

  const runAudit = async () => {
    setAuditLoading(true);
    try {
      const res = await fetch(`${API_BASE}/reports/quality-audit`);
      if (res.ok) {
        setAuditResult(await res.json());
      }
    } catch (err) {
      console.error(err);
    } finally {
      setAuditLoading(false);
    }
  };

  return (
    <div className="app-container">
      {/* Encabezado */}
      <header className="app-header">
        <div className="header-brand">
          <span className="header-logo">🌿</span>
          <div>
            <h1 className="header-title">Green Data ETL — Medallion + Ollama AI</h1>
            <p className="header-subtitle">Inventario GEI Colombia & Calidad del Aire (Arquitectura Bronze → Silver → Gold)</p>
          </div>
        </div>

        <div className="header-status">
          <div className={`status-badge ${health?.status === 'ok' ? '' : 'offline'}`}>
            <span className="status-dot"></span>
            <span>Backend: {health?.status === 'ok' ? 'Conectado' : 'Desconectado'}</span>
          </div>

          <div className={`status-badge ${ollamaStatus?.ollama_running ? '' : 'offline'}`}>
            <span className="status-dot"></span>
            <span>Ollama: {ollamaStatus?.model_available ? `${ollamaStatus.model_name} listo` : 'Sin servicio'}</span>
          </div>
        </div>
      </header>

      {/* Navegación por pestañas */}
      <nav className="nav-tabs">
        <button
          className={`nav-tab ${activeTab === 'pipeline' ? 'active' : ''}`}
          onClick={() => setActiveTab('pipeline')}
        >
          <span className="nav-tab-icon">⚙️</span> Pipeline Medallion
        </button>
        <button
          className={`nav-tab ${activeTab === 'dashboard' ? 'active' : ''}`}
          onClick={() => setActiveTab('dashboard')}
        >
          <span className="nav-tab-icon">📊</span> Datos & KPIs Gold
        </button>
        <button
          className={`nav-tab ${activeTab === 'chat' ? 'active' : ''}`}
          onClick={() => setActiveTab('chat')}
        >
          <span className="nav-tab-icon">🤖</span> Consultas IA (Ollama)
        </button>
        <button
          className={`nav-tab ${activeTab === 'sql' ? 'active' : ''}`}
          onClick={() => setActiveTab('sql')}
        >
          <span className="nav-tab-icon">🔍</span> Explorador SQL
        </button>
        <button
          className={`nav-tab ${activeTab === 'audit' ? 'active' : ''}`}
          onClick={() => { setActiveTab('audit'); runAudit(); }}
        >
          <span className="nav-tab-icon">🛡️</span> Auditoría de Calidad
        </button>
      </nav>

      {/* Contenido Principal */}
      <main className="main-content">
        {/* PESTAÑA 1: PIPELINE PASO A PASO */}
        {activeTab === 'pipeline' && (
          <div>
            <div className="section-header">
              <div>
                <h2 className="section-title">Control de Ejecución Paso a Paso</h2>
                <p className="section-subtitle">
                  Ejecuta cada capa del proceso ETL de forma modular e independiente respetando la jerarquía Medallion.
                </p>
              </div>
            </div>

            <div className="pipeline-stepper">
              {/* Bronze */}
              <div
                className={`pipeline-step ${pipelineStatus?.bronze?.completed ? 'completed' : ''} ${runningStep === 'bronze' ? 'active' : ''}`}
                onClick={() => !runningStep && runStep('bronze')}
              >
                <span className="step-icon">🥉</span>
                <span className="step-label">Capa Bronze (Ingestión Cruda)</span>
                <span className="step-status">
                  {runningStep === 'bronze' ? 'Procesando...' : pipelineStatus?.bronze?.completed ? 'Completado' : 'Pendiente'}
                </span>
                <button className="btn btn-bronze btn-sm" disabled={runningStep !== null}>
                  {runningStep === 'bronze' ? 'Extrayendo...' : 'Ejecutar Bronze'}
                </button>
              </div>

              {/* Silver */}
              <div
                className={`pipeline-step ${pipelineStatus?.silver?.completed ? 'completed' : ''} ${runningStep === 'silver' ? 'active' : ''}`}
                onClick={() => !runningStep && runStep('silver')}
              >
                <span className="step-icon">🥈</span>
                <span className="step-label">Capa Silver (Limpieza & Normalización)</span>
                <span className="step-status">
                  {runningStep === 'silver' ? 'Procesando...' : pipelineStatus?.silver?.completed ? 'Completado' : 'Pendiente'}
                </span>
                <button className="btn btn-silver btn-sm" disabled={runningStep !== null}>
                  {runningStep === 'silver' ? 'Transformando...' : 'Ejecutar Silver'}
                </button>
              </div>

              {/* Gold */}
              <div
                className={`pipeline-step ${pipelineStatus?.gold?.completed ? 'completed' : ''} ${runningStep === 'gold' ? 'active' : ''}`}
                onClick={() => !runningStep && runStep('gold')}
              >
                <span className="step-icon">🥇</span>
                <span className="step-label">Capa Gold (Star Schema & Warehouse)</span>
                <span className="step-status">
                  {runningStep === 'gold' ? 'Procesando...' : pipelineStatus?.gold?.completed ? 'Completado' : 'Pendiente'}
                </span>
                <button className="btn btn-gold btn-sm" disabled={runningStep !== null}>
                  {runningStep === 'gold' ? 'Construyendo...' : 'Ejecutar Gold'}
                </button>
              </div>
            </div>

            {/* Logs de ejecución en tiempo real */}
            <div className="card" style={{ marginTop: '1.5rem' }}>
              <div className="card-header">
                <h3 className="card-title">📜 Registro de Operaciones ETL</h3>
                <button className="btn btn-outline btn-sm" onClick={() => setPipelineLogs([])}>Limpiar Registro</button>
              </div>
              <div style={{
                background: 'var(--slate-950)',
                padding: '1rem',
                borderRadius: 'var(--radius-sm)',
                fontFamily: 'var(--font-mono)',
                fontSize: '0.82rem',
                minHeight: '140px',
                maxHeight: '260px',
                overflowY: 'auto'
              }}>
                {pipelineLogs.length === 0 ? (
                  <span style={{ color: 'var(--text-muted)' }}>Haz clic en un botón superior para iniciar una fase del pipeline...</span>
                ) : (
                  pipelineLogs.map((log, i) => (
                    <div key={i} style={{ marginBottom: '4px', color: log.includes('❌') ? 'var(--red-400)' : log.includes('✅') ? 'var(--emerald-400)' : 'var(--text-secondary)' }}>
                      {log}
                    </div>
                  ))
                )}
              </div>
            </div>
          </div>
        )}

        {/* PESTAÑA 2: DASHBOARD & KPIS */}
        {activeTab === 'dashboard' && (
          <div>
            <div className="section-header">
              <div>
                <h2 className="section-title">Visualización de Datos Gold</h2>
                <p className="section-subtitle">Marts analíticos consolidados y métricas listas para toma de decisiones.</p>
              </div>
              <button className="btn btn-primary btn-sm" onClick={loadDashboardData}>🔄 Actualizar</button>
            </div>

            {/* Grid de KPIs */}
            {kpis.length === 0 ? (
              <div className="card" style={{ textAlign: 'center', padding: '1.5rem', marginBottom: '1.5rem', color: 'var(--text-muted)' }}>
                ℹ️ Los KPIs se cargan automáticamente desde el Data Warehouse Gold. Si acabas de iniciar, haz clic en "Actualizar" o ejecuta la fase Gold en la pestaña anterior.
              </div>
            ) : (
              <div className="kpi-grid">
                {kpis.map((kpi, idx) => {
                  const cardColors = ['gold', 'emerald', 'blue', 'red', 'amber', 'violet', 'cyan', 'rose'];
                  const theme = cardColors[idx % cardColors.length];
                  return (
                    <div key={idx} className={`kpi-card ${theme}`}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '0.4rem' }}>
                        <div className="kpi-label" style={{ margin: 0 }}>{kpi.kpi_nombre}</div>
                        {kpi.estado && (
                          <span style={{
                            fontSize: '0.65rem',
                            padding: '2px 6px',
                            borderRadius: '8px',
                            background: 'rgba(255, 255, 255, 0.08)',
                            color: 'var(--text-secondary)',
                            fontWeight: 600,
                            whiteSpace: 'nowrap'
                          }}>
                            {kpi.estado}
                          </span>
                        )}
                      </div>
                      <div className="kpi-value">
                        {kpi.valor || kpi.valor_actual || '0'}
                        {kpi.unidad && !String(kpi.valor || '').includes(kpi.unidad) && (
                          <span style={{ fontSize: '0.75rem', marginLeft: '6px', color: 'var(--text-muted)', fontWeight: 400 }}>
                            {kpi.unidad}
                          </span>
                        )}
                      </div>
                      <div className="kpi-detail">{kpi.interpretacion || kpi.descripcion}</div>
                    </div>
                  );
                })}
              </div>
            )}

            {/* Tablas de Sectores y Calidad del Aire */}
            <div className="grid-2">
              <div className="card">
                <div className="card-header">
                  <h3 className="card-title">🏭 Emisiones por Macro-Sector</h3>
                  <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>{sectors.length} registros</span>
                </div>
                <div className="data-table-container" style={{ maxHeight: '380px' }}>
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>Año</th>
                        <th>Sector</th>
                        <th>Brutas (kt)</th>
                        <th>Netas (kt)</th>
                        <th>% Part.</th>
                      </tr>
                    </thead>
                    <tbody>
                      {sectors.slice(0, 15).map((s, idx) => (
                        <tr key={idx}>
                          <td>{s.anio}</td>
                          <td><strong>{s.sector_macro_nombre}</strong></td>
                          <td>{s.emisiones_brutas_kt?.toLocaleString()}</td>
                          <td>{s.emisiones_netas_kt?.toLocaleString()}</td>
                          <td>{s.participacion_pct}%</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              <div className="card">
                <div className="card-header">
                  <h3 className="card-title">💨 Estaciones de Calidad del Aire (WAQI)</h3>
                  <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>{airQuality.length} estaciones</span>
                </div>
                <div className="data-table-container" style={{ maxHeight: '380px' }}>
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>Estación</th>
                        <th>Ciudad</th>
                        <th>AQI</th>
                        <th>Nivel</th>
                        <th>PM2.5</th>
                      </tr>
                    </thead>
                    <tbody>
                      {airQuality.length === 0 ? (
                        <tr>
                          <td colSpan={5} style={{ textAlign: 'center', color: 'var(--text-muted)', padding: '2rem' }}>
                            No hay estaciones registradas o cargando datos...
                          </td>
                        </tr>
                      ) : (
                        airQuality.map((st, idx) => (
                          <tr key={idx}>
                            <td><strong>{st.estacion_nombre}</strong></td>
                            <td>{st.ciudad}</td>
                            <td>
                              <span style={{
                                padding: '2px 8px',
                                borderRadius: '12px',
                                background: st.aqi > 100 ? 'rgba(239, 68, 68, 0.2)' : st.aqi > 50 ? 'rgba(245, 158, 11, 0.2)' : 'rgba(16, 185, 129, 0.2)',
                                color: st.aqi > 100 ? 'var(--red-400)' : st.aqi > 50 ? 'var(--amber-400)' : 'var(--emerald-400)',
                                fontWeight: 'bold'
                              }}>
                                {st.aqi}
                              </span>
                            </td>
                            <td>{st.aqi_categoria}</td>
                            <td>{st.pm25 !== null && st.pm25 !== undefined ? `${st.pm25} µg/m³` : 'N/A'}</td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* PESTAÑA 3: CHAT IA CON OLLAMA */}
        {activeTab === 'chat' && (
          <div className="card chat-container">
            <div className="card-header">
              <h3 className="card-title">🤖 Asistente de Análisis con Ollama</h3>
              <span className="status-badge">Contexto Gold Cargado</span>
            </div>

            {/* Sugerencias Rápidas */}
            <div className="quick-prompts">
              <button
                className="quick-prompt"
                onClick={() => handleSendMessage('Genera un reporte ejecutivo completo sobre las emisiones de Colombia y la calidad del aire')}
              >
                📄 Generar reporte ejecutivo completo
              </button>
              <button
                className="quick-prompt"
                onClick={() => handleSendMessage('¿Cuáles son los 3 sectores que más gases contaminantes emiten y cuáles absorben?')}
              >
                🏭 ¿Cuáles sectores emiten más?
              </button>
              <button
                className="quick-prompt"
                onClick={() => handleSendMessage('¿Cómo se encuentra la calidad del aire actual en las estaciones de monitoreo?')}
              >
                💨 Estado de la calidad del aire
              </button>
              <button
                className="quick-prompt"
                onClick={() => handleSendMessage('Explica cómo la arquitectura Medallion mejoró la confiabilidad de los datos en este proyecto')}
              >
                🏗️ Explicar arquitectura Medallion
              </button>
            </div>

            {/* Mensajes */}
            <div className="chat-messages">
              {chatMessages.map((msg, idx) => (
                <div key={idx} className={`chat-message ${msg.role}`}>
                  <div className="chat-avatar">{msg.role === 'ai' ? '🤖' : '👤'}</div>
                  <div className="chat-bubble">{msg.text}</div>
                </div>
              ))}
              {chatLoading && (
                <div className="chat-message ai">
                  <div className="chat-avatar">🤖</div>
                  <div className="chat-typing">
                    <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>Analizando</span>
                    <div className="typing-dots">
                      <div className="typing-dot"></div>
                      <div className="typing-dot"></div>
                      <div className="typing-dot"></div>
                    </div>
                  </div>
                </div>
              )}
            </div>

            {/* Input de Chat */}
            <div className="chat-input-area">
              <div className="chat-input-wrapper">
                <textarea
                  className="chat-input"
                  placeholder="Haz una pregunta a Ollama o solicita un informe detallado con los datos..."
                  rows={2}
                  value={chatInput}
                  onChange={(e) => setChatInput(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter' && !e.shiftKey) {
                      e.preventDefault();
                      handleSendMessage();
                    }
                  }}
                />
                <button
                  className="chat-send-btn"
                  disabled={chatLoading || !chatInput.trim()}
                  onClick={() => handleSendMessage()}
                >
                  ➤
                </button>
              </div>
            </div>
          </div>
        )}

        {/* PESTAÑA 4: EXPLORADOR SQL */}
        {activeTab === 'sql' && (
          <div className="card">
            <div className="card-header">
              <h3 className="card-title">🔍 Consultas SQL Directas al Data Warehouse Gold</h3>
              <button className="btn btn-primary btn-sm" onClick={executeSql} disabled={sqlLoading}>
                {sqlLoading ? 'Ejecutando...' : '▶ Ejecutar Consulta'}
              </button>
            </div>

            <textarea
              className="sql-editor"
              value={customSql}
              onChange={(e) => setCustomSql(e.target.value)}
            />

            {sqlResult && (
              <div style={{ marginTop: '1.5rem' }}>
                <p style={{ fontSize: '0.8rem', color: 'var(--text-muted)', marginBottom: '0.5rem' }}>
                  Resultados: {sqlResult.row_count} filas encontradas en {sqlResult.elapsed_ms}ms
                </p>
                <div className="data-table-container" style={{ maxHeight: '400px' }}>
                  <table className="data-table">
                    <thead>
                      <tr>
                        {sqlResult.columns.map((col, idx) => (
                          <th key={idx}>{col}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {sqlResult.data.map((row, rIdx) => (
                        <tr key={rIdx}>
                          {sqlResult.columns.map((col, cIdx) => (
                            <td key={cIdx}>{String(row[col])}</td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        )}

        {/* PESTAÑA 5: AUDITORÍA DE CALIDAD */}
        {activeTab === 'audit' && (
          <div className="card">
            <div className="card-header">
              <h3 className="card-title">🛡️ Auditoría de Calidad e Integridad de Datos</h3>
              <button className="btn btn-primary btn-sm" onClick={runAudit} disabled={auditLoading}>
                {auditLoading ? 'Auditando...' : '🔄 Re-auditar'}
              </button>
            </div>

            {auditResult && (
              <div>
                <div className="kpi-grid">
                  <div className="kpi-card emerald">
                    <div className="kpi-label">Pruebas Totales</div>
                    <div className="kpi-value">{auditResult.total_tests}</div>
                  </div>
                  <div className="kpi-card emerald">
                    <div className="kpi-label">Aprobadas</div>
                    <div className="kpi-value">{auditResult.passed}</div>
                  </div>
                  <div className="kpi-card gold">
                    <div className="kpi-label">Tasa de Aprobación</div>
                    <div className="kpi-value">{auditResult.pass_rate}</div>
                  </div>
                </div>

                <div className="data-table-container">
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>Prueba</th>
                        <th>Dataset</th>
                        <th>Estado</th>
                        <th>Detalle</th>
                      </tr>
                    </thead>
                    <tbody>
                      {auditResult.tests?.map((t, idx) => (
                        <tr key={idx}>
                          <td><strong>{t.Test}</strong></td>
                          <td>{t.Dataset}</td>
                          <td>
                            <span style={{
                              padding: '2px 8px',
                              borderRadius: '12px',
                              background: t.Status === 'PASSED' ? 'rgba(16, 185, 129, 0.2)' : 'rgba(239, 68, 68, 0.2)',
                              color: t.Status === 'PASSED' ? 'var(--emerald-400)' : 'var(--red-400)',
                              fontWeight: 'bold'
                            }}>
                              {t.Status}
                            </span>
                          </td>
                          <td>{t.Details}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        )}
      </main>
    </div>
  );
}
