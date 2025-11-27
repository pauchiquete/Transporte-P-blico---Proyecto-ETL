"""
Dashboard de Análisis de Transporte Público
Visualiza los datos procesados por el pipeline ETL de Airflow
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime
import os
from pathlib import Path

# Configuración de la página
st.set_page_config(
    page_title="Dashboard Transporte Público",
    page_icon="🚌",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilo CSS personalizado
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .insight-box {
        background-color: #d1e7f0;
        padding: 1.5rem;
        border-radius: 0.75rem;
        margin-top: 1rem;
        border-left: 4px solid #0288d1;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .insight-box h4 {
        color: #01579b;
        margin-top: 0;
        font-weight: bold;
        font-size: 1.1rem;
    }
    .insight-box p {
        color: #0d47a1;
        margin: 0.5rem 0;
        font-size: 0.95rem;
    }
    .insight-box strong {
        color: #01579b;
        font-weight: bold;
    }
    .insight-box em {
        color: #0277bd;
        font-style: italic;
    }
    </style>
""", unsafe_allow_html=True)

# Función para cargar datos
@st.cache_data
def load_data():
    """Carga los datos procesados del pipeline ETL"""
    # Determinar rutas posibles
    data_paths = [
        # Path local relativo (cuando se ejecuta localmente)
        Path(__file__).parent.parent / "data" / "processed_transport_data.parquet",
        # Path local absoluta (cuando se ejecuta localmente desde Windows)
        Path("C:/Users/pauli/OneDrive/Escritorio/proyecto-transporte/data/processed_transport_data.parquet"),
        # Path de Docker
        Path("/opt/airflow/data/processed_transport_data.parquet"),
    ]
    
    csv_paths = [
        # Path local relativo
        Path(__file__).parent.parent / "data" / "clean_transport_data.csv",
        # Path local absoluta
        Path("C:/Users/pauli/OneDrive/Escritorio/proyecto-transporte/data/clean_transport_data.csv"),
        # Path de Docker
        Path("/opt/airflow/data/clean_transport_data.csv"),
    ]
    
    # Intentar cargar desde Parquet
    for parquet_path in data_paths:
        if parquet_path.exists():
            try:
                st.info(f"📁 Cargando datos desde: {parquet_path}")
                df = pd.read_parquet(str(parquet_path))
                st.success("✅ Datos cargados exitosamente desde Parquet")
                return df
            except Exception as e:
                st.warning(f"⚠️ Error al cargar Parquet desde {parquet_path}: {str(e)}")
                continue
    
    # Intentar cargar desde CSV
    for csv_path in csv_paths:
        if csv_path.exists():
            try:
                st.info(f"📁 Cargando datos desde: {csv_path}")
                df = pd.read_csv(str(csv_path))
                # Convertir columnas de fecha si existen
                for col in ['fecha', 'hora_programada', 'hora_real']:
                    if col in df.columns:
                        try:
                            df[col] = pd.to_datetime(df[col], errors='coerce')
                        except:
                            pass
                st.success("✅ Datos cargados exitosamente desde CSV")
                return df
            except Exception as e:
                st.warning(f"⚠️ Error al cargar CSV desde {csv_path}: {str(e)}")
                continue
    
    # Si no se encuentran datos, mostrar advertencia
    st.warning("⚠️ No se encontraron datos procesados. Mostrando datos de ejemplo.")
    return generate_sample_data()

def generate_sample_data():
    """Genera datos de muestra para demostración"""
    np.random.seed(42)
    dates = pd.date_range(end=datetime.now(), periods=2000, freq='10T')
    routes = ['Ruta 1 - Centro', 'Ruta 2 - Norte', 'Ruta 3 - Sur', 
              'Ruta 4 - Este', 'Ruta 5 - Oeste']
    
    df = pd.DataFrame({
        'fecha': dates,
        'ruta': np.random.choice(routes, 2000),
        'numero_bus': np.random.randint(100, 150, 2000),
        'pasajeros_subieron': np.random.poisson(8, 2000),
        'pasajeros_bajaron': np.random.poisson(6, 2000),
        'capacidad_maxima': np.random.choice([40, 50, 60], 2000),
        'ocupacion_actual': np.random.randint(5, 45, 2000),
        'porcentaje_ocupacion': np.random.uniform(10, 95, 2000),
        'retraso_minutos': np.random.exponential(2, 2000),
        'puntualidad': np.random.choice(['A Tiempo', 'Retraso Leve', 'Retraso Grave'], 2000),
        'periodo_dia': np.random.choice(['Mañana', 'Tarde', 'Noche'], 2000),
        'es_hora_pico': np.random.choice([True, False], 2000),
        'dia_semana': pd.date_range(end=datetime.now(), periods=2000, freq='10T').day_name()
    })
    
    return df

# Header principal
st.markdown('<p class="main-header">🚌 Dashboard de Análisis de Transporte Público</p>', 
            unsafe_allow_html=True)
st.markdown("---")

# Cargar datos
with st.spinner('Cargando datos del pipeline ETL...'):
    df = load_data()

# Convertir columnas de fecha a datetime si son strings
for col in ['fecha', 'hora_programada', 'hora_real']:
    if col in df.columns:
        if df[col].dtype == 'object':
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except:
                pass

# Limpiar valores NaN y 'nan' en columnas categóricas
for col in ['periodo_dia', 'puntualidad', 'ruta']:
    if col in df.columns:
        df[col] = df[col].replace(['nan', 'NaN', 'None'], 'Desconocido')
        df[col] = df[col].fillna('Desconocido')

# Sidebar - Filtros
st.sidebar.title("🔍 Filtros")
st.sidebar.markdown("---")

# Filtro por ruta
rutas_disponibles = ['Todas'] + sorted(df['ruta'].unique().tolist())
ruta_seleccionada = st.sidebar.selectbox("Seleccionar Ruta:", rutas_disponibles)

# Filtro por fecha
fecha_min = df['fecha'].min().date()
fecha_max = df['fecha'].max().date()
rango_fechas = st.sidebar.date_input(
    "Rango de Fechas:",
    value=(fecha_min, fecha_max),
    min_value=fecha_min,
    max_value=fecha_max
)

# Filtro por periodo del día
periodos_disponibles = ['Todos'] + sorted(df['periodo_dia'].unique().tolist())
periodo_seleccionado = st.sidebar.selectbox("Periodo del Día:", periodos_disponibles)

# Aplicar filtros
df_filtered = df.copy()
if ruta_seleccionada != 'Todas':
    df_filtered = df_filtered[df_filtered['ruta'] == ruta_seleccionada]
if len(rango_fechas) == 2:
    df_filtered = df_filtered[
        (df_filtered['fecha'].dt.date >= rango_fechas[0]) & 
        (df_filtered['fecha'].dt.date <= rango_fechas[1])
    ]
if periodo_seleccionado != 'Todos':
    df_filtered = df_filtered[df_filtered['periodo_dia'] == periodo_seleccionado]

# KPIs principales
st.subheader("📊 Indicadores Clave de Rendimiento (KPIs)")

col1, col2, col3, col4 = st.columns(4)

with col1:
    ocupacion_promedio = df_filtered['porcentaje_ocupacion'].mean()
    st.metric(
        label="Ocupación Promedio",
        value=f"{ocupacion_promedio:.1f}%",
        delta=f"{ocupacion_promedio - 50:.1f}% vs. objetivo 50%"
    )

with col2:
    retraso_promedio = df_filtered['retraso_minutos'].mean()
    st.metric(
        label="Retraso Promedio",
        value=f"{retraso_promedio:.1f} min",
        delta=f"{2 - retraso_promedio:.1f} min vs. meta 2min",
        delta_color="inverse"
    )

with col3:
    puntualidad_rate = (df_filtered['puntualidad'] == 'A Tiempo').sum() / len(df_filtered) * 100
    st.metric(
        label="Tasa de Puntualidad",
        value=f"{puntualidad_rate:.1f}%",
        delta=f"{puntualidad_rate - 70:.1f}% vs. objetivo 70%"
    )

with col4:
    total_pasajeros = df_filtered['pasajeros_subieron'].sum()
    st.metric(
        label="Total Pasajeros",
        value=f"{total_pasajeros:,}",
        delta=f"{len(df_filtered)} viajes"
    )

st.markdown("---")

# Gráficos principales
st.subheader("📈 Análisis Visual")

# Row 1: Dos gráficos lado a lado
col1, col2 = st.columns(2)

with col1:
    # GRÁFICO 1: Ocupación por Ruta y Periodo del Día
    st.markdown("### 🚍 Ocupación Promedio por Ruta y Periodo")
    
    ocupacion_ruta_periodo = df_filtered.groupby(['ruta', 'periodo_dia'])['porcentaje_ocupacion'].mean().reset_index()
    
    fig1 = px.bar(
        ocupacion_ruta_periodo,
        x='ruta',
        y='porcentaje_ocupacion',
        color='periodo_dia',
        barmode='group',
        title='Comparación de Ocupación por Ruta',
        labels={'porcentaje_ocupacion': 'Ocupación (%)', 'ruta': 'Ruta'},
        color_discrete_sequence=px.colors.qualitative.Set2,
        height=400
    )
    
    fig1.add_hline(y=80, line_dash="dash", line_color="red", 
                   annotation_text="Capacidad Crítica (80%)")
    fig1.add_hline(y=50, line_dash="dot", line_color="green", 
                   annotation_text="Objetivo (50%)")
    
    fig1.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig1, use_container_width=True)
    
    with st.expander("💡 ¿Por qué este gráfico?"):
        st.write("""
        Este gráfico de barras agrupadas permite identificar:
        - **Rutas sobrecargadas** que necesitan más unidades
        - **Horarios de mayor demanda** para optimizar frecuencias
        - **Patrones de uso** a lo largo del día
        
        Las líneas de referencia ayudan a detectar rápidamente rutas 
        que superan el 80% de ocupación (crítico) o están por debajo del 50% (ineficiente).
        """)

with col2:
    # GRÁFICO 2: Distribución de Puntualidad
    st.markdown("### ⏱️ Distribución de Puntualidad")
    
    puntualidad_counts = df_filtered['puntualidad'].value_counts().reset_index()
    puntualidad_counts.columns = ['Estado', 'Cantidad']
    
    colors_puntualidad = {'A Tiempo': '#2ecc71', 'Retraso Leve': '#f39c12', 
                          'Retraso Grave': '#e74c3c'}
    
    fig2 = px.pie(
        puntualidad_counts,
        values='Cantidad',
        names='Estado',
        title='Distribución de Estados de Puntualidad',
        color='Estado',
        color_discrete_map=colors_puntualidad,
        hole=0.4,
        height=400
    )
    
    fig2.update_traces(textposition='inside', textinfo='percent+label')
    st.plotly_chart(fig2, use_container_width=True)
    
    with st.expander("💡 ¿Por qué este gráfico?"):
        st.write("""
        El gráfico de dona muestra de forma clara:
        - **Proporción de servicios puntuales** vs. retrasados
        - **Identificación rápida** de problemas de puntualidad
        - **Comparación visual** entre categorías
        
        Los colores (verde=bueno, naranja=aceptable, rojo=malo) facilitan 
        la interpretación inmediata del rendimiento del sistema.
        """)

# Row 2: Gráfico de línea de tiempo completo
st.markdown("### 📅 Evolución Temporal de Ocupación y Retrasos")

# Resample por hora para mejor visualización
df_timeseries = df_filtered.set_index('fecha').resample('1H').agg({
    'porcentaje_ocupacion': 'mean',
    'retraso_minutos': 'mean'
}).reset_index()

fig3 = make_subplots(
    rows=2, cols=1,
    subplot_titles=('Ocupación a lo Largo del Tiempo', 'Retrasos a lo Largo del Tiempo'),
    vertical_spacing=0.15,
    specs=[[{"secondary_y": False}], [{"secondary_y": False}]]
)

# Gráfico de ocupación
fig3.add_trace(
    go.Scatter(
        x=df_timeseries['fecha'],
        y=df_timeseries['porcentaje_ocupacion'],
        mode='lines',
        name='Ocupación (%)',
        line=dict(color='#3498db', width=2),
        fill='tozeroy'
    ),
    row=1, col=1
)

# Gráfico de retrasos
fig3.add_trace(
    go.Scatter(
        x=df_timeseries['fecha'],
        y=df_timeseries['retraso_minutos'],
        mode='lines',
        name='Retraso (min)',
        line=dict(color='#e74c3c', width=2)
    ),
    row=2, col=1
)

fig3.update_xaxes(title_text="Fecha y Hora", row=2, col=1)
fig3.update_yaxes(title_text="Ocupación (%)", row=1, col=1)
fig3.update_yaxes(title_text="Retraso (min)", row=2, col=1)

fig3.update_layout(height=600, showlegend=True)
st.plotly_chart(fig3, use_container_width=True)

with st.expander("💡 ¿Por qué este gráfico?"):
    st.write("""
    Las series temporales permiten:
    - **Detectar tendencias** y patrones horarios/diarios
    - **Identificar correlaciones** entre ocupación y retrasos
    - **Planificar intervenciones** basadas en datos históricos
    
    El área sombreada en ocupación ayuda a visualizar la magnitud del uso del servicio.
    """)

# Row 3: Análisis por día de la semana
st.markdown("### 📆 Análisis por Día de la Semana")

col1, col2 = st.columns(2)

with col1:
    # Pasajeros por día de la semana
    dias_orden = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    pasajeros_dia = df_filtered.groupby('dia_semana')['pasajeros_subieron'].sum().reindex(dias_orden).reset_index()
    
    dias_es = {'Monday': 'Lunes', 'Tuesday': 'Martes', 'Wednesday': 'Miércoles',
               'Thursday': 'Jueves', 'Friday': 'Viernes', 'Saturday': 'Sábado', 'Sunday': 'Domingo'}
    pasajeros_dia['dia_semana'] = pasajeros_dia['dia_semana'].map(dias_es)
    
    fig4 = px.bar(
        pasajeros_dia,
        x='dia_semana',
        y='pasajeros_subieron',
        title='Total de Pasajeros por Día',
        labels={'pasajeros_subieron': 'Total Pasajeros', 'dia_semana': 'Día'},
        color='pasajeros_subieron',
        color_continuous_scale='Blues'
    )
    st.plotly_chart(fig4, use_container_width=True)

with col2:
    # Hora pico vs no pico
    hora_pico_stats = df_filtered.groupby('es_hora_pico').agg({
        'porcentaje_ocupacion': 'mean',
        'retraso_minutos': 'mean'
    }).reset_index()
    
    hora_pico_stats['es_hora_pico'] = hora_pico_stats['es_hora_pico'].map({
        True: 'Hora Pico', False: 'Hora Normal'
    })
    
    fig5 = go.Figure()
    fig5.add_trace(go.Bar(
        name='Ocupación (%)',
        x=hora_pico_stats['es_hora_pico'],
        y=hora_pico_stats['porcentaje_ocupacion'],
        marker_color='lightblue'
    ))
    fig5.add_trace(go.Bar(
        name='Retraso (min)',
        x=hora_pico_stats['es_hora_pico'],
        y=hora_pico_stats['retraso_minutos'],
        marker_color='salmon'
    ))
    
    fig5.update_layout(
        title='Comparación: Hora Pico vs Hora Normal',
        barmode='group',
        xaxis_title='Tipo de Hora',
        yaxis_title='Valor'
    )
    st.plotly_chart(fig5, use_container_width=True)

# Sección de insights y recomendaciones
st.markdown("---")
st.subheader("💡 Insights y Recomendaciones")

col1, col2, col3 = st.columns(3)

with col1:
    ruta_mas_ocupada = df_filtered.groupby('ruta')['porcentaje_ocupacion'].mean().idxmax()
    ocupacion_max = df_filtered.groupby('ruta')['porcentaje_ocupacion'].mean().max()
    
    st.markdown(f"""
    <div class="insight-box">
    <h4>🔴 Ruta Crítica</h4>
    <p><strong>{ruta_mas_ocupada}</strong></p>
    <p>Ocupación promedio: <strong>{ocupacion_max:.1f}%</strong></p>
    <p>✅ <em>Recomendación: Incrementar frecuencia de unidades en esta ruta.</em></p>
    </div>
    """, unsafe_allow_html=True)

with col2:
    retraso_grave = (df_filtered['puntualidad'] == 'Retraso Grave').sum()
    porcentaje_grave = (retraso_grave / len(df_filtered) * 100)
    
    st.markdown(f"""
    <div class="insight-box">
    <h4>⏱️ Puntualidad</h4>
    <p>Retrasos graves: <strong>{retraso_grave}</strong> ({porcentaje_grave:.1f}%)</p>
    <p>Retraso promedio: <strong>{retraso_promedio:.1f} min</strong></p>
    <p>✅ <em>Recomendación: Revisar rutas con mayor congestión y ajustar tiempos programados.</em></p>
    </div>
    """, unsafe_allow_html=True)

with col3:
    hora_pico_ocupacion = df_filtered[df_filtered['es_hora_pico']]['porcentaje_ocupacion'].mean()
    
    st.markdown(f"""
    <div class="insight-box">
    <h4>🕐 Horas Pico</h4>
    <p>Ocupación en hora pico: <strong>{hora_pico_ocupacion:.1f}%</strong></p>
    <p>Diferencia vs normal: <strong>+{hora_pico_ocupacion - ocupacion_promedio:.1f}%</strong></p>
    <p>✅ <em>Recomendación: Implementar unidades express en horarios 7-9 AM y 5-7 PM.</em></p>
    </div>
    """, unsafe_allow_html=True)

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #7f8c8d;'>
    <p>📊 Dashboard generado con datos del Pipeline ETL de Apache Airflow</p>
    <p>Última actualización: {}</p>
</div>
""".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")), unsafe_allow_html=True)

# Sidebar - Información adicional
st.sidebar.markdown("---")
st.sidebar.markdown("### 📋 Acerca de este Dashboard")
st.sidebar.info("""
Este dashboard analiza datos del sistema de transporte público para:

✅ **Optimizar** la frecuencia de rutas
✅ **Mejorar** la puntualidad del servicio
✅ **Reducir** la congestión en horas pico
✅ **Aumentar** la satisfacción de usuarios

**Datos procesados por:**
Apache Airflow ETL Pipeline
""")

st.sidebar.markdown("---")
st.sidebar.markdown("### 📊 Estadísticas del Dataset")
st.sidebar.write(f"**Total de registros:** {len(df_filtered):,}")
st.sidebar.write(f"**Rutas analizadas:** {df_filtered['ruta'].nunique()}")
st.sidebar.write(f"**Buses en operación:** {df_filtered['numero_bus'].nunique()}")
st.sidebar.write(f"**Periodo:** {fecha_min} a {fecha_max}")
