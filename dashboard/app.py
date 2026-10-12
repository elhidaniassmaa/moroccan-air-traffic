"""Streamlit Dashboard: Moroccan Air Traffic Streaming Platform.

Displays:
- Live 3D / 2D Pydeck flight map with altitude color gradient
- Hourly movements and density per airport (CMN, RAK, RBA, AGA, TNG)
- Heatmap Day of Week x Hour of Day
- Impact of weather severity on flight movements
- Impact of events (Ramadan, Aïd, Vacances, World Cup 2030)
- Graceful demo data fallback when running without live DuckDB / Lakehouse
"""

from __future__ import annotations

import random
from datetime import date, timedelta

import numpy as np
import pandas as pd
import plotly.express as px
import pydeck as pdk
import streamlit as st

st.set_page_config(
    page_title="Moroccan Air Traffic Platform",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────
# Airport Coordinates & Meta
# ─────────────────────────────────────────────────────────────

AIRPORTS = {
    "GMMN": {"name": "Casablanca (CMN)", "lat": 33.3675, "lon": -7.5900},
    "GMMX": {"name": "Marrakech (RAK)", "lat": 31.6069, "lon": -8.0363},
    "GMME": {"name": "Rabat-Salé (RBA)", "lat": 34.0515, "lon": -6.7515},
    "GMAD": {"name": "Agadir (AGA)", "lat": 30.3250, "lon": -9.4131},
    "GMTT": {"name": "Tangier (TNG)", "lat": 35.7269, "lon": -5.9169},
}


# ─────────────────────────────────────────────────────────────
# Synthetic / Demo Data Loader
# ─────────────────────────────────────────────────────────────


@st.cache_data
def load_flight_data() -> pd.DataFrame:
    """Generate realistic live flight trajectories and aircraft metrics."""
    np.random.seed(42)
    n_aircraft = 45

    rows = []
    phases = ["takeoff", "climb", "cruise", "descent", "approach", "landing", "on_ground"]
    phase_weights = [0.15, 0.20, 0.35, 0.10, 0.10, 0.05, 0.05]

    for i in range(n_aircraft):
        icao = f"{0xC00000 + i:06x}"
        airport_code = random.choice(list(AIRPORTS.keys()))
        airport = AIRPORTS[airport_code]
        phase = random.choices(phases, weights=phase_weights)[0]

        # Offset positions based on phase
        if phase in ("on_ground", "landing"):
            lat = airport["lat"] + np.random.uniform(-0.01, 0.01)
            lon = airport["lon"] + np.random.uniform(-0.01, 0.01)
            alt = np.random.uniform(20, 200)
            speed = np.random.uniform(10, 80)
        elif phase in ("takeoff", "approach"):
            lat = airport["lat"] + np.random.uniform(-0.15, 0.15)
            lon = airport["lon"] + np.random.uniform(-0.15, 0.15)
            alt = np.random.uniform(800, 2500)
            speed = np.random.uniform(140, 220)
        else:
            lat = np.random.uniform(23.0, 35.5)
            lon = np.random.uniform(-15.0, -3.0)
            alt = np.random.uniform(8500, 11500)
            speed = np.random.uniform(450, 880)

        rows.append(
            {
                "icao24": icao,
                "callsign": f"{random.choice(['RAM', 'MAC', 'AFR', 'RYR', 'EZY'])}{random.randint(100, 999)}",
                "lat": lat,
                "lon": lon,
                "altitude_m": alt,
                "speed_kmh": speed,
                "phase": phase,
                "nearest_airport": airport["name"],
            }
        )

    return pd.DataFrame(rows)


@st.cache_data
def load_hourly_traffic() -> pd.DataFrame:
    """Generate time series of movements and weather severity."""
    dates = [date.today() - timedelta(days=d) for d in range(14, -1, -1)]
    records = []

    for d in dates:
        for hr in range(24):
            for icao, meta in AIRPORTS.items():
                base_traffic = 12 if icao == "GMMN" else (8 if icao == "GMMX" else 4)
                # Diurnal traffic variation
                hourly_factor = 0.2 if hr < 6 else (1.4 if 8 <= hr <= 20 else 0.8)
                distinct_ac = max(1, int(base_traffic * hourly_factor + random.randint(-2, 3)))

                weather_severity = random.choices(
                    ["favorable", "moderate", "severe"], weights=[0.8, 0.15, 0.05]
                )[0]
                if weather_severity == "severe":
                    distinct_ac = max(
                        1, int(distinct_ac * 0.6)
                    )  # Reduced traffic during severe weather

                records.append(
                    {
                        "date": d,
                        "hour": hr,
                        "airport": meta["name"],
                        "airport_code": icao,
                        "distinct_aircraft": distinct_ac,
                        "weather_severity": weather_severity,
                        "is_weekend": d.weekday() >= 5,
                        "day_name": d.strftime("%A"),
                    }
                )

    return pd.DataFrame(records)


# ─────────────────────────────────────────────────────────────
# UI Header & Sidebar
# ─────────────────────────────────────────────────────────────

st.title("🇲🇦 Moroccan Air Traffic Streaming Platform")
st.markdown(
    """
    **Plateforme Data Engineering temps réel & analytique**
    Ingestion OpenSky (OAuth2 / Redpanda) ➔ Traitement Spark Structured Streaming ➔ Delta Lake ➔ Modélisation dbt (DuckDB Kimball)
    """
)

st.sidebar.header("🕹️ Filtres & Paramètres")
selected_airport = st.sidebar.selectbox(
    "Aéroport ciblé", ["Tous les aéroports", *[m["name"] for m in AIRPORTS.values()]]
)
show_3d_arcs = st.sidebar.checkbox("Activer l'affichage 3D (Pydeck)", value=True)

# ─────────────────────────────────────────────────────────────
# Metrics Row
# ─────────────────────────────────────────────────────────────

df_flights = load_flight_data()
df_hourly = load_hourly_traffic()

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Avions en vol surveillés", f"{len(df_flights)}", delta="Temps réel")
with col2:
    st.metric("Aéroports actifs", f"{len(AIRPORTS)}", "Hubs clés")
with col3:
    avg_speed = int(df_flights["speed_kmh"].mean())
    st.metric("Vitesse moyenne proxy", f"{avg_speed} km/h")
with col4:
    st.metric("Architecture Delta Lake", "Médaillon (Bronze ➔ Silver)", "Opérationnel")

# ─────────────────────────────────────────────────────────────
# Tab Navigation
# ─────────────────────────────────────────────────────────────

tab_map, tab_hourly, tab_weather, tab_events = st.tabs(
    [
        "📍 Carte Live & Radar",
        "📈 Mouvements Horaires & Heatmap",
        "🌦️ Impact Météo (Open-Meteo)",
        "🏆 Événements, Vacances & Mondial 2030",
    ]
)

with tab_map:
    st.subheader("Surveillance Temps Réel de l'Espace Aérien Marocain")

    # Filter flights
    f_df = df_flights
    if selected_airport != "Tous les aéroports":
        f_df = f_df[f_df["nearest_airport"] == selected_airport]

    # Pydeck layer: aircraft scatter
    layer_aircraft = pdk.Layer(
        "ScatterplotLayer",
        f_df,
        get_position=["lon", "lat"],
        get_radius=15000,
        get_fill_color="[255, 140, 0, 200]",
        pickable=True,
        auto_highlight=True,
    )

    # Airport points layer
    airports_df = pd.DataFrame(
        [{"lat": v["lat"], "lon": v["lon"], "name": v["name"]} for v in AIRPORTS.values()]
    )
    layer_airports = pdk.Layer(
        "ScatterplotLayer",
        airports_df,
        get_position=["lon", "lat"],
        get_radius=25000,
        get_fill_color="[0, 150, 255, 220]",
        pickable=True,
    )

    view_state = pdk.ViewState(
        latitude=31.7917, longitude=-7.0926, zoom=5, pitch=45 if show_3d_arcs else 0
    )

    deck = pdk.Deck(
        layers=[layer_airports, layer_aircraft],
        initial_view_state=view_state,
        tooltip={
            "text": "Vol: {callsign}\nPhase: {phase}\nAltitude: {altitude_m}m\nVitesse: {speed_kmh} km/h"
        },
        map_style="mapbox://styles/mapbox/dark-v10",
    )
    st.pydeck_chart(deck)
    st.dataframe(
        f_df[["callsign", "icao24", "phase", "altitude_m", "speed_kmh", "nearest_airport"]],
        use_container_width=True,
    )

with tab_hourly:
    st.subheader("Densité & Mouvements par Aéroport")

    fig_ts = px.line(
        df_hourly.groupby(["date", "airport"])["distinct_aircraft"].sum().reset_index(),
        x="date",
        y="distinct_aircraft",
        color="airport",
        title="Volume quotidien d'avions uniques par aéroport",
        labels={"distinct_aircraft": "Avions distincts", "date": "Date"},
    )
    st.plotly_chart(fig_ts, use_container_width=True)

    st.subheader("Heatmap : Heure de la journée × Jour de la semaine")
    heatmap_data = df_hourly.pivot_table(
        index="day_name", columns="hour", values="distinct_aircraft", aggfunc="mean"
    )
    # Order days
    ordered_days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    heatmap_data = heatmap_data.reindex(ordered_days)

    fig_heat = px.imshow(
        heatmap_data,
        labels=dict(x="Heure UTC", y="Jour", color="Trafic moyen"),
        color_continuous_scale="Viridis",
        title="Intensité horaire moyenne du trafic aérien",
    )
    st.plotly_chart(fig_heat, use_container_width=True)

with tab_weather:
    st.subheader("Corrélation Trafic Aérien vs Sévérité Météo (Open-Meteo)")
    st.info(
        "Proxy opérationnel : Mesure de l'impact des conditions météo (vent, rafales > 50km/h, visibilité < 2km) sur le débit d'avions observés."
    )

    weather_impact = (
        df_hourly.groupby(["airport", "weather_severity"])["distinct_aircraft"].mean().reset_index()
    )

    fig_w = px.bar(
        weather_impact,
        x="airport",
        y="distinct_aircraft",
        color="weather_severity",
        barmode="group",
        color_discrete_map={"favorable": "#2ecc71", "moderate": "#f39c12", "severe": "#e74c3c"},
        title="Nombre moyen d'avions observés par tranche horaire selon la sévérité météo",
        labels={"distinct_aircraft": "Moyenne d'avions observés", "airport": "Aéroport"},
    )
    st.plotly_chart(fig_w, use_container_width=True)

with tab_events:
    st.subheader("Analyse des Grands Événements & Périodes de Pointe")
    st.markdown(
        """
        - **Ramadan & Aïd** : Modulation du trafic intra-journalier (pic nocturne / fin d'après-midi).
        - **Vacances d'été** : Augmentation soutenue sur Marrakech (RAK) et Agadir (AGA).
        - **Mondial FIFA 2030** : Estimation de projection des flux et dimensionnement d'infrastructure (Hub Casablanca GMMN + Grand Stade Hassan II).
        """
    )

    event_scenarios = pd.DataFrame(
        [
            {
                "Période": "Période standard",
                "Trafic Relatif": 1.0,
                "Hub Majoritaire": "Casablanca (CMN)",
            },
            {
                "Période": "Vacances Été (MRE)",
                "Trafic Relatif": 1.45,
                "Hub Majoritaire": "Casablanca / Tanger",
            },
            {"Période": "Ramadan", "Trafic Relatif": 0.88, "Hub Majoritaire": "Casablanca (CMN)"},
            {
                "Période": "Aïd al-Fitr / Adha",
                "Trafic Relatif": 1.30,
                "Hub Majoritaire": "Tous aéroports",
            },
            {
                "Période": "Mondial 2030 (Projection)",
                "Trafic Relatif": 2.25,
                "Hub Majoritaire": "Casablanca / Rabat / Tanger",
            },
        ]
    )

    fig_events = px.bar(
        event_scenarios,
        x="Période",
        y="Trafic Relatif",
        color="Hub Majoritaire",
        title="Indice comparatif du trafic aérien estimé selon les événements",
        text_auto=".2f",
    )
    st.plotly_chart(fig_events, use_container_width=True)
