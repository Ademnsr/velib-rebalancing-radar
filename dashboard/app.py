import streamlit as st

st.title("Vélib Rebalancing Radar")
st.write("Classement des stations à rééquilibrer en priorité")

conn = st.connection("snowflake")

df = conn.query("SELECT * FROM MARTS.mart_priorite_reequilibrage ORDER BY score_criticite DESC LIMIT 20")

st.dataframe(df)
st.subheader("Carte des stations les plus critiques")

df_carte = df.rename(columns={"latitude": "lat", "longitude": "lon"})
st.map(df_carte)