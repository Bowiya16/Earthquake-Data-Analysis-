import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px

# ---------------- PAGE SETUP ----------------
st.set_page_config(page_title="Earthquake Dashboard", layout="wide")
st.title("🌍 Global Earthquake Analysis Dashboard")

# ---------------- MYSQL CONNECTION ----------------
engine = create_engine(
    "mysql+mysqlconnector://root:12345@localhost:3306/earthquake_db"
)

# ---------------- LOAD DATA ----------------
@st.cache_data
def load_data():
    return pd.read_sql("SELECT * FROM earthquakes", engine)

df = load_data()

# ---------------- DERIVED COLUMNS ----------------
df["time"] = pd.to_datetime(df["time"])
df["year"] = df["time"].dt.year
df["month"] = df["time"].dt.month
df["day_of_week"] = df["time"].dt.day_name()
df["country"] = df["place"].str.extract(r",\s*([^,]+)$").fillna("Unknown")

# ---------------- SIDEBAR FILTERS ----------------
st.sidebar.header("Filters")

year = st.sidebar.selectbox("Select Year", sorted(df["year"].dropna().unique()))
country = st.sidebar.multiselect("Select Country", sorted(df["country"].unique()))

filtered_df = df[df["year"] == year]
if country:
    filtered_df = filtered_df[filtered_df["country"].isin(country)]

# ---------------- KPIs ----------------
c1, c2, c3 = st.columns(3)
c1.metric("Total Earthquakes", len(filtered_df))
c2.metric("Max Magnitude", round(filtered_df["magnitude"].max(), 2))
c3.metric("Avg Depth (km)", round(filtered_df["depth_km"].mean(), 2))

st.divider()

# ---------------- CHARTS ----------------
# Earthquakes per Year
yearly = df.groupby("year").size().reset_index(name="count")
fig1 = px.bar(yearly, x="year", y="count", title="Earthquakes per Year")
st.plotly_chart(fig1, use_container_width=True)

# Magnitude Distribution
fig2 = px.histogram(filtered_df, x="magnitude", nbins=30, title="Magnitude Distribution")
st.plotly_chart(fig2, use_container_width=True)

# Map
st.subheader("Earthquake Locations")
st.map(filtered_df[["latitude", "longitude"]])

# ---------------- DATA PREVIEW ----------------
st.subheader("Data Preview")
st.dataframe(filtered_df.head(500))
