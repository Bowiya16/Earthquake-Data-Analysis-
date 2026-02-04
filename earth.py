# %% [markdown]
# ## Importing Packages

# %%
import requests
import json
from datetime import datetime
import pandas as pd

from sqlalchemy import create_engine # database connection

# %% [markdown]
# # DataSet retrival Procedure

# %% [markdown]
# ## Definition

# %% [markdown]
# 

# %%
BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

all_earthquakes = []

# %% [markdown]
# 

# %% [markdown]
# ## Data fecthing

# %%
all_earthquakes = []

for year in range(2020, 2026):
    for month in range(1, 13):

        start_date = datetime(year, month, 1)

        if month == 12:
            end_date = datetime(year + 1, 1, 1)
        else:
            end_date = datetime(year, month + 1, 1)

        starttime = start_date.strftime("%Y-%m-%d")
        endtime = end_date.strftime("%Y-%m-%d")

        print(f"Fetching data for {starttime} to {endtime}")

        params = {
            "format": "geojson",
            "starttime": starttime,
            "endtime": endtime,
            "minmagnitude": 4.5,
            "orderby": "time",
            "limit": 10000
        }

        try:
            response = requests.get(BASE_URL, params=params, timeout=30)

            if response.status_code == 200:
                data = response.json()
                count = len(data["features"])
                print(f"  Records: {count}")
                all_earthquakes.extend(data["features"])
            else:
                print(f"  FAILED: {response.status_code}")

        except requests.exceptions.RequestException as e:
            print("  ERROR:", e)

# Save JSON output
output_file = "earthquake_2020_2025_monthwise.json"
with open(output_file, "w") as f:
    json.dump(all_earthquakes, f, indent=4)

print("Data saved successfully in JSON format")


# %% [markdown]
# ## Data Reading from Json

# %%
with open ("earthquake_2020_2025_monthwise.json","r") as file:
    data = json.load(file)
    print(type(data))
print("Total records:", len(data))
print("Sample record keys:", data[0].keys())

# %% [markdown]
# ## Json to datframe conversion

# %%
records = []

for eq in data:
    prop = eq.get("properties", {})
    geo = eq.get("geometry", {})
    coords = geo.get("coordinates", [None, None, None])

    records.append({
        "id": eq.get("id"),
        "time": prop.get("time"),
        "updated": prop.get("updated"),
        "mag": prop.get("mag"),
        "magType": prop.get("magType"),
        "place": prop.get("place"),
        "status": prop.get("status"),
        "tsunami": prop.get("tsunami"),
        "sig": prop.get("sig"),
        "net": prop.get("net"),
        "nst": prop.get("nst"),
        "dmin": prop.get("dmin"),
        "rms": prop.get("rms"),
        "gap": prop.get("gap"),
        "magError": prop.get("magError"),
        "depthError": prop.get("depthError"),
        "magNst": prop.get("magNst"),
        "locationSource": prop.get("locationSource"),
        "magSource": prop.get("magSource"),
        "types": prop.get("types"),
        "ids": prop.get("ids"),
        "sources": prop.get("sources"),
        "type": prop.get("type"),
        "longitude": coords[0],
        "latitude": coords[1],
        "depth_km": coords[2]
    })

df = pd.DataFrame(records)

df.head()


# %% [markdown]
# ## EDA

# %%
df.shape

# %%
df.columns

# %%
df.types

# %%
df.info()

# %%
print("Total null values:", df.isnull().sum().sum())

# %%
df.describe()

# %%
df.isnull().sum()
print(df.isnull().sum())

# %% [markdown]
# ## Data Conversions

# %%
# Convert epoch milliseconds to datetime
df["time"] = pd.to_datetime(df["time"], unit="ms", errors="coerce")
df["updated"] = pd.to_datetime(df["updated"], unit="ms", errors="coerce")
print(df[["time", "updated"]].head())

# %% [markdown]
# ## Data Cleaning

# %%
numeric_cols = [
    "mag", "depth_km", "nst", "dmin", "rms", "gap",
    "magError", "depthError", "magNst", "sig",
    "latitude", "longitude"
]

text_cols = [
    "place", "magType", "status", "type", "net",
    "sources", "ids", "types", "locationSource", "magSource"
]


df[numeric_cols] = df[numeric_cols].fillna(0)
df[text_cols] = df[text_cols].fillna("unknown")

df.head()

# %%

# for col in numeric_cols:
#     df[col] = pd.to_numeric(df[col], errors="coerce")
# df.head(3)

# %%
df.columns = df.columns.str.lower().str.replace(" ", "_")
df.head(2)

# %% [markdown]
# ### column country using regex

# %%
df["country"] = df["place"].str.extract(r",\s*([^,]+)$")
df["country"]

# %% [markdown]
# ## Derived columns

# %% [markdown]
# ### Year, month, day and day of week

# %%
df['time'].dtype

# %%
df["year"] = df["time"].dt.year
df["year"]

# %%
df["month"] = df["time"].dt.month
df["month"]

# %%
df["day"] = df["time"].dt.day
df["day"]

# %%
df["day_of_week"] = df["time"].dt.day_name()
df["day_of_week"]

# %%
df[["time", "year", "month", "day", "day_of_week"]].head()

# %% [markdown]
# ### Depth Flag

# %%
df["depth_flag"] = df["depth_km"].apply(
    lambda x: "shallow" if x < 70 else "deep"
)
df["depth_flag"].head()

# %% [markdown]
# ### Mag flag

# %%
df["magflag"] = df["mag"].apply(
    lambda x: "destructive" if x >= 6.0 else "strong"
)
df["magflag"]

# %% [markdown]
# # Push data to MySQL

# %%
username = "root"
password = "12345"          # your MySQL password
host = "localhost"
port = 3306                 # INTEGER, not string
database = "earthquake_db"

engine = create_engine(
    f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}"
)

print("MySQL connection created successfully")


# %%
df = df.rename(columns={
    "mag": "magnitude",
    "magtype": "magnitude_type",
    "nst": "station_count",
    "dmin": "min_station_distance",
    "rms": "rms_error",
    "gap": "azimuthal_gap",
    "sig": "significance",
    "net": "network",
    "type": "event_type",
    "magerror": "magnitude_error",
    "deptherror": "depth_error",
    "magnst": "magnitude_station_count",
    "locationsource": "location_source",
    "magsource": "magnitude_source"
})

# %%
df.to_sql(
    name="earthquakes",
    con=engine,
    if_exists="append",
    index=False
)
print("Data inserted into MySQL successfully")

# %%


# %% [markdown]
# # Insights using SQL

# %% [markdown]
# ## Magnitude & Depth

# %%
query = "SELECT * FROM earthquakes"

df_mysql = pd.read_sql(query, con=engine)

# %%
df_mysql.head()

# %% [markdown]
# 
# 

# %% [markdown]
# ### 1. Top 10 strongest earthquakes (by magnitude)
# 

# %%
query = "SELECT id,magnitude,depth_km,place,time FROM earthquakes ORDER BY magnitude DESC LIMIT 10;"
pd.read_sql(query, con=engine)

# %% [markdown]
# ### 2.  Top 10 deepest earthquakes (by depth_km)

# %%
query = "SELECT id,depth_km,magnitude,place FROM earthquakes ORDER BY depth_km DESC LIMIT 10;"
pd.read_sql(query, con=engine)

# %% [markdown]
# ### 3. Shallow earthquakes (< 50 km) with magnitude > 7.5

# %%
query = "SELECT id,magnitude,depth_km,place,time FROM earthquakes WHERE depth_km<50 AND magnitude>7.5 ORDER BY magnitude DESC"
pd.read_sql(query, con=engine)

# %% [markdown]
# ### 4. Average depth per continent

# %%
query = "SELECT CASE WHEN place LIKE '%Asia%' THEN 'Asia' WHEN place LIKE '%Europe%' THEN 'Europe' WHEN place LIKE '%Africa%' THEN 'Africa' WHEN place LIKE '%America%' THEN 'America' WHEN place LIKE '%Australia%' THEN 'Australia' WHEN place LIKE '%Antarctica%' THEN 'Antarctica' ELSE 'Other' END AS continent,AVG(depth_km) AS avg_depth_km FROM earthquakes GROUP BY continent"
pd.read_sql(query, con=engine)

# %% [markdown]
# ### 5. Average magnitude per magnitude type (magType)

# %%
query = "SELECT magnitude_type,AVG(magnitude) AS avg_magnitude FROM earthquakes GROUP BY magnitude_type ORDER BY avg_magnitude DESC"
pd.read_sql(query, con=engine)

# %% [markdown]
# ## Time Analysis

# %% [markdown]
# ### 6. Year with most earthquakes (FIXED)

# %%
query = "SELECT YEAR(time) AS year,COUNT(*) AS total FROM earthquakes GROUP BY YEAR(time) ORDER BY total DESC LIMIT 1"
pd.read_sql(query, con=engine)

# %% [markdown]
# ### 7. Month with highest number of earthquakes

# %%
query = "SELECT MONTH(time) AS month,COUNT(*) AS total FROM earthquakes GROUP BY MONTH(time) ORDER BY total DESC LIMIT 1"
pd.read_sql(query, con=engine)

# %% [markdown]
# ### 8. Day of week with most earthquakes

# %%
query = "SELECT DAYNAME(time) AS day_of_week,COUNT(*) AS total FROM earthquakes GROUP BY DAYNAME(time) ORDER BY total DESC LIMIT 1"
pd.read_sql(query, con=engine)

# %% [markdown]
# ### 9. Count of earthquakes per hour

# %%
query = "SELECT HOUR(time) AS hour,COUNT(*) AS total FROM earthquakes GROUP BY HOUR(time) ORDER BY hour"
pd.read_sql(query, con=engine)

# %% [markdown]
# ### 10. Most active reporting network (net) 

# %%
query = "SELECT network,COUNT(*) AS total FROM earthquakes GROUP BY network ORDER BY total DESC LIMIT 1"
pd.read_sql(query, con=engine)

# %% [markdown]
# ## Casualties & Economic Loss

# %% [markdown]
# ### 11. Top 5 places with highest casualties - column not found

# %%
query = "SELECT place,SUM(casualties) AS total_casualties FROM earthquakes GROUP BY place ORDER BY total_casualties DESC LIMIT 5"
pd.read_sql(query, con=engine)

# %% [markdown]
# ### 12. Total estimated economic loss per continent - column not found

# %%
query = "SELECT CASE WHEN place LIKE '%Asia%' THEN 'Asia' WHEN place LIKE '%Europe%' THEN 'Europe' WHEN place LIKE '%Africa%' THEN 'Africa' WHEN place LIKE '%America%' THEN 'America' WHEN place LIKE '%Australia%' THEN 'Australia' ELSE 'Other' END AS continent,SUM(economic_loss) AS total_loss FROM earthquakes GROUP BY continent;"
pd.read_sql(query, con=engine)


# %% [markdown]
# ### 13. Average economic loss by alert level - column not found

# %%

pd.read_sql(query, con=engine)

# %% [markdown]
# ## Event Type & Quality Metrics

# %% [markdown]
# ### 14. Count of reviewed vs automatic earthquakes (status).

# %%
query = "SELECT status,COUNT(*) AS total FROM earthquakes GROUP BY status"
pd.read_sql(query, con=engine)

# %% [markdown]
# ### 15. Count by earthquake type (type).

# %%
query = "SELECT event_type,COUNT(*) AS total FROM earthquakes GROUP BY event_type ORDER BY total DESC"
pd.read_sql(query, con=engine)

# %% [markdown]
# ### 16. Number of earthquakes by data type (types).

# %%
query = "SELECT types,COUNT(*) AS total FROM earthquakes GROUP BY types ORDER BY total DESC"
pd.read_sql(query, con=engine)

# %% [markdown]
# ### 17. Average RMS and gap per continent.

# %%
query = "SELECT CASE WHEN place LIKE '%Asia%' THEN 'Asia' WHEN place LIKE '%Europe%' THEN 'Europe' WHEN place LIKE '%Africa%' THEN 'Africa' WHEN place LIKE '%America%' THEN 'America' WHEN place LIKE '%Australia%' THEN 'Australia' WHEN place LIKE '%Antarctica%' THEN 'Antarctica' ELSE 'Other' END AS continent,AVG(rms_error) AS avg_rms,AVG(azimuthal_gap) AS avg_gap FROM earthquakes GROUP BY continent"
pd.read_sql(query, con=engine)

# %% [markdown]
# ### 18. Events with high station coverage (nst > threshold).

# %%
query = "SELECT id,station_count,magnitude,place,time FROM earthquakes WHERE station_count>100 ORDER BY station_count DESC"
pd.read_sql(query, con=engine)

# %% [markdown]
# ## Tsunamis & Alerts

# %% [markdown]
# ### 19. Number of tsunamis triggered per year.

# %%
query = "SELECT YEAR(time) AS year,COUNT(*) AS tsunami_count FROM earthquakes WHERE tsunami=1 GROUP BY YEAR(time) ORDER BY year"
pd.read_sql(query, con=engine)

# %% [markdown]
# ### 20. Count earthquakes by alert levels (red, orange, etc.). - column not found

# %%
query = "SELECT alert_level,COUNT(*) AS total FROM earthquakes GROUP BY alert_level ORDER BY total DESC"
pd.read_sql(query, con=engine)

# %% [markdown]
# ## Seismic Pattern & Trends Analysis

# %% [markdown]
# ### 21.Find the top 5 countries with the highest average magnitude of earthquakes in the past 10 years

# %%
query = "SELECT country,AVG(magnitude) AS avg_magnitude FROM (SELECT CASE WHEN place LIKE '%Japan%' THEN 'Japan' WHEN place LIKE '%Indonesia%' THEN 'Indonesia' WHEN place LIKE '%Chile%' THEN 'Chile' WHEN place LIKE '%Mexico%' THEN 'Mexico' WHEN place LIKE '%China%' THEN 'China' WHEN place LIKE '%India%' THEN 'India' WHEN place LIKE '%USA%' OR place LIKE '%United States%' THEN 'USA' ELSE 'Other' END AS country,magnitude,time FROM earthquakes WHERE time>=DATE_SUB(CURDATE(),INTERVAL 10 YEAR)) t GROUP BY country ORDER BY avg_magnitude DESC LIMIT 5"
pd.read_sql(query, con=engine)

# %% [markdown]
# ### 22. Top 5 countries with highest average magnitude in the past 10 years

# %%
query = "SELECT country,AVG(magnitude) AS avg_magnitude FROM (SELECT CASE WHEN place LIKE '%Japan%' THEN 'Japan' WHEN place LIKE '%Indonesia%' THEN 'Indonesia' WHEN place LIKE '%Chile%' THEN 'Chile' WHEN place LIKE '%Mexico%' THEN 'Mexico' WHEN place LIKE '%USA%' OR place LIKE '%United States%' THEN 'USA' WHEN place LIKE '%China%' THEN 'China' WHEN place LIKE '%India%' THEN 'India' ELSE 'Other' END AS country,magnitude,time FROM earthquakes WHERE time>=DATE_SUB(CURDATE(),INTERVAL 10 YEAR)) t GROUP BY country ORDER BY avg_magnitude DESC LIMIT 5"
pd.read_sql(query, con=engine)

# %% [markdown]
# ### 23. Year-over-year growth rate of total earthquakes globally

# %%
query = "SELECT year,((total-LAG(total) OVER(ORDER BY year))/LAG(total) OVER(ORDER BY year))*100 AS yoy_growth_pct FROM (SELECT YEAR(time) AS year,COUNT(*) AS total FROM earthquakes GROUP BY YEAR(time)) t"
pd.read_sql(query, con=engine)

# %% [markdown]
# ### 24. Top 3 most seismically active regions (frequency + avg magnitude)

# %%
query = "SELECT region,COUNT(*) AS frequency,AVG(magnitude) AS avg_magnitude,(COUNT(*)*AVG(magnitude)) AS activity_score FROM (SELECT CASE WHEN place LIKE '%Ring of Fire%' OR place LIKE '%Japan%' OR place LIKE '%Indonesia%' OR place LIKE '%Chile%' THEN 'Pacific Ring of Fire' WHEN place LIKE '%Himalaya%' OR place LIKE '%India%' OR place LIKE '%Nepal%' THEN 'Himalayan Belt' WHEN place LIKE '%Mediterranean%' OR place LIKE '%Turkey%' OR place LIKE '%Italy%' THEN 'Mediterranean Belt' ELSE 'Other' END AS region,magnitude FROM earthquakes) t GROUP BY region ORDER BY activity_score DESC LIMIT 3"
pd.read_sql(query, con=engine)

# %% [markdown]
# 

# %% [markdown]
# ## Depth, Location & Distance-Based Analysis.

# %% [markdown]
# ### 25. For each country, calculate the average depth of earthquakes within ±5° latitude range of the equator.

# %%
query = "SELECT country,AVG(depth_km) AS avg_depth FROM (SELECT CASE WHEN place LIKE '%Japan%' THEN 'Japan' WHEN place LIKE '%Indonesia%' THEN 'Indonesia' WHEN place LIKE '%Chile%' THEN 'Chile' WHEN place LIKE '%Mexico%' THEN 'Mexico' WHEN place LIKE '%USA%' OR place LIKE '%United States%' THEN 'USA' ELSE 'Other' END AS country,depth_km,latitude FROM earthquakes WHERE latitude BETWEEN -5 AND 5) t GROUP BY country"
pd.read_sql(query, con=engine)

# %% [markdown]
# ### 26. Identify countries having the highest ratio of shallow to deep earthquakes.

# %%
query = "SELECT country,SUM(depth_km<70)/NULLIF(SUM(depth_km>300),0) AS shallow_to_deep_ratio FROM (SELECT CASE WHEN place LIKE '%Japan%' THEN 'Japan' WHEN place LIKE '%Indonesia%' THEN 'Indonesia' WHEN place LIKE '%Chile%' THEN 'Chile' WHEN place LIKE '%Mexico%' THEN 'Mexico' WHEN place LIKE '%USA%' OR place LIKE '%United States%' THEN 'USA' ELSE 'Other' END AS country,depth_km FROM earthquakes) t GROUP BY country ORDER BY shallow_to_deep_ratio DESC"
pd.read_sql(query, con=engine)

# %% [markdown]
# ### 27. Find the average magnitude difference between earthquakes with tsunami alerts and those without.

# %%
query = "SELECT (SELECT AVG(magnitude) FROM earthquakes WHERE tsunami=1)-(SELECT AVG(magnitude) FROM earthquakes WHERE tsunami=0) AS avg_magnitude_difference"
pd.read_sql(query, con=engine)

# %% [markdown]
# ### 28. Using the gap and rms columns, identify events with the lowest data reliability (highest average error margins).

# %%
query = "SELECT id,place,((azimuthal_gap+rms_error)/2) AS avg_error FROM earthquakes ORDER BY avg_error DESC LIMIT 10"
pd.read_sql(query, con=engine)

# %% [markdown]
# ### 29. Find pairs of consecutive earthquakes (by time) that occurred within 50 km of each other and within 1 hour.

# %%
query = "SELECT region,COUNT(*) AS deep_event_count FROM (SELECT CASE WHEN place LIKE '%Japan%' OR place LIKE '%Indonesia%' OR place LIKE '%Chile%' THEN 'Pacific Ring of Fire' WHEN place LIKE '%India%' OR place LIKE '%Nepal%' THEN 'Himalayan Belt' WHEN place LIKE '%Turkey%' OR place LIKE '%Italy%' THEN 'Mediterranean Belt' ELSE 'Other' END AS region,depth_km FROM earthquakes WHERE depth_km>300) t GROUP BY region ORDER BY deep_event_count DESC"
pd.read_sql(query, con=engine)

# %% [markdown]
# ### 30. Determine the regions with the highest frequency of deep-focus earthquakes (depth > 300 km).

# %%
query = "SELECT region,COUNT(*) AS deep_event_count FROM (SELECT CASE WHEN place LIKE '%Japan%' OR place LIKE '%Indonesia%' OR place LIKE '%Chile%' THEN 'Pacific Ring of Fire' WHEN place LIKE '%India%' OR place LIKE '%Nepal%' THEN 'Himalayan Belt' WHEN place LIKE '%Turkey%' OR place LIKE '%Italy%' THEN 'Mediterranean Belt' ELSE 'Other' END AS region,depth_km FROM earthquakes WHERE depth_km>300) t GROUP BY region ORDER BY deep_event_count DESC"
pd.read_sql(query, con=engine)

# %%
df.place.head(2)



# %%



