### NYC Green Taxi Data Cleaning & Analysis

This project involves cleaning and analyzing the NYC Green Taxi dataset (December 2019) using Pandas and Apache Spark in Databricks. The dataset is sourced from Databricks' public repository.

---

### Dataset Source:
- **Databricks:** `dbfs:/databricks-datasets/nyctaxi/tripdata/green/green_tripdata_2019-12.csv.gz`

---

### Tasks & Implementation:

Load Dataset:**
`df = (
    spark.read.format('csv')
    .load(path, header=True)
    .toPandas()
)`
1. Convert Datetime Columns:

`df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])`
2. Filter for December 2019 Trips:

`df = df[
    (df['lpep_pickup_datetime'] >= '2019-12-01') &
    (df['lpep_dropoff_datetime'] <= '2019-12-31')
]`
3. Remove Invalid Trips:

`df = df[df['lpep_dropoff_datetime'] >= df['lpep_pickup_datetime']]`
4. Remove Negative Values:

`cols = [
    'trip_distance', 'fare_amount', 'extra', 'mta_tax',   
    'tip_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge'
]`

`df = df[(df[cols] >= 0).all(axis=1)]`
5. Handle Missing Values:

`df = df.dropna(subset=['VendorID'])
print(f"Rows after cleaning: {len(df)}")`
6. Rename Columns:

`df.rename(columns={
    'lpep_pickup_datetime': 'pickup_datetime',
    'lpep_dropoff_datetime': 'dropoff_datetime'
}, inplace=True)`
7. Identify Highest Fare Trip:

`highest_fare_trip = df.loc[df['fare_amount'].idxmax()]
print(highest_fare_trip)`


Running the Project:
`Load data via Spark in Databricks.
Execute cleaning and analysis steps with Pandas.
Review final dataset and insights.`


Tools & Libraries:
`Databricks, Apache Spark, Pandas`

License:
`For educational and analytical use only.`


References:
`Databricks Datasets
Pandas Docs
Spark Docs`
