# Databricks notebook source
#TASK 0
path = 'dbfs:/databricks-datasets/nyctaxi/tripdata/green/green_tripdata_2019-12.csv.gz'
df = (
    spark.read.format('csv')
    .load(path, header=True)
    .toPandas()
)
display(df)
df.info


# COMMAND ----------


import pandas as pd

# TASK 1 Convert 'lpep_pickup_datetime' and 'lpep_dropoff_datetime' columns to datetime type
df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

display(df)
print(df['lpep_pickup_datetime'].dtype)
print(df['lpep_dropoff_datetime'].dtype)
print(df)
df.info

# COMMAND ----------

# TASK 2 Datetimes between the start and end of December 2019
start_date = pd.to_datetime('2019-12-01 00:00:00')
end_date = pd.to_datetime('2019-12-31 23:59:59')

# Filter the DataFrame for trips in December 2019
df_dec_2019 = df[
    (df['lpep_pickup_datetime'] >= start_date) & 
    (df['lpep_dropoff_datetime'] <= end_date)
]

display(df_dec_2019)
df_dec_2019

# COMMAND ----------

#TASK 3  Filter out trips where dropoff time is before pickup time

df_filtered = df_dec_2019[df_dec_2019['lpep_dropoff_datetime'] >= df_dec_2019['lpep_pickup_datetime']]
df_dropped = df_dec_2019[df_dec_2019['lpep_dropoff_datetime'] < df_dec_2019['lpep_pickup_datetime']]
# Verify the filtering 
display(df_filtered)
print("The number of DataFrame rows after filtered out trips:", df_filtered.shape[0])
#Displaying dropped 2 rows
df_dropped

# COMMAND ----------



# COMMAND ----------

#TASK 4
columns = [
    'trip_distance', 'fare_amount', 'extra', 'mta_tax', 
    'tip_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge'
]

df_remove_negative = df_filtered.copy()
# Loop through each column and convert to float
for col in columns:
    df_remove_negative[col] = df_remove_negative[col].astype(float)  # Convert to float
    # Filter out rows where the column has negative values
    df_remove_negative = df_remove_negative[df_remove_negative[col] >= 0]

display(df_remove_negative)
print("The number of removed DataFrame rows:", df_remove_negative.shape[0]) #Number of rows [0]

# COMMAND ----------

# Task 5  Remove records with missing values in VendorID
df_cleaned = df_remove_negative.dropna(subset=['VendorID'])

display(df_cleaned)

print("The number of cleaned DataFrame rows after removing N/A values in Vendor:", df_cleaned.shape[0])

# COMMAND ----------

# Task 6
# Remove 'lpep' prefix from the columns lpep_pickup_datetime and lpep_dropoff_datetime
df_remove_lpep = df_cleaned.rename(columns={
    'lpep_pickup_datetime': 'pickup_datetime',
    'lpep_dropoff_datetime': 'dropoff_datetime'
})

display(df_remove_lpep)

# COMMAND ----------

# Task 7  Find the trip with the highest fare amount
highest_fare_trip = df_remove_lpep.loc[df_remove_lpep['fare_amount'].astype(float).idxmax()]

# Display the trip with the highest fare amount
display(highest_fare_trip)
