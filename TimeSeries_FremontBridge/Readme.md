Time-Series Analysis with Pandas

This analyzing time-series data (Seattle’s Fremont Bridge bicycle crossings) using Pandas. The dataset is publicly available from Seattle's data portal.

---

### Dataset Source:

`https://data.seattle.gov/api/views/65db-xm6k/rows.csv?accessType=DOWNLOAD`

---

### Tasks & Implementation:

**Load Dataset:**
`!curl -o FremontBridge.csv https://data.seattle.gov/api/views/65db-xm6k/rows.csv?accessType=DOWNLOAD`


**1. Plot Weekly bicycle crossings using resampling:**
`data.resample('W').sum().plot()`


**2. Plot Weekly bicycle crossings using a 7-day rolling mean:**
`data = df.resample('D').sum() data.rolling(7).mean().plot()`


**3. 50-day rolling mean with Gaussian window (std=30 days):**
`data.rolling(window=50, win_type='gaussian').mean(std=30).plot()`


**4. Plot Average Daily Counts (Monday–Sunday):**
`by_day = df.groupby(df.index.day_name()).mean() by_day_ordered = by_day.reindex(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']) by_day_ordered.plot()`


**4. Plot daily data by hourly intervals:**
`hourly_ticks = 4 * 60 * 60 * np.arange(6) by_time = df.groupby(df.index.time).mean() by_time.plot(xticks=hourly_ticks, style=[':', '--', '-'])`


---

### Running the Project:
- Load dataset using Pandas.
- Run data resampling, rolling mean, and hourly analysis.
- Visualize results clearly.

---

### Tools & Libraries:
- Python, Pandas, Matplotlib, NumPy

---

### License:
- For educational and analytical purposes only.

---

### References:
- Seattle Open Data  
- Pandas Documentation  
- Matplotlib Documentation  
