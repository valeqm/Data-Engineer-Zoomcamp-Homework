# Module 5 Homework

"THIS README FILE HAS BEEN MODIFIED TO INCLUDE THE STEPS AND SOLUTIONS FOR THE HOMEWORK."

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the Yellow 2024-10 data from the official website: 

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet
```


## Question 1: Install Spark and PySpark

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?

> [!NOTE]
> To install PySpark follow this [guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/05-batch/setup/pyspark.md)

```bash
3.5.5
```


## Question 2: Yellow October 2024

Read the October 2024 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- 6MB
- 25MB ✅
- 75MB
- 100MB

```bash
total 93M
-rw-r--r-- 1 root root 24M Mar  7 04:37 part-00000-0a606d33-92df-434c-8ed0-99f32af1b85d-c000.snappy.parquet
-rw-r--r-- 1 root root 24M Mar  7 04:37 part-00001-0a606d33-92df-434c-8ed0-99f32af1b85d-c000.snappy.parquet
-rw-r--r-- 1 root root 24M Mar  7 04:37 part-00002-0a606d33-92df-434c-8ed0-99f32af1b85d-c000.snappy.parquet
-rw-r--r-- 1 root root 24M Mar  7 04:37 part-00003-0a606d33-92df-434c-8ed0-99f32af1b85d-c000.snappy.parquet
-rw-r--r-- 1 root root   0 Mar  7 04:37 _SUCCESS
```

## Question 3: Count records 

How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.

- 85,567
- 105,567
- 125,567 ✅
- 145,567

```bash
spark.sql("""
Select count(*) from trips_data where date(tpep_pickup_datetime) == '2024-10-15'
""").show()
```

## Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

- 122
- 142
- 162 ✅
- 182

```bash
spark.sql("""
select (UNIX_TIMESTAMP(tpep_dropoff_datetime) - UNIX_TIMESTAMP(tpep_pickup_datetime)) / 3600 AS trip_duration_hours
from trips_data
order by trip_duration_hours desc
limit 1
""").show()
```

## Question 5: User Interface

Spark’s User Interface which shows the application's dashboard runs on which local port?

- 80
- 443
- 4040 ✅
- 8080


## Question 6: Least frequent pickup location zone

Load the zone lookup data into a temp view in Spark:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Using the zone lookup data and the Yellow October 2024 data, what is the name of the LEAST frequent pickup location Zone?

- Governor's Island/Ellis Island/Liberty Island ✅
- Arden Heights
- Rikers Island
- Jamaica Bay

```bash
spark.sql("""
select zone
from zone_lookup z
join trips_data t
on z.LocationID = t.PULocationID
group by zone
order by count(*) asc
limit 1
""").show()
```

## Submitting the solutions

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2025/homework/hw5
- Deadline: See the website
