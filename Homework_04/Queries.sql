--Create External Table Green
CREATE OR REPLACE EXTERNAL TABLE `woven-nova-447816-e0.trips_data_all.ext_green_tripdata`
(
    VendorID STRING,
    lpep_pickup_datetime STRING,
    lpep_dropoff_datetime STRING,
    store_and_fwd_flag STRING,
    RatecodeID STRING,
    PULocationID STRING,
    DOLocationID STRING,
    passenger_count STRING,
    trip_distance STRING,
    fare_amount STRING,
    extra STRING,
    mta_tax STRING,
    tip_amount STRING,
    tolls_amount STRING,
    ehail_fee STRING,
    improvement_surcharge STRING,
    total_amount STRING,
    payment_type STRING,
    trip_type STRING,
    congestion_surcharge STRING
)
OPTIONS (
    format = 'PARQUET',
    uris = ['gs://ny-taxi-woven-nova-447816-e0-bucket/green/*.parquet']
);

--Create Table green_tripdata
CREATE OR REPLACE TABLE `woven-nova-447816-e0.trips_data_all.green_tripdata` AS 
SELECT
    VendorID,
    TIMESTAMP(lpep_pickup_datetime) AS lpep_pickup_datetime,
    TIMESTAMP(lpep_dropoff_datetime) AS lpep_dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID,
    PULocationID,
    DOLocationID,
    CAST(passenger_count AS INT64) AS passenger_count,
    CAST(trip_distance AS NUMERIC) AS trip_distance,
    CAST(fare_amount AS NUMERIC) AS fare_amount,
    CAST(extra AS NUMERIC) AS extra,
    CAST(mta_tax AS NUMERIC) AS mta_tax,
    CAST(tip_amount AS NUMERIC) AS tip_amount,
    CAST(tolls_amount AS NUMERIC) AS tolls_amount,
    CAST(ehail_fee AS NUMERIC) AS ehail_fee,
    CAST(improvement_surcharge AS NUMERIC) AS improvement_surcharge,
    CAST(total_amount AS NUMERIC) AS total_amount,
    CAST(payment_type AS INT64) AS payment_type,
    trip_type,
    CAST(congestion_surcharge AS NUMERIC) AS congestion_surcharge
FROM `woven-nova-447816-e0.trips_data_all.ext_green_tripdata`;

--Create External Table yellow
CREATE OR REPLACE EXTERNAL TABLE `woven-nova-447816-e0.trips_data_all.ext_yellow_tripdata`
(
    VendorID STRING,
    tpep_pickup_datetime STRING,
    tpep_dropoff_datetime STRING,
    store_and_fwd_flag STRING,
    RatecodeID STRING,
    PULocationID STRING,
    DOLocationID STRING,
    passenger_count STRING,
    trip_distance STRING,
    fare_amount STRING,
    extra STRING,
    mta_tax STRING,
    tip_amount STRING,
    tolls_amount STRING,
    ehail_fee STRING,
    improvement_surcharge STRING,
    total_amount STRING,
    payment_type STRING,
    trip_type STRING,
    congestion_surcharge STRING
)
OPTIONS (
    format = 'PARQUET',
    uris = ['gs://ny-taxi-woven-nova-447816-e0-bucket/yellow/*.parquet']
);

--Create Table yellow_tripdata
CREATE OR REPLACE TABLE `woven-nova-447816-e0.trips_data_all.yellow_tripdata`
AS 
SELECT
    VendorID,
    TIMESTAMP(tpep_pickup_datetime) AS tpep_pickup_datetime,
    TIMESTAMP(tpep_dropoff_datetime) AS tpep_dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID,
    PULocationID,
    DOLocationID,
    CAST(passenger_count AS INT64) AS passenger_count,
    CAST(trip_distance AS NUMERIC) AS trip_distance,
    CAST(fare_amount AS NUMERIC) AS fare_amount,
    CAST(extra AS NUMERIC) AS extra,
    CAST(mta_tax AS NUMERIC) AS mta_tax,
    CAST(tip_amount AS NUMERIC) AS tip_amount,
    CAST(tolls_amount AS NUMERIC) AS tolls_amount,
    CAST(ehail_fee AS NUMERIC) AS ehail_fee,
    CAST(improvement_surcharge AS NUMERIC) AS improvement_surcharge,
    CAST(total_amount AS NUMERIC) AS total_amount,
    CAST(payment_type AS INT64) AS payment_type,
    trip_type, 
    CAST(congestion_surcharge AS NUMERIC) AS congestion_surcharge
FROM `woven-nova-447816-e0.trips_data_all.ext_yellow_tripdata`;

--Create External Table fhv
CREATE OR REPLACE EXTERNAL TABLE `woven-nova-447816-e0.trips_data_all.ext_fhv_tripdata`
(
    dispatching_base_num STRING,
    pickup_datetime STRING,
    dropOff_datetime STRING,
    PUlocationID STRING,
    DOlocationID STRING,
    SR_Flag STRING,
    Affiliated_base_number STRING
)
OPTIONS (
    format = 'PARQUET',
    uris = ['gs://ny-taxi-woven-nova-447816-e0-bucket/fhv/*.parquet']
);

--Create Table fvh_tripdata
CREATE OR REPLACE TABLE `woven-nova-447816-e0.trips_data_all.fhv_tripdata_internal` AS
SELECT 
    dispatching_base_num,
    TIMESTAMP(pickup_datetime) AS pickup_datetime,
    TIMESTAMP(dropOff_datetime) AS dropOff_datetime,
    SAFE_CAST(PUlocationID AS INT64) AS PUlocationID,
    SAFE_CAST(DOlocationID AS INT64) AS DOlocationID,
    SR_Flag,
    Affiliated_base_number
FROM `woven-nova-447816-e0.trips_data_all.ext_fhv_tripdata`;
