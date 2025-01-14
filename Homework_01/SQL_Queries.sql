-- Question 3
select
  count(case when trip_distance <= 1 then 1 end) as "1",
  count(case when trip_distance > 1 and trip_distance <= 3 then 1 end) as "2",
  count(case when trip_distance > 3 and trip_distance <= 7 then 1 end) as "3",
  count(case when trip_distance > 7 and trip_distance <= 10 then 1 end) as "4",
  count(case when trip_distance > 10 then 1 end) as "5"
from green_taxi_trips
where date(lpep_pickup_datetime) >= '2019-10-01' and date(lpep_pickup_datetime) < '2019-11-01'
and date(lpep_dropoff_datetime) >= '2019-10-01' and date(lpep_dropoff_datetime) < '2019-11-01';

-- Question 4
select max(trip_distance),date(lpep_pickup_datetime)
from green_taxi_trips
group by date(lpep_pickup_datetime)
order by max(trip_distance) desc
limit 1;

-- Question 5
select zl."Zone" , sum(gtt.total_amount) as total
from green_taxi_trips gtt 
inner join zone_lookup zl 
on gtt."PULocationID" = zl."LocationID"
where date(gtt.lpep_pickup_datetime)='2019-10-18' and zl."Zone" != 'Unknown'
group by zl."Zone" 
having sum(gtt.total_amount)>13000
order by total desc
limit 3;

-- Question 6
select gtt.tip_amount,zl_do."Zone" as "DOZone"
from green_taxi_trips gtt
inner join zone_lookup zl_pu 
	on gtt."PULocationID" = zl_pu."LocationID"
inner join zone_lookup zl_do 
	on gtt."DOLocationID" = zl_do."LocationID"
where TO_CHAR(gtt.lpep_pickup_datetime, 'YYYY-MM') = '2019-10' and zl_pu."Zone"='East Harlem North'
order by gtt.tip_amount desc
limit 1;