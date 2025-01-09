-- Question 3
select count(*) as total
from green_taxi_trips
where date(lpep_pickup_datetime) = '2019-09-18' and date(lpep_dropoff_datetime) = '2019-09-18';

-- Question 4
select max(trip_distance),date(lpep_pickup_datetime)
from green_taxi_trips
group by date(lpep_pickup_datetime)
order by max(trip_distance) desc
limit 1;

-- Question 5
select zl."Borough", sum(gtt.total_amount) as total
from green_taxi_trips gtt 
inner join zone_lookup zl 
on gtt."PULocationID" = zl."LocationID"
where date(gtt.lpep_pickup_datetime)='2019-09-18' and zl."Borough" != 'Unknown'
group by zl."Borough" 
having sum(gtt.total_amount)>50000
order by total desc
limit 3;

-- Question 6
select gtt.tip_amount,zl_do."Zone" as "DOZone"
from green_taxi_trips gtt
inner join zone_lookup zl_pu 
	on gtt."PULocationID" = zl_pu."LocationID"
inner join zone_lookup zl_do 
	on gtt."DOLocationID" = zl_do."LocationID"
where TO_CHAR(gtt.lpep_pickup_datetime, 'YYYY-MM') = '2019-09' and zl_pu."Zone"='Astoria'
order by gtt.tip_amount desc
limit 1;