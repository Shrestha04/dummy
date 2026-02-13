LOAD DATA LOCAL INFILE 'C:/path/green_taxi.csv'
INTO TABLE green_taxi_data
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
 VendorID,
 lpep_pickup_datetime,
 lpep_dropoff_datetime,
 store_and_fwd_flag,
 RatecodeID,
 PULocationID,
 DOLocationID,
 passenger_count,
 trip_distance,
 fare_amount,
 extra,
 mta_tax,
 tip_amount,
 tolls_amount,
 ehail_fee,
 improvement_surcharge,
 total_amount,
 payment_type,
 trip_type,
 congestion_surcharge
);


    
