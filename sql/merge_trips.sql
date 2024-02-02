INSERT INTO tlc_trip_records.f_trips
(
  trip_id,
  vendor_id,
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  passenger_count,
  trip_distance,
  ratecode_id,
  store_and_fwd_flag,
  pickup_location_id,
  dropoff_location_id,
  payment_type_id,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge,
  airport_fee
)
/*
  Select all the columns in the Staging.trips table and perform the data processing steps:
  - Adding the trip_id. 
  For simplicity, use the row numbers of records in the main trips table as the trip_id
  - Identify and cast to null any invalid inputs in the columns: 
  vendor_id, ratecode_id, store_and_fwd_flag, and payment_type_id.
  - Eliminate records where both distance and fare amount values are equal to 0, 
  as these entries are likely due to recording errors.
*/
SELECT
  (SELECT MAX(trip_id) FROM tlc_trip_records.f_trips) + row_number() OVER(),
  CASE WHEN VendorID IN (1, 2) THEN VendorID ELSE NULL END,
  CAST(tpep_pickup_datetime AS DATETIME),
  CAST(tpep_dropoff_datetime AS DATETIME),
  passenger_count,
  trip_distance,
  CASE WHEN RatecodeID IN(1,2,3,4,5,6) THEN RatecodeID ELSE NULL END,
  CASE WHEN store_and_fwd_flag IN ('Y', 'N') THEN store_and_fwd_flag ELSE NULL END,
  PULocationID,
  DOLocationID,
  CASE WHEN payment_type IN(1,2,3,4,5,6) THEN payment_type ELSE NULL END,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge,
  Airport_fee
FROM staging.trips
WHERE trip_distance > 0 OR fare_amount > 0
