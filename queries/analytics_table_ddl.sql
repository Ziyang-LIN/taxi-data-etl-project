CREATE OR REPLACE TABLE `taxi-data-elt-project-394401.taxi_dataset.taxi_analytics_tbl` AS (
  SELECT 
    t.trip_id,
    v.vendor_info,
    r.rate_code_type,
    p.payment_type,
    c.passenger_count,
    t.pick_up_location_id,
    t.drop_off_location_id,
    d.tpep_pickup_datetime,
    d.tpep_dropoff_datetime,
    t.trip_distance,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.congestion_surcharge,
    t.improvement_surcharge,
    t.airport_fee,
    t.total_amount
  FROM `taxi-data-elt-project-394401.taxi_dataset.fact_trips` t 
    JOIN `taxi-data-elt-project-394401.taxi_dataset.dim_datetime` d ON t.datetime_id = d.datetime_id
    JOIN `taxi-data-elt-project-394401.taxi_dataset.dim_vendor` v ON t.vendor_id = v.vendor_id
    JOIN `taxi-data-elt-project-394401.taxi_dataset.dim_rate_code` r ON t.rate_code_id = r.rate_code_id
    JOIN `taxi-data-elt-project-394401.taxi_dataset.dim_payment_type` p ON t.payment_type_id = p.payment_type_id
    JOIN `taxi-data-elt-project-394401.taxi_dataset.dim_passenger_count` c ON t.passenger_count_id = c.passenger_count_id
);