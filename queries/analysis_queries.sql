# Average fare amount by each vendor
SELECT avg(t.fare_amount) AS avg_fare_amount, v.vendor_info, v.vendor_id
FROM `taxi-data-elt-project-394401.taxi_dataset.fact_trips` t
    JOIN `taxi-data-elt-project-394401.taxi_dataset.dim_vendor` v ON t.vendor_id = v.vendor_id
GROUP BY v.vendor_id, v.vendor_info;

# Average tip amount by each payment type
SELECT avg(t.tip_amount) AS avg_tip_amount, p.payment_type_id, p.payment_type
FROM `taxi-data-elt-project-394401.taxi_dataset.fact_trips` t
    JOIN `taxi-data-elt-project-394401.taxi_dataset.dim_payment_type` p ON t.payment_type_id = p.payment_type_id
GROUP BY p.payment_type_id, p.payment_type;

# Find total number of trips by passenger count
SELECT count(t.trip_id) AS num_trips, c.passenger_count
FROM `taxi-data-elt-project-394401.taxi_dataset.fact_trips` t
    JOIN `taxi-data-elt-project-394401.taxi_dataset.dim_passenger_count` c ON t.passenger_count_id = c.passenger_count_id
GROUP BY c.passenger_count;

# Find top 5 pick up hours of the day by average congestion surcharge
SELECT avg(t.congestion_surcharge) AS avg_congestion_charge, d.pick_up_hour
FROM `taxi-data-elt-project-394401.taxi_dataset.fact_trips` t
    JOIN `taxi-data-elt-project-394401.taxi_dataset.dim_datetime` d ON t.datetime_id = d.datetime_id
GROUP BY d.pick_up_hour
ORDER BY avg_congestion_charge DESC LIMIT 5;