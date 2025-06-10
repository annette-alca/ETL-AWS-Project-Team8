-- CREATE DATABASE warehouse_test;
\c warehouse_test;

DELETE FROM dim_date;DELETE FROM dim_location;DELETE FROM dim_design;DELETE FROM dim_staff;DELETE FROM dim_currency;DELETE FROM dim_counterparty;DELETE FROM fact_sales_order;


SELECT * FROM dim_date LIMIT 3;SELECT * FROM dim_location LIMIT 3;SELECT * FROM dim_design LIMIT 3;SELECT * FROM dim_staff LIMIT 3;SELECT * FROM dim_currency LIMIT 3;SELECT * FROM dim_counterparty LIMIT 3;
SELECT * FROM fact_sales_order LIMIT 3;
