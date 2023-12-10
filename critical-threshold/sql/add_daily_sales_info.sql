-- Add the result in output table where between the current date and its before 30 days which is 1 month
-- For each moth the date is change(Assume this works with distribution systems)
-- Run this query daily
INSERT INTO daily_sales_info
SELECT
    seller_id,
    product_id,
    ROUND((COUNT(product_id)::decimal / 30), 3) as daily_average_sale_quantity,
    ROUND((SUM(price)::decimal / 30), 3) as daily_average_sale_price
FROM orders
WHERE order_date >= '2021-06-07 00:00:00' AND order_date <= '2021-07-07 23:59:59'
GROUP BY seller_id, product_id;