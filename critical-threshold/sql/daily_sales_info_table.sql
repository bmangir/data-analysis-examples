CREATE TABLE daily_sales_info(
	seller_id integer,
	product_id integer,
	daily_average_sale_quantity numeric,
	daily_average_sale_price numeric
);

INSERT INTO daily_sales_info
SELECT
    seller_id,
    product_id,
    ROUND((COUNT(product_id)::decimal / 30), 3) as daily_average_sale_quantity,
    ROUND((SUM(price)::decimal / 30), 3) as daily_average_sale_price
FROM orders
WHERE order_date >= '2021-06-07 00:00:00' AND order_date <= '2021-07-07 23:59:59'
GROUP BY seller_id, product_id;
