CREATE TABLE sales_distribution_report(
	order_date DATE,
	seller_id integer,
	ciro numeric,
	basket_count bigint,
	item_count bigint,
	distribution_range integer
);

INSERT INTO sales_distribution_report
SELECT
	DATE(order_date) as order_date,
	seller_id,
	SUM(price) as ciro,
	COUNT(DISTINCT order_id) as basket_count,
	COUNT(item_id) as item_count,
	CASE
		WHEN SUM(price) >= 0 AND SUM(price) < 100 THEN 1
		WHEN SUM(price) >= 100 AND SUM(price) < 500 THEN 2
		WHEN SUM(price) >= 500 AND SUM(price) < 2000 THEN 3
		WHEN SUM(price) >= 2000 THEN 4
	END as distribution_range
FROM baskets
GROUP BY DATE(order_date), seller_id;