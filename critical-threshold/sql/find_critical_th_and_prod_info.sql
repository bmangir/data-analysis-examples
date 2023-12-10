-- Find the critical threshold for daily results of sellers' sales and insert them
-- Assume that this works with distribution systems
-- I do not use epoch time for this project
INSERT INTO result_table
SELECT
	dsi.seller_id,
	dsi.product_id,
	dsi.daily_average_sale_quantity,
	dsi.daily_average_sale_price,
	ROUND((p.stock / dsi.daily_average_sale_quantity), 3) as critical_threshold,
	p.name,
	p.brand,
	p.category,
	p.product_size,
	p.stock
FROM daily_sales_info dsi
JOIN products p ON dsi.product_id = p.id;