-- Crate temporary table
CREATE TABLE temp (data jsonb);
COPY temp (data) FROM '/private/tmp/orders_nsdo.json';

-- Create actual table
CREATE TABLE orders (
	order_date timestamp,
	uuid integer,
	product_id integer,
	seller_id integer,
	price decimal,
	unique(uuid)
);

-- Insert the input data from temp table into orders table
INSERT INTO orders
SELECT (data->>'orderDate')::timestamp, (data->>'uuid')::integer, (data->>'productId')::integer, (data->>'sellerId')::integer, (data->>'price')::decimal
FROM temp;

-- Delete the temporary table
DROP TABLE temp;