-- Create a temp table to store the input data
CREATE TABLE temp (data jsonb);
COPY temp (data) FROM '/private/tmp/orders.json';

-- Create the actual table
CREATE TABLE orders (
	order_date timestamp,
	order_id integer,
	seller_id integer,
	item_id integer,
	category_id integer,
	price decimal
);

-- Insert the data into orders table
INSERT INTO orders
SELECT (data->>'orderDate')::timestamp, (data->>'orderId')::integer, (data->>'sellerId')::integer, (data->>'itemId')::integer, (data->>'categoryId')::integer, (data->>'price')::decimal
FROM temp;

-- Delete the whole temp table
DROP TABLE temp;