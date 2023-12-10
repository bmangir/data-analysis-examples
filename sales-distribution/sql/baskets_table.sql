-- Create a temp table to store the input data
CREATE TABLE temp (data jsonb);
COPY temp (data) FROM 'orders_input.json';

-- Create the actual table
CREATE TABLE baskets (
	order_date timestamp,
	order_id integer,
	seller_id integer,
	item_id integer,
	category_id integer,
	price decimal,
	status text
);

-- Insert the data into baskets table
INSERT INTO baskets
SELECT (data->>'orderDate')::timestamp, (data->>'orderId')::integer, (data->>'sellerId')::integer, (data->>'itemId')::integer, (data->>'categoryId')::integer, (data->>'price')::decimal, data->>'status'
FROM temp;

-- Delete the whole temp table
DROP TABLE temp;