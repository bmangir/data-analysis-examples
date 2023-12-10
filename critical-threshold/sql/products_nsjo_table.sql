-- Crate temporary table
CREATE TABLE temp (data jsonb);
COPY temp (data) FROM '/private/tmp/products_nsdo.json';

-- Create actual table
CREATE TABLE products (
	uuid text,
	id integer,
	name text,
	color text,
	brand text,
	category text,
	product_size text,
	stock integer,
	PRIMARY KEY(uuid)
);

-- Insert the input data from temp table into products table
INSERT INTO products
SELECT data->>'uuid', (data->>'id')::integer, data->>'name', data->>'color', data->>'brand', data->>'category', data->>'productSize', (data->>'stock')::integer
FROM temp;

-- Delete the temporary table
DROP TABLE temp;