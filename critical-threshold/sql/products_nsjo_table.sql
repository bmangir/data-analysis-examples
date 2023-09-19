CREATE TABLE temp (data jsonb);
COPY temp (data) FROM '/private/tmp/products_nsdo.json';

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

INSERT INTO products
SELECT data->>'uuid', (data->>'id')::integer, data->>'name', data->>'color', data->>'brand', data->>'category', data->>'productSize', (data->>'stock')::integer
FROM temp;

DROP TABLE temp;