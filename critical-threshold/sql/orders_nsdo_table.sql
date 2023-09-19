CREATE TABLE temp (data jsonb);
COPY temp (data) FROM '/private/tmp/orders_nsdo.json';

CREATE TABLE orders (
	order_date timestamp,
	uuid integer,
	product_id integer,
	seller_id integer,
	price decimal,
	unique(uuid)
);

INSERT INTO orders
SELECT (data->>'orderDate')::timestamp, (data->>'uuid')::integer, (data->>'productId')::integer, (data->>'sellerId')::integer, (data->>'price')::decimal
FROM temp;

DROP TABLE temp;