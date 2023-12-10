CREATE TABLE temp (data jsonb);
COPY temp (data) FROM '/private/tmp/orders.json';

CREATE TABLE orders (
	order_date timestamp,
	order_id integer,
	seller_id integer,
	item_id integer,
	category_id integer,
	price decimal
);

INSERT INTO orders
SELECT (data->>'orderDate')::timestamp, (data->>'orderId')::integer, (data->>'sellerId')::integer, (data->>'itemId')::integer, (data->>'categoryId')::integer, (data->>'price')::decimal
FROM temp;

DROP TABLE temp;