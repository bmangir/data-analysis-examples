CREATE TABLE temp (data jsonb);
COPY temp (data) FROM '/private/tmp/visits.json';

CREATE TABLE visits (
	seller_id int,
	product_id int,
	url text,
	visited_at double precision,
	customer_id int
);

INSERT INTO visits
SELECT (data->>'seller_id')::integer, (data->>'product_id')::integer, (data->>'url')::text,
	(data->>'visited_at')::double precision, (data->>'customer_id')::integer
FROM temp;

DROP TABLE temp;