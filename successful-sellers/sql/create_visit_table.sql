-- Create a temp table to store the input data
CREATE TABLE temp (data jsonb);
COPY temp (data) FROM '/private/tmp/visits.json';

-- Create the actual table
CREATE TABLE visits (
	seller_id int,
	product_id int,
	url text,
	visited_at double precision,
	customer_id int
);

-- Insert the data into visits table
INSERT INTO visits
SELECT (data->>'seller_id')::integer, (data->>'product_id')::integer, (data->>'url')::text,
	(data->>'visited_at')::double precision, (data->>'customer_id')::integer
FROM temp;

-- Delete the whole temp table
DROP TABLE temp;