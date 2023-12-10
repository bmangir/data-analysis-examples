-- Create a temp table to store the input data
CREATE TABLE temp (data jsonb);
COPY temp (data) FROM '/private/tmp/productreviews.json';

-- Create the actual table
CREATE TABLE product_reviews (
	seller_id int,
	product_id int,
	rate decimal,
	review text,
	created_date double precision,
	last_modified_date double precision,
	status char(20)
);

-- Insert the data into product_reviews table
INSERT INTO product_reviews
SELECT (data->>'seller_id')::integer, (data->>'product_id')::integer, (data->>'rate')::decimal, (data->>'review')::text, 
	(data->>'created_date')::double precision, (data->>'last_modified_date')::double precision, (data->>'status')::char(20)
FROM temp;

-- Delete the whole temp table
DROP TABLE temp;