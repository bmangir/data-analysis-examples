-- Create a temp table to store the input data
CREATE TABLE temp (data jsonb);
COPY temp (data) FROM '/private/tmp/sellerscores.json';

-- Create the actual table
CREATE TABLE seller_scores (
	seller_id int,
	score decimal,
	calculated_at double precision
);

-- Insert the data into seller_scores table
INSERT INTO seller_scores
SELECT (data->>'seller_id')::integer, (data->>'score')::decimal, 
	(data->>'calculated_at')::double precision
FROM temp;

-- Delete the whole temp table
DROP TABLE temp;