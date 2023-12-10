-- Create a temp table to store the input data
CREATE TABLE temp (data jsonb);
COPY temp (data) FROM '/private/tmp/sellerdetails.json';

-- Create the actual table
CREATE TABLE seller_details (
	id int,
	name text,
	created_date double precision,
	email text,
	status char(7),
	PRIMARY KEY(id)
);

-- Insert the data into seller_details table
INSERT INTO seller_details
SELECT (data->>'id')::integer, (data->>'name')::text, 
	(data->>'createdDate')::double precision, (data->>'email')::text, (data->>'status')::char(7)
FROM temp;

-- Delete the whole temp table
DROP TABLE temp;