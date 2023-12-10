CREATE TABLE temp (data jsonb);
COPY temp (data) FROM '/private/tmp/sellerdetails.json';

CREATE TABLE seller_details (
	id int,
	name text,
	created_date double precision,
	email text,
	status char(7),
	PRIMARY KEY(id)
);

INSERT INTO seller_details
SELECT (data->>'id')::integer, (data->>'name')::text, 
	(data->>'createdDate')::double precision, (data->>'email')::text, (data->>'status')::char(7)
FROM temp;

DROP TABLE temp;