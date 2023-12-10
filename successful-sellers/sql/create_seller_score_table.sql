CREATE TABLE temp (data jsonb);
COPY temp (data) FROM '/private/tmp/sellerscores.json';

CREATE TABLE seller_scores (
	seller_id int,
	score decimal,
	calculated_at double precision
);

INSERT INTO seller_scores
SELECT (data->>'seller_id')::integer, (data->>'score')::decimal, 
	(data->>'calculated_at')::double precision
FROM temp;

DROP TABLE temp;