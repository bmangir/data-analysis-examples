CREATE TABLE seller_success(
	id SERIAL ,
	seller_id integer,
	created_date_diff interval,
	total_sold_prod integer,
	total_visits integer,
	avg_prod_review decimal,
	avg_seller_score decimal,
	is_successful boolean,
	processed_date integer,
	PRIMARY KEY(id)
);