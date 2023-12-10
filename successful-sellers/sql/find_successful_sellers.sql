-- Run this query every 24 hours
INSERT INTO seller_success(seller_id, created_last_year, total_sold_prod, total_visits, avg_prod_review, avg_seller_score, is_successful, processed_date)
	SELECT
		SD.id as seller_id,
		(NOW() - to_timestamp(SD.created_date/1000)) as created_date_diff,
		O.total_sold_prod as total_sold_prod,
		V.total_visit,
		ROUND(PR.avg_prod_review, 2),
		ROUND(SC.avg_seller_score, 2),
		CASE
			WHEN (NOW() - to_timestamp(SD.created_date/1000)) <= interval '1 year'
				AND O.total_sold_prod >= 4
				AND V.total_visit >= 3
				AND ROUND(PR.avg_prod_review, 2) >= 2.5
				AND ROUND(SC.avg_seller_score,2) >= 5 THEN True
			ELSE False
		END as is_successful,
		EXTRACT(EPOCH FROM CURRENT_DATE) as processed_date
	FROM seller_details SD
	LEFT JOIN (SELECT
			   O.seller_id,
			   COUNT(O.item_id) as total_sold_prod
			   FROM orders O
			   GROUP BY O.seller_id) as O ON SD.id = O.seller_id
	LEFT JOIN (SELECT
			   V.seller_id,
			   COUNT(V.customer_id) as total_visit
			   FROM visits V
			   GROUP BY V.seller_id) V ON SD.id = V.seller_id
	LEFT JOIN (SELECT
				PR.seller_id,
				AVG(PR.rate) as avg_prod_review
				FROM product_reviews PR
				WHERE PR.status = 'APPROVED'
				GROUP BY PR.seller_id) PR ON SD.id = PR.seller_id
	LEFT JOIN (SELECT
				SC.seller_id,
				AVG(SC.score) as avg_seller_score
				FROM seller_scores SC
				GROUP BY SC.seller_id) SC ON SD.id = SC.seller_id;
