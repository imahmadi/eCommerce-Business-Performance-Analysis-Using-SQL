-------------------------------------------------------------------------------
---------------------------                     -------------------------------
--------------------------   DATA PREPARATION   -------------------------------
--------------------------                      -------------------------------
-------------------------------------------------------------------------------

--- Make New Table to Remove Duplicate Zip Code ---

CREATE TABLE geolocation_dataset_new AS (
SELECT geolocation_zip_code_prefix,
       MAX(CASE WHEN seqnum = 1 THEN geolocation_lat END) AS geolocation_lat,
	   MAX(CASE WHEN seqnum = 1 THEN geolocation_lng END) AS geolocation_lng,
	   MAX(CASE WHEN seqnum = 1 THEN geolocation_city END) AS geolocation_city,
	   MAX(CASE WHEN seqnum = 1 THEN geolocation_state end) AS geolocation_state
FROM (SELECT geolocation_zip_code_prefix, 
	  		geolocation_lat, 
	  		geolocation_lng, 
	  		geolocation_city, 
	  		geolocation_state, 
	  		COUNT(*) as cnt,
	  		ROW_NUMBER() OVER (PARTITION BY geolocation_zip_code_prefix ORDER BY COUNT(*) DESC) AS seqnum
      FROM geolocation_dataset
      GROUP BY geolocation_zip_code_prefix, 
	  			geolocation_lat, 
	  			geolocation_lng, 
	  			geolocation_city, 
	  			geolocation_state
     ) cc
GROUP BY geolocation_zip_code_prefix
);

--- Replace geolocation_dataset With The Newer Version ---

ALTER TABLE geolocation_dataset RENAME TO geolocation_dataset_old;
ALTER TABLE geolocation_dataset_new RENAME TO geolocation_dataset;
DROP TABLE geolocation_dataset_old;

COMMIT;

--- Check ---

SELECT *
FROM geolocation_dataset;


-------------------------------------------------------------------------------
---------------                                              ------------------
---------------   ANNUAL CUSTOMER ACTIVITY GROWTH ANALYSIS   ------------------
---------------                                              ------------------
-------------------------------------------------------------------------------

-----------  ------------------------------------------------
-------- SUMMARY  -------------------------------------------
-----------  ------------------------------------------------

--- To simplified the query, I would make view table named 'summary'
--- for further analysis, I would only using ordet_id that appear in 'order_items_dataset' table, 
--- because in my opinion it's strange if order id's doesn't have a details about the ordered items 
--- even if the transaction are canceled midway


CREATE VIEW summary AS
WITH 
t1
AS
(
	SELECT orders_dataset.order_id,
			orders_dataset.customer_id,
			orders_dataset.order_status,
			orders_dataset.order_purchase_timestamp,
			orders_dataset.order_approved_at,
			orders_dataset.order_estimated_delivery_date,
			customers_dataset.customer_unique_id,
			customers_dataset.customer_city,
			customers_dataset.customer_state
	FROM orders_dataset
	LEFT JOIN customers_dataset
	ON orders_dataset.customer_id = customers_dataset.customer_id
),
t2
AS
(
	SELECT order_id, 
		COUNT(payment_sequential) AS total_payment_sequential, 
		SUM(payment_installments) AS total_payment_installments, 
		SUM(payment_value) AS total_payment_value 
	FROM public.order_payments_dataset
	GROUP BY order_id
),
t3 
AS 
(
	SELECT order_items.order_id,
			order_items.product_id,
			order_items.order_item_id,
			order_items.seller_id,
			product.product_category_name AS product_category,
			order_items.price,
			order_items.freight_value AS shipping_cost,
			ROUND((order_items.price + order_items.freight_value)::numeric,2) AS total_cost,
			orders.order_status,
			review.review_score,
			orders.order_purchase_timestamp AS purchase_date,
			orders.order_approved_at AS approved_date,
			orders.order_estimated_delivery_date AS est_delivery_date,
			order_items.shipping_limit_date AS shipping_limit,
			product.product_weight_g,
			sellers.seller_city,
			sellers.seller_state,
			orders.customer_unique_id,
			orders.customer_city,
			orders.customer_state,
			payment.total_payment_sequential, 
			payment.total_payment_installments, 
			payment.total_payment_value 
	FROM order_items_dataset AS order_items
	LEFT JOIN t1 AS orders
		ON order_items.order_id = orders.order_id
	LEFT JOIN product_dataset AS product
		ON order_items.product_id = product.product_id
	LEFT JOIN sellers_dataset AS sellers
		ON order_items.seller_id = sellers.seller_id
	LEFT JOIN t2 AS payment
		ON order_items.order_id = payment.order_id
	LEFT JOIN order_reviews_dataset AS review
		ON order_items.order_id = review.order_id
)

SELECT *
FROM t3;


------------------------------------------------------------------
------ Show Monthly Avg. Active Customer, New Customer, ----------
---- Repeat Order Customer, and Avg. Order for Each year ---------
------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS yearly_customer AS
WITH monthly_customer AS 
(
	SELECT DATE_PART('year', purchase_date) AS date_year,
			DATE_PART('month', purchase_date) AS date_month, 
			COUNT(DISTINCT(customer_unique_id)) AS total_customer
	FROM summary
	GROUP BY DATE_PART('year', purchase_date),
			DATE_PART('month', purchase_date)
),
avg_monthly_customer_each_year --- 01 Monthly Avg. Customer Each Year ---
AS
(
	SELECT date_year, 
			ROUND(AVG(total_customer),2) AS avg_monthly_customer
	FROM monthly_customer
	GROUP BY date_year
),
first_order AS 
(
	SELECT DISTINCT ON(customer_unique_id) 
	customer_unique_id, purchase_date
	FROM summary
),
yearly_new_customer --- 02 Yearly New Customer ---
AS
(
	SELECT DATE_PART('year', purchase_date) AS date_year, 
			COUNT(customer_unique_id) AS new_customer
	FROM first_order 
	GROUP BY DATE_PART('year', purchase_date)
	ORDER BY DATE_PART('year', purchase_date)
),
customer_who_reorder AS
(
	SELECT DATE_PART('year', purchase_date) AS date_year, 
			customer_unique_id, 
			COUNT(*) AS total_order
	FROM summary
	GROUP BY DATE_PART('year', purchase_date), 
			customer_unique_id
	HAVING COUNT(*) > 1
),
yearly_reorder_customer --- 03 Yearlt Total Reorder Customer ---
AS 
(
	SELECT date_year, 
			COUNT(customer_unique_id) AS repeat_order_cust
	FROM customer_who_reorder
	GROUP BY date_year
	ORDER BY date_year ASC
),
yearly_customer_order AS
(
	SELECT DATE_PART('year', purchase_date) AS date_year, 
			customer_unique_id, 
			COUNT(*) AS yearly_order
	FROM summary
	GROUP BY DATE_PART('year', purchase_date), 
			customer_unique_id
),
yearly_avg_order --- 04 Yearly Avg. Order ---
AS
(
	SELECT date_year, 
			ROUND(AVG(yearly_order),3) AS avg_order
	FROM yearly_customer_order
	GROUP BY date_year
	ORDER BY date_year
)


SELECT t1.date_year, 
		t1.avg_monthly_customer, 
		t2.new_customer, 
		t3.repeat_order_cust, 
		t4.avg_order
FROM avg_monthly_customer_each_year AS t1
LEFT JOIN yearly_new_customer AS t2 
	ON t1.date_year = t2.date_year
LEFT JOIN yearly_reorder_customer AS t3
	ON t1.date_year = t3.date_year
LEFT JOIN yearly_avg_order AS t4
	ON t1.date_year = t4.date_year;


------------------------------------------------------------------
-------------------- Customer Monthly ----------------------------
------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS monthly_customer AS
WITH monthly_all_customer 
AS 
(
	SELECT DATE_PART('year', purchase_date) AS date_year,
			DATE_PART('month', purchase_date) AS date_month, 
			COUNT(DISTINCT(customer_unique_id)) AS total_customer
	FROM summary
	GROUP BY DATE_PART('year', purchase_date),
			DATE_PART('month', purchase_date)
),
first_order 
AS 
(
	SELECT DISTINCT ON(customer_unique_id) 
	customer_unique_id, purchase_date
	FROM summary
),
monthly_new_customer 
AS
(
	SELECT DATE_PART('year', purchase_date) AS date_year,
			DATE_PART('month', purchase_date) AS date_month, 
			COUNT(customer_unique_id) AS new_customer
	FROM first_order 
	GROUP BY DATE_PART('year', purchase_date), 
			DATE_PART('month', purchase_date)
	ORDER BY DATE_PART('year', purchase_date), 
			DATE_PART('month', purchase_date)
)

SELECT all_cust.date_year, 
		all_cust.date_month, 
		new_cust.new_customer,
		all_cust.total_customer
FROM monthly_all_customer AS all_cust
LEFT JOIN monthly_new_customer AS new_cust
ON all_cust.date_year = new_cust.date_year AND all_cust.date_month = new_cust.date_month;


--- Change Null Values into 0 ---
UPDATE monthly_customer
SET new_customer = 0
WHERE new_customer IS NULL;

--- make old_customer column ---
CREATE TABLE IF NOT EXISTS monthly_customer_new AS
SELECT date_year, 
		date_month,
		new_customer,
		(total_customer - new_customer) AS old_customer,
		total_customer
FROM monthly_customer
ORDER BY date_year ASC, date_month ASC;

--- Replace the old one ---

ALTER TABLE monthly_customer RENAME TO monthly_customer_old;
ALTER TABLE monthly_customer_new RENAME TO monthly_customer;
DROP TABLE monthly_customer_old;

------------------------------------------------------------------
--------------- YEARLY TRANSACTION STATS   -----------------------
------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS yearly_transaction AS
WITH day_diff_between_order AS
(
	SELECT customer_unique_id, 
			purchase_date,
			lead(purchase_date) over (partition by customer_unique_id order by customer_unique_id),
			ABS(lead(purchase_date) over (partition by customer_unique_id order by customer_unique_id) - purchase_date - 1) as tau
	FROM summary
	ORDER BY customer_unique_id, purchase_date
),
yearly_day_diff AS
(
	SELECT DATE_PART('year', purchase_date) AS date_year, 
			ROUND(AVG(tau), 2) AS day_diff
	FROM day_diff_between_order
	WHERE tau IS NOT NULL
	GROUP BY DATE_PART('year', purchase_date)
	ORDER BY DATE_PART('year', purchase_date) ASC
),
yearly_stats AS
(
	SELECT date_year, 
			MAX(total_order) AS highest_transaction_by_user,
			ROUND(AVG(total_order),2) AS avg_total_transaction,
			MODE() WITHIN GROUP (ORDER BY total_order) AS mode_of_transaction,
			ROUND(AVG(is_1order)*100, 2) AS percentage_1transaction
	FROM (SELECT DATE_PART('year', purchase_date) AS date_year, 
		  		customer_unique_id, COUNT(*) AS total_order,
		  		CASE 
		  		WHEN COUNT(*) = 1 THEN 1
		 		 ELSE 0
		  		END is_1order
			FROM summary
			GROUP BY DATE_PART('year', purchase_date), customer_unique_id) yearly_max
	GROUP BY date_year
),
avg_unique AS
(
	SELECT date_year,
			ROUND(AVG(total_items),2) AS avg_unique_items_each_transaction
	FROM (SELECT DATE_PART('year', purchase_date) AS date_year,
				order_id, 
				COUNT(*) AS total_items
			FROM summary
			GROUP BY DATE_PART('year', purchase_date),
					order_id) AS t1
	GROUP BY date_year
)

SELECT stats.date_year,
		day_diff.day_diff,
		stats.highest_transaction_by_user,
		stats.avg_total_transaction,
		stats.percentage_1transaction,
		avg_unique.avg_unique_items_each_transaction,
		rating.avg_rating
FROM yearly_stats AS stats
LEFT JOIN yearly_day_diff AS day_diff
	ON stats.date_year =  day_diff.date_year
LEFT JOIN avg_unique
	ON stats.date_year =  avg_unique.date_year
LEFT JOIN (SELECT DATE_PART('year', purchase_date) AS date_year,
					ROUND(AVG(review_score), 2) AS avg_rating
			FROM summary
			GROUP BY DATE_PART('year', purchase_date)) AS rating
	ON stats.date_year =  rating.date_year;
	

------------------------------------------------------------------
----------------------   CUSTOMER ORIGIN  ------------------------
------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS origin_customer AS
WITH total_city_state AS
(
	SELECT DATE_PART('year', est_delivery_date) AS date_year,
			COUNT(DISTINCT(customer_city)) AS total_city,
			COUNT(DISTINCT(customer_state)) AS total_state
	FROM summary
	GROUP BY DATE_PART('year', est_delivery_date)
),
highest_state AS
(
	SELECT *
	FROM (SELECT DATE_PART('year', purchase_date) AS date_year,
				customer_state AS highest_state,
				COUNT(*) AS total_transaction_in_state,
				RANK() OVER (PARTITION BY DATE_PART('year', purchase_date) ORDER BY COUNT(*) DESC) AS yearly_rank
		FROM summary
		GROUP BY DATE_PART('year', purchase_date),
					customer_state
		ORDER BY DATE_PART('year', purchase_date) ASC,
					COUNT(*) DESC) AS t1
	WHERE yearly_rank <= 1
),
highest_city AS
(
	SELECT *
	FROM (SELECT DATE_PART('year', purchase_date) AS date_year,
				customer_city AS highest_city,
				COUNT(*) AS total_transaction_in_city,
				RANK() OVER (PARTITION BY DATE_PART('year', purchase_date) ORDER BY COUNT(*) DESC) AS yearly_rank
		FROM summary
		GROUP BY DATE_PART('year', purchase_date),
					customer_city
		ORDER BY DATE_PART('year', purchase_date) ASC,
					COUNT(*) DESC) AS t1
	WHERE yearly_rank <= 1
),
percentage_state_city AS
(
	SELECT DATE_PART ('year', est_delivery_date) AS date_year,
			COUNT(*) AS total_transaction,
			ROUND(AVG(is_hstate)*100,2) AS state_percentage_from_total,
			ROUND(AVG(is_hcity)*100,2) AS city_percentage_from_total
	FROM (SELECT est_delivery_date,
				CASE
				WHEN customer_state = 'SP' THEN 1
				ELSE 0
				END AS is_hstate,
				CASE
				WHEN customer_city = 'sao paulo' THEN 1
				ELSE 0
				END AS is_hcity
		FROM summary)
		AS t1
	GROUP BY DATE_PART ('year', est_delivery_date)
	ORDER BY DATE_PART ('year', est_delivery_date) ASC
)

SELECT t1.date_year,
		t1.total_state,
		t2.highest_state AS highest_customer_state,
		t2.total_transaction_in_state,
		t4.state_percentage_from_total,
		t1.total_city,
		t3.highest_city AS highest_customer_city,
		t3.total_transaction_in_city,
		t4.city_percentage_from_total
FROM total_city_state AS t1
LEFT JOIN highest_state AS t2
	ON t1.date_year = t2.date_year
LEFT JOIN highest_city AS t3
	ON t1.date_year = t3.date_year
LEFT JOIN percentage_state_city AS t4
	ON t1.date_year = t4.date_year;


------------------------------------------------------------------
----------   CUSTOMER ORIGIN (CITY) PERCENTAGE   -----------------
------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS origin_customer_city AS
WITH t1 AS
(
	SELECT customer_city, COUNT(*) AS total_customer
	FROM (SELECT customer_unique_id, 
				customer_city,
				customer_state
		FROM summary
		GROUP BY customer_unique_id, 
				customer_city,
				customer_state) AS t1
	GROUP BY customer_city
),
t2 AS
(
	SELECT customer_city,
			ROUND((total_customer/(SELECT SUM(total_customer) FROM t1))*100,3) AS percentage
	FROM t1
	ORDER BY ROUND((total_customer/(SELECT SUM(total_customer) FROM t1))*100,3) DESC
) 

SELECT top_5,
		SUM(percentage) AS total_percentage,
		COUNT(*) AS total_cities
FROM (SELECT *, 
			RANK() OVER ( ORDER BY percentage DESC) AS ranking,
			CASE 
			WHEN RANK() OVER ( ORDER BY percentage DESC) <= 5 THEN customer_city
			ELSE 'others' END top_5
	FROM t2) AS t3
GROUP BY top_5;


------------------------------------------------------------------
----------   CUSTOMER ORIGIN (STATE) PERCENTAGE   ----------------
------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS origin_customer_state AS
WITH t1 AS
(
	SELECT customer_state, COUNT(*) AS total_customer
	FROM (SELECT customer_unique_id, 
				customer_city,
				customer_state
		FROM summary
		GROUP BY customer_unique_id, 
				customer_city,
				customer_state) AS t1
	GROUP BY customer_state
),
t2 AS
(
	SELECT customer_state,
			ROUND((total_customer/(SELECT SUM(total_customer) FROM t1))*100,3) AS percentage
	FROM t1
	ORDER BY ROUND((total_customer/(SELECT SUM(total_customer) FROM t1))*100,3) DESC
)

SELECT top_5,
		SUM(percentage) AS total_percentage,
		COUNT(*) AS total_states
FROM (SELECT *, 
			RANK() OVER ( ORDER BY percentage DESC) AS ranking,
			CASE 
			WHEN RANK() OVER ( ORDER BY percentage DESC) <= 5 THEN customer_state
			ELSE 'others' END top_5
	FROM t2) AS t3
GROUP BY top_5;


-------------------------------------------------------------------------------
---------------                                              ------------------
---------------   ANNUAL PRODUCT CATEGORY QUALITY ANALYSIS   ------------------
---------------                                              ------------------
-------------------------------------------------------------------------------

-----------------------   ----------------------------------------
------------    Annual Product Summary   -------------------------
-----------------------   ----------------------------------------

WITH yearly_revenue AS --- 01 annual revenue ---
(
	SELECT DATE_PART('year', est_delivery_date) AS date_year, 
			SUM(total_cost) AS revenue
	FROM summary
	WHERE order_status = 'delivered'
	GROUP BY DATE_PART('year', est_delivery_date)
),
yearly_cancel AS --- 02 Annual Total Canceled Transaction ---
(
	SELECT DATE_PART('year', approved_date) AS date_year, 
			COUNT(*) AS total_cancel
	FROM summary
	WHERE order_status = 'canceled'
	GROUP BY DATE_PART('year', approved_date)
	ORDER BY DATE_PART('year', approved_date) ASC
),
top_product AS --- 03 Annual Top Product ----
(
	SELECT date_year, 
			product_category AS top_product
	FROM (SELECT DATE_PART('year', est_delivery_date) AS date_year,
				product_category,
				SUM(total_cost) AS revenue,
				RANK() OVER (PARTITION BY DATE_PART('year', est_delivery_date) ORDER BY SUM(total_cost) DESC) AS yearly_rank
		FROM summary
		WHERE order_status = 'delivered'
		GROUP BY DATE_PART('year', est_delivery_date),
					product_category
		ORDER BY DATE_PART('year', est_delivery_date) ASC,
					SUM(total_cost) DESC) AS t1
	WHERE yearly_rank = 1
),
top_cancel AS --- 04 Annual Top Cancel ---
(
	SELECT date_year, 
			product_category AS most_canceled
	FROM (SELECT DATE_PART('year', approved_date) AS date_year,
				product_category,
				COUNT(*) AS total_cancel,
				RANK() OVER (PARTITION BY DATE_PART('year', approved_date) ORDER BY COUNT(*) DESC) AS yearly_rank
		FROM summary
		WHERE order_status = 'canceled'
		GROUP BY DATE_PART('year', approved_date),
					product_category
		ORDER BY DATE_PART('year', approved_date) ASC,
					COUNT(*) DESC) AS t1
	WHERE yearly_rank = 1
)

SELECT t1.date_year,
		t1.revenue,
		t2.total_cancel,
		t3.top_product,
		t4.most_canceled
FROM yearly_revenue AS t1
LEFT JOIN yearly_cancel AS t2
	ON t1.date_year = t2.date_year
LEFT JOIN top_product AS t3
	ON t1.date_year = t3.date_year
LEFT JOIN top_cancel AS t4
	ON t1.date_year = t4.date_year;


-----------------------   ----------------------------------------
------------   TOP re-order product   ----------------------------
-----------------------   ----------------------------------------

CREATE TABLE IF NOT EXISTS top_reorder_product AS
WITH reorder_product_rank AS
(
	SELECT date_year,
			product_category,
			product_id AS top_product_id,
			SUM((total_transaction - 1)) AS total_reorder,
			RANK() OVER (PARTITION BY date_year ORDER BY SUM((total_transaction - 1)) DESC) AS yearly_rank
	FROM (SELECT DATE_PART('year', est_delivery_date) AS date_year,
					customer_unique_id,
					product_category,
					product_id,
					COUNT(*) AS total_transaction
			FROM summary
			WHERE order_status = 'delivered'
			GROUP BY DATE_PART('year', est_delivery_date),
						customer_unique_id,
						product_category,
						product_id
		  ) AS t1
	WHERE total_transaction > 1
	GROUP BY date_year,
				product_category,
				product_id
	ORDER BY date_year ASC,
			SUM((total_transaction - 1)) DESC
)

SELECT *
FROM reorder_product_rank
WHERE yearly_rank <=3 

-----------------------   ----------------------------------------
-----------   TOP re-order category   ----------------------------
-----------------------   ----------------------------------------

CREATE TABLE IF NOT EXISTS top_reorder_category AS
WITH reorder_category_rank AS
(
	SELECT date_year,
			product_category AS top_reorder_category,
			SUM((total_transaction - 1)) AS total_reorder,
			RANK() OVER (PARTITION BY date_year ORDER BY SUM((total_transaction - 1)) DESC) AS yearly_rank
	FROM (SELECT DATE_PART('year', est_delivery_date) AS date_year,
					customer_unique_id,
					product_category,
					COUNT(*) AS total_transaction
			FROM summary
			WHERE order_status = 'delivered'
			GROUP BY DATE_PART('year', est_delivery_date),
						customer_unique_id,
						product_category
		  ) AS t1
	WHERE total_transaction > 1
	GROUP BY date_year,
				product_category
	ORDER BY date_year ASC,
			SUM((total_transaction - 1)) DESC
)

SELECT *
FROM reorder_category_rank
WHERE yearly_rank <=3;


-----------------------   ----------------------------------------
----------   TOP 3 Category by Revenue   -------------------------
-----------------------   ----------------------------------------

CREATE TABLE IF NOT EXISTS top_category_revenue AS
SELECT date_year, 
		product_category AS top_category_revenue,
		revenue,
		yearly_rank
FROM (SELECT DATE_PART('year', est_delivery_date) AS date_year,
			product_category,
			SUM(total_cost) AS revenue,
			RANK() OVER (PARTITION BY DATE_PART('year', est_delivery_date) ORDER BY SUM(total_cost) DESC) AS yearly_rank
	FROM summary
	WHERE order_status = 'delivered'
	GROUP BY DATE_PART('year', est_delivery_date),
				product_category
	ORDER BY DATE_PART('year', est_delivery_date) ASC,
				SUM(total_cost) DESC) AS t1
WHERE yearly_rank <= 3;


-----------------------   ----------------------------------------
--------   TOP 3 YEARLY TRANSACTION PRODUCT CATEGORY   -----------
-----------------------   ----------------------------------------

CREATE TABLE top_transaction_category AS
SELECT *
FROM (SELECT DATE_PART('year', purchase_date) AS purchase_year,
			product_category,
			COUNT(*),
			RANK() OVER (PARTITION BY DATE_PART('year', purchase_date) ORDER BY COUNT(*) DESC) AS yearly_rank
	FROM summary
	GROUP BY DATE_PART('year', purchase_date),
				product_category
	ORDER BY DATE_PART('year', purchase_date) ASC,
				COUNT(*) DESC) AS t1
WHERE yearly_rank <= 3;


-----------------------   ----------------------------------------
--------------   TOP 3 Summary   ---------------------------------
-----------------------   ----------------------------------------

CREATE TABLE IF NOT EXISTS top_summary AS
SELECT t1.rn,
		t1.date_year,
		t1.top_reorder_category,
		t1.total_reorder AS total_reorder_category,
		t1.yearly_rank AS reorder_rank,
		t2.product_category AS top_reorder_product_category,
		t2.top_product_id,
		t2.total_reorder,
		t2.yearly_rank AS reorder_product_rank,
		t3.top_category_revenue,
		t3.revenue,
		t3.yearly_rank AS revenue_rank,
		t4.product_category AS top_transaction_category,
		t4.count AS total_transaction,
		t4.yearly_rank AS transaction_rank
FROM (SELECT ROW_NUMBER() OVER() AS rn, *
	  FROM top_reorder_category) AS t1
JOIN (SELECT ROW_NUMBER() OVER() AS rn, *
	  FROM top_reorder_product) AS t2
	  ON t1.rn = t2.rn
JOIN (SELECT ROW_NUMBER() OVER() AS rn, *
	  FROM top_category_revenue) AS t3
	  ON t1.rn = t3.rn
JOIN (SELECT ROW_NUMBER() OVER() AS rn, *
	  FROM top_transaction_category) AS t4
	  ON t1.rn = t4.rn;


------------------------------------------------------------------
---------------------   SELLER ORIGIN  ---------------------------
------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS origin_seller AS
WITH total_city_state AS
(
	SELECT DATE_PART('year', est_delivery_date) AS date_year,
			COUNT(DISTINCT(seller_city)) AS total_city,
			COUNT(DISTINCT(seller_state)) AS total_state
	FROM summary
	GROUP BY DATE_PART('year', est_delivery_date)
),
highest_state AS
(
	SELECT *
	FROM (SELECT DATE_PART('year', purchase_date) AS date_year,
				seller_state AS highest_state,
				COUNT(*) AS total_transaction_in_state,
				RANK() OVER (PARTITION BY DATE_PART('year', purchase_date) ORDER BY COUNT(*) DESC) AS yearly_rank
		FROM summary
		GROUP BY DATE_PART('year', purchase_date),
					seller_state
		ORDER BY DATE_PART('year', purchase_date) ASC,
					COUNT(*) DESC) AS t1
	WHERE yearly_rank <= 1
),
highest_city AS
(
	SELECT *
	FROM (SELECT DATE_PART('year', purchase_date) AS date_year,
				seller_city AS highest_city,
				COUNT(*) AS total_transaction_in_city,
				RANK() OVER (PARTITION BY DATE_PART('year', purchase_date) ORDER BY COUNT(*) DESC) AS yearly_rank
		FROM (	SELECT order_id, 
			  			purchase_date, 
			  			seller_city 
				FROM summary
			  	GROUP BY order_id, 
			  			purchase_date, 
			  			seller_city
		  	) AS t1
		GROUP BY DATE_PART('year', purchase_date),
					seller_city
		ORDER BY DATE_PART('year', purchase_date) ASC,
					COUNT(*) DESC) AS t1
	WHERE yearly_rank <= 1
),
percentage_state_city AS
(
	SELECT DATE_PART ('year', est_delivery_date) AS date_year,
			COUNT(*) AS total_transaction,
			ROUND(AVG(is_hstate)*100,2) AS state_percentage_from_total,
			ROUND(AVG(is_hcity)*100,2) AS city_percentage_from_total
	FROM (SELECT est_delivery_date,
				CASE
				WHEN seller_state = 'SP' THEN 1
				ELSE 0
				END AS is_hstate,
				CASE
				WHEN seller_city = 'sao paulo' THEN 1
				ELSE 0
				END AS is_hcity
		FROM summary)
		AS t1
	GROUP BY DATE_PART ('year', est_delivery_date)
	ORDER BY DATE_PART ('year', est_delivery_date) ASC
)

SELECT t1.date_year,
		t1.total_state,
		t2.highest_state,
		t2.total_transaction_in_state,
		t4.state_percentage_from_total,
		t1.total_city,
		t3.highest_city,
		t3.total_transaction_in_city,
		t4.city_percentage_from_total
FROM total_city_state AS t1
LEFT JOIN highest_state AS t2
	ON t1.date_year = t2.date_year
LEFT JOIN highest_city AS t3
	ON t1.date_year = t3.date_year
LEFT JOIN percentage_state_city AS t4
	ON t1.date_year = t4.date_year;


------------------------------------------------------------------
-------------   SELLER ORIGIN (CITY) PERCENTAGE  -----------------
------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS origin_seller_city AS
WITH t1 AS
(
	SELECT seller_city, COUNT(*) AS total_seller
	FROM (SELECT seller_id, 
				seller_city,
				seller_state
		FROM summary
		GROUP BY seller_id, 
				seller_city,
				seller_state) AS t1
	GROUP BY seller_city
),
t2 AS 
(
	SELECT seller_city,
			ROUND((total_seller/(SELECT SUM(total_seller) FROM t1))*100,2) AS percentage
	FROM t1
	ORDER BY ROUND((total_seller/(SELECT SUM(total_seller) FROM t1))*100,2) DESC
)

SELECT top_5,
		SUM(percentage) AS total_percentage,
		COUNT(*) AS total_cities
FROM (SELECT *, 
			RANK() OVER ( ORDER BY percentage DESC) AS ranking,
			CASE 
			WHEN RANK() OVER ( ORDER BY percentage DESC) <= 5 THEN seller_city
			ELSE 'others' END top_5
	FROM t2) AS t3
GROUP BY top_5;

------------------------------------------------------------------
------------   SELLER ORIGIN (STATE) PERCENTAGE   ----------------
------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS origin_seller_state AS
WITH t1 AS
(
	SELECT seller_state, COUNT(*) AS total_seller
	FROM (SELECT seller_id, 
				seller_city,
				seller_state
		FROM summary
		GROUP BY seller_id, 
				seller_city,
				seller_state) AS t1
	GROUP BY seller_state
),
t2 AS
(
	SELECT seller_state,
			ROUND((total_seller/(SELECT SUM(total_seller) FROM t1))*100,2) AS percentage
	FROM t1
	ORDER BY ROUND((total_seller/(SELECT SUM(total_seller) FROM t1))*100,2) DESC
)

SELECT top_5,
		SUM(percentage) AS total_percentage,
		COUNT(*) AS total_state
FROM (SELECT *, 
			RANK() OVER ( ORDER BY percentage DESC) AS ranking,
			CASE 
			WHEN RANK() OVER ( ORDER BY percentage DESC) <= 5 THEN seller_state
			ELSE 'others' END top_5
	FROM t2) AS t3
GROUP BY top_5;


-------------------------------------------------------------------------------
------------------                                        ---------------------
------------------   ANNUAL PAYMENT TYPE USAGE ANALYSIS   ---------------------
------------------                                        ---------------------
-------------------------------------------------------------------------------

-----------------------   ----------------------------------------
------------    Annual Payment Summary   -------------------------
-----------------------   ----------------------------------------

CREATE TABLE IF NOT EXISTS payment_summary AS
WITH payment_date AS
(
	SELECT t1.*,
			t2.order_estimated_delivery_date AS est_delivery_date,
			DATE_PART('year', t2.order_estimated_delivery_date) AS est_delivery_year 
	FROM order_payments_dataset AS t1
	LEFT JOIN orders_dataset AS t2
		ON t1.order_id = t2.order_id
	WHERE t1.payment_type != 'not_defined'
),
payment_recap AS
(
	SELECT payment_type, 
			COUNT(*) AS total_transaction
	FROM payment_date
	GROUP BY payment_type
	ORDER BY COUNT(*)
),
recap_16 AS
(
	SELECT payment_type, 
			COUNT(*) AS "2016"
	FROM payment_date
	WHERE est_delivery_year = 2016
	GROUP BY payment_type
	ORDER BY COUNT(*)
),
recap_17 AS
(
	SELECT payment_type, 
			COUNT(*) AS "2017"
	FROM payment_date
	WHERE est_delivery_year = 2017
	GROUP BY payment_type
	ORDER BY COUNT(*)
),
recap_18 AS
(
	SELECT payment_type, 
			COUNT(*) AS "2018"
	FROM payment_date
	WHERE est_delivery_year = 2018
	GROUP BY payment_type
	ORDER BY COUNT(*)
)

SELECT t1.payment_type,
		t1.total_transaction,
		t2."2016",
		t3."2017",
		t4."2018"
FROM payment_recap AS t1
LEFT JOIN recap_16 AS t2
	ON t1.payment_type = t2.payment_type
LEFT JOIN recap_17 AS t3
	ON t1.payment_type = t3.payment_type
LEFT JOIN recap_18 AS t4
	ON t1.payment_type = t4.payment_type
ORDER BY t1.total_transaction DESC;


-----------------------   ----------------------------------------
------------   All Time Payment Percentage   ---------------------
-----------------------   ----------------------------------------

CREATE TABLE IF NOT EXISTS payment_percentage AS
SELECT payment_type,
		total_transaction,
		ROUND((total_transaction/(SELECT SUM(total_transaction) FROM payment_summary)*100),1) AS percentage
FROM payment_summary;

