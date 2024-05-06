-- DDL таблицы инкрементальных загрузок

DROP TABLE IF EXISTS dwh.load_dates_customer_report_datamart;

CREATE TABLE IF NOT EXISTS dwh.load_dates_customer_report_datamart (
	id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	load_dttm DATE NOT NULL,
	CONSTRAINT load_dates_customer PRIMARY KEY (id));



-- -- определяем, какие данные были изменены в витрине или добавлены в DWH. Формируем дельту изменений

WITH dwh_delta AS (
	       SELECT dcs.customer_id,
	              dcs.customer_name,
                      dcs.customer_address,
                      dcs.customer_birthday,
                      dcs.customer_email,
                      dc.craftsman_id,
                      fo.order_id,
                      dp.product_id,
                      dp.product_price,
                      dp.product_type,
                      (fo.order_completion_date - fo.order_created_date) AS diff_order_date, 
                      fo.order_status,
                      to_char(fo.order_created_date, 'yyyy-mm') AS report_period,
                      crd.customer_id AS exist_customer_id,
                      dc.load_dttm AS craftsman_load_dttm,
                      dcs.load_dttm AS customers_load_dttm,
                      dp.load_dttm AS products_load_dttm
               FROM dwh.f_order fo
               INNER JOIN dwh.d_craftsman dc on fo.craftsman_id=dc.craftsman_id
               INNER JOIN dwh.d_customer dcs on fo.customer_id=dcs.customer_id
               INNER JOIN dwh.d_product dp on fo.product_id=dp.product_id
               LEFT JOIN dwh.customer_report_datamart crd on dcs.customer_id=crd.customer_id
               WHERE fo.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01')
                                     FROM dwh.load_dates_customer_report_datamart) or 
                     dc.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01')
                                     FROM dwh.load_dates_customer_report_datamart) or 
                     dcs.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01')
                                      FROM dwh.load_dates_customer_report_datamart) or 
                     dp.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01')
                                     FROM dwh.load_dates_customer_report_datamart))),



-- делаем выборку мастеров ручной работы, по которым были изменения в DWH

dwh.dwh_update_delta AS (
                     SELECT dd.exist_customer_id AS customer_id
                     FROM dwh.dwh_delta dd 
                     WHERE dd.exist_customer_id IS NOT null),



-- делаем расчёт витрины по новым данным

dwh.dwh_delta_insert_result AS (
                            SELECT  T5.customer_id,
		                    T5.customer_name,
	                 	    T5.customer_address,
		    	            T5.customer_birthday,
		    		    T5.customer_email,
		                    T5.customer_money,
		                    T5.platform_money,
		                    T5.count_order,
		                    T5.avg_price_order,
		                    T5.median_time_order_completed,
		                    T5.top_product_category,
		                    T5.top_craftsman_id,
		                    T5.count_order_created,
                                    T5.count_order_in_progress, 
                                    T5.count_order_delivery, 
                                    T5.count_order_done, 
                                    T5.count_order_not_done,
                                    T5.report_period   
                            FROM (SELECT *,
                                         RANK() OVER(PARTITION BY T2.customer_id ORDER BY count_product DESC) AS rank_count_product,
                                         RANK() OVER(PARTITION BY T2.customer_id ORDER BY count_craftsman DESC) AS rank_count_craftsman
                                  FROM (SELECT -- основные параметры
          			               T1.customer_id,
			                       T1.customer_name,
		                               T1.customer_address,
			                       T1.customer_birthday,
			                       T1.customer_email,
			                       SUM(T1.product_price) as customer_money,
			                       SUM(T1.product_price)*0.1 as platform_money,
			                       COUNT(T1.order_id) as count_order,
			                       AVG(T1.product_price) as avg_price_order,
			                       percentile_cont(0.5) WITHIN GROUP(ORDER BY T1.diff_order_date) as median_time_order_completed,
			                       SUM(CASE WHEN T1.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created,
	                                       SUM(CASE WHEN T1.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress, 
	                                       SUM(CASE WHEN T1.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery, 
	                                       SUM(CASE WHEN T1.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done, 
	                                       SUM(CASE WHEN T1.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done,
			                       T1.report_period
                                        FROM dwh.dwh_delta AS T1
                                        WHERE T1.exist_customer_id IS NULL
                                        GROUP BY T1.customer_id, T1.customer_name, T1.customer_address, T1.customer_birthday, T1.customer_email, T1.report_period) AS T2 
                                        INNER JOIN (SELECT -- самая популярная категория товаров у заказчика
                                                           dd.customer_id AS customer_id_for_product_type, 
                                                           dd.product_type as top_product_category, 
                                                           COUNT(dd.product_id) AS count_product
                                                    FROM dwh.dwh_delta AS dd
                                                    GROUP BY dd.customer_id, dd.product_type
                                                    ORDER BY count_product DESC) AS T3 ON T2.customer_id = T3.customer_id_for_product_type
		                        INNER JOIN (SELECT -- самый популярный мастер у заказчика
                                                           dd.customer_id as customer_id_for_top_craftsman,
                                                           dd.craftsman_id as top_craftsman_id,
                                                           COUNT(craftsman_id) as count_craftsman
                                                    FROM dwh.dwh_delta as dd
                                                    GROUP BY dd.customer_id, dd.craftsman_id
                                                    ORDER BY count_craftsman desc) as T4 on T2.customer_id = T4.customer_id_for_top_craftsman) AS T5
                            WHERE T5.rank_count_product = 1
                                  and T5.rank_count_craftsman = 1
                            ORDER BY report_period),



-- делаем перерасчёт для существующих записей витринs, так как данные обновились за отчётные периоды

dwh.dwh_delta_update_result AS (
                            SELECT  T5.customer_id,
                                    T5.customer_name,
		                    T5.customer_address,
		                    T5.customer_birthday,
		                    T5.customer_email,
		                    T5.customer_money,
		                    T5.platform_money,
		                    T5.count_order,
		                    T5.avg_price_order,
		                    T5.median_time_order_completed,
		                    T5.top_product_category,
		                    T5.top_craftsman_id,
		                    T5.count_order_created,
                                    T5.count_order_in_progress, 
                                    T5.count_order_delivery, 
                                    T5.count_order_done, 
                                    T5.count_order_not_done,
                                    T5.report_period   
                            FROM (SELECT *,
                                         RANK() OVER(PARTITION BY T2.customer_id ORDER BY count_product DESC) AS rank_count_product,
                                         RANK() OVER(PARTITION BY T2.customer_id ORDER BY count_craftsman DESC) AS rank_count_craftsman
                                  FROM (SELECT -- основные параметры
          			               T1.customer_id,
			                       T1.customer_name,
			                       T1.customer_address,
			                       T1.customer_birthday,
			                       T1.customer_email,
			                       SUM(T1.product_price) as customer_money,
			                       SUM(T1.product_price)*0.1 as platform_money,
			                       COUNT(T1.order_id) as count_order,
			                       AVG(T1.product_price) as avg_price_order,
			                       percentile_cont(0.5) WITHIN GROUP(ORDER BY T1.diff_order_date) as median_time_order_completed,
			                       SUM(CASE WHEN T1.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created,
	                                       SUM(CASE WHEN T1.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress, 
	                                       SUM(CASE WHEN T1.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery, 
	                                       SUM(CASE WHEN T1.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done, 
	                                       SUM(CASE WHEN T1.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done,
			                       T1.report_period
                                        FROM (SELECT dcs.customer_id,
                                                     dcs.customer_name,
			                             dcs.customer_address,
			                             dcs.customer_birthday,
			                             dcs.customer_email,
			                             dp.product_price,
			                             dp.product_type,
			                             fo.order_id,
			                             fo.order_status,
			                             (fo.order_completion_date - fo.order_created_date) AS diff_order_date,
			                             TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period,
			                             dc.craftsman_id
                                              FROM dwh.f_order fo
                                              INNER JOIN dwh.d_craftsman dc on fo.craftsman_id=dc.craftsman_id
                                              INNER JOIN dwh.d_customer dcs on fo.customer_id=dcs.customer_id
                                              INNER JOIN dwh.d_product dp on fo.product_id=dp.product_id) AS T1
                                              GROUP BY T1.customer_id, T1.customer_name, T1.customer_address, T1.customer_birthday, T1.customer_email, T1.report_period) AS T2 
          
                                        INNER JOIN (SELECT -- самая популярная категория товаров у заказчика
                                                           dd.customer_id AS customer_id_for_product_type, 
                                                           dd.product_type as top_product_category, 
                                                           COUNT(dd.product_id) AS count_product
                                                    FROM dwh.dwh_delta AS dd
                                                    GROUP BY dd.customer_id, dd.product_type
                                                    ORDER BY count_product DESC) AS T3 ON T2.customer_id = T3.customer_id_for_product_type
          
	                	        INNER JOIN (SELECT -- самый популярный мастер у заказчика
                                                          dd.customer_id as customer_id_for_top_craftsman,
                                                          dd.craftsman_id as top_craftsman_id,
                                                          COUNT(craftsman_id) as count_craftsman
                                                    FROM dwh.dwh_delta as dd
                                                    GROUP BY dd.customer_id, dd.craftsman_id
                                                    ORDER BY count_craftsman desc) as T4 on T2.customer_id = T4.customer_id_for_top_craftsman) AS T5
                             WHERE T5.rank_count_product = 1
                                   and T5.rank_count_craftsman = 1
                             ORDER BY report_period),



-- выполняем insert новых расчитанных данных для витрины

insert_delta AS (
             INSERT INTO dwh.customer_report_datamart (customer_id,
		        			       customer_name,
						       customer_address,
						       customer_birthday,
						       customer_email,
						       customer_money,
						       platform_money,
						       count_order,
						       avg_price_order,
						       median_time_order_completed,
						       top_product_category,
						       top_craftsman_id,
						       count_order_created,
						       count_order_in_progress, 
						       count_order_delivery, 
						       count_order_done, 
						       count_order_not_done,
						       report_period)
             SELECT customer_id,
	            customer_name,
	            customer_address,
 	            customer_birthday,
 	            customer_email,
	            customer_money,
	            platform_money,
	            count_order,
	            avg_price_order,
	            median_time_order_completed,
	            top_product_category,
	            top_craftsman_id,
	            count_order_created,
                    count_order_in_progress, 
                    count_order_delivery, 
                    count_order_done, 
                    count_order_not_done,
                    report_period
             FROM dwh.dwh_delta_insert_result),



-- выполняем обновление показателей в отчёте по уже существующим мастерам

update_delta AS (
             UPDATE dwh.customer_report_datamart
             SET customer_name = updates.customer_name, 
                 customer_address = updates.customer_address, 
                 customer_birthday = updates.customer_birthday, 
                 customer_email = updates.customer_email, 
                 customer_money = updates.customer_money, 
                 platform_money = updates.platform_money, 
                 count_order = updates.count_order, 
                 avg_price_order = updates.avg_price_order,  
                 median_time_order_completed = updates.median_time_order_completed, 
                 top_product_category = updates.top_product_category, 
                 top_craftsman_id = updates.top_craftsman_id,
                 count_order_created = updates.count_order_created, 
                 count_order_in_progress = updates.count_order_in_progress, 
                 count_order_delivery = updates.count_order_delivery, 
                 count_order_done = updates.count_order_done,
                 count_order_not_done = updates.count_order_not_done, 
                 report_period = updates.report_period
	     FROM (SELECT customer_id,
		          customer_name,
		          customer_address,
		 	  customer_birthday,
		 	  customer_email,
			  customer_money,
			  platform_money,
			  count_order,
			  avg_price_order,
			  median_time_order_completed,
			  top_product_category,
			  top_craftsman_id,
			  count_order_created,
		          count_order_in_progress, 
		          count_order_delivery, 
		          count_order_done, 
		          count_order_not_done,
		          report_period
                   FROM dwh.dwh_delta_update_result) AS updates
              WHERE dwh.customer_report_datamart.customer_id = updates.customer_id),



-- делаем запись в таблицу загрузок

insert_load_data AS (INSERT INTO dwh.load_dates_craftsman_report_datamart (load_dttm)
                     SELECT GREATEST(COALESCE(MAX(craftsman_load_dttm), NOW()), 
			             COALESCE(MAX(customers_load_dttm), NOW()), 
				     COALESCE(MAX(products_load_dttm), NOW())) 
                     FROM dwh.dwh_delta)



SELECT 'increment datamart';

