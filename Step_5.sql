-- DDL витрины

DROP TABLE IF EXISTS dwh.customer_report_datamart;

CREATE TABLE IF NOT EXISTS dwh.customer_report_datamart (
id BIGINT GENERATED ALWAYS AS identity NOT NULL,
customer_id BIGINT NOT NULL,
customer_name VARCHAR NOT NULL,
customer_address VARCHAR NOT NULL,
customer_birthday DATE NOT NULL,
customer_email VARCHAR NOT NULL,
customer_money NUMERIC(15,2) NOT NULL, -- сумма, которую потратил заказчик
platform_money NUMERIC(15,2) NOT NULL, -- сумма, которую заработала платформа от покупок заказчика за месяц (10% от суммы, которую потратил заказчик
count_order BIGINT NOT NULL, --количество заказов у заказчика за месяц;
avg_price_order NUMERIC(15,2) NOT NULL, --средняя стоимость одного заказа у заказчика за месяц;
median_time_order_completed NUMERIC(10,1), --медианное время в днях от момента создания заказа до его завершения за месяц;
top_product_category VARCHAR NOT NULL, --самая популярная категория товаров у этого заказчика за месяц;
top_craftsman_id BIGINT NOT NULL, --идентификатор самого популярного мастера ручной работы у заказчика. Если заказчик сделал одинаковое количество заказов у нескольких мастеров, возьмите любого;
count_order_created BIGINT NOT NULL, --количество созданных заказов за месяц;
count_order_in_progress BIGINT NOT NULL, --количество заказов в процессе изготовки за месяц;
count_order_delivery BIGINT NOT NULL, --количество заказов в доставке за месяц;
count_order_done BIGINT NOT NULL, --количество завершённых заказов за месяц;
count_order_not_done BIGINT NOT NULL, --количество незавершённых заказов за месяц;
report_period VARCHAR NOT NULL,--отчётный период, год и месяц.
CONSTRAINT customer_report_datamart_pk PRIMARY KEY (id));
