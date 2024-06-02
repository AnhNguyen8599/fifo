# fifo
FIFO rule for inventory in financial management
create database fifo

CREATE TABLE fifo.movement (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, 
  document_id INT, 
  warehouse VARCHAR(100), 
  sku VARCHAR(100), 
  quantity INT, 
  balance INT, 
  created_at DATETIME
); 

CREATE TABLE fifo.document (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, 
  customer_id INT, 
  type TEXT
); 

CREATE TABLE fifo.customer (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, 
  contact VARCHAR(100)
); 

INSERT INTO fifo.movement (document_id, sku, warehouse, quantity, balance, created_at) 
VALUES 
  (1, 'iPhone', 'HK', 10, 10, '2021-1-1'), 
  (1, 'iPod', 'HK', 3, 3, '2021-1-1'), 
  (2, 'iPod', 'HK', -1, 2, '2021-1-2'), 
  (3, 'iPod', 'HK', -2, 0, '2021-1-3'), 
  (4, 'iPod', 'HK', 5, 5, '2021-2-1'), 
  (5, 'iMac', 'US', 5, 5, '2021-2-1'), 
  (5, 'iPhone', 'US', 2, 2, '2021-2-1'), 
  (6, 'iMac', 'HK', 5, 5, '2021-2-2'), 
  (7, 'iPod', 'HK', -4, 1, '2021-2-8'), 
  (8, 'iMac', 'HK', -1, 4, '2021-2-9'), 
  (9, 'iPhone', 'US', -1, 1, '2021-2-17'), 
  (10, 'iMac', 'HK', 1, 5, '2021-3-2'), 
  (11, 'iMac', 'HK', -1, 4, '2021-3-8'), 
  (11, 'iPod', 'HK', -1, 0, '2021-3-8'), 
  (12, 'iMac', 'US', 5, 10, '2021-3-10'); 

INSERT INTO fifo.document (id, customer_id, type) 
VALUES 
  (1, NULL, "purchase"), 
  (2, 1, 'sales_order'), 
  (3, 2, 'sales_order'), 
  (4, NULL, 'purchase'), 
  (5, NULL, 'purchase'), 
  (6, NULL, 'purchase'), 
  (7, 3, 'sales_order'), 
  (8, 1, 'sales_order'), 
  (9, 4, 'sales_order'), 
  (10, NULL, 'purchase'), 
  (11, NULL, 'sales_order'), 
  (12, NULL, 'purchase'), 
  (13, NULL, 'purchase'); 

INSERT INTO fifo.customer (id, contact) 
VALUES 
  (1, 'boris0407@gmail.com'), 
  (2, 'candywong@gmail.com'), 
  (3, 'flora2002@gmail.com'), 
  (4, 'glory@gmail.com'), 
  (5, 'himsonfong@gmail.com')


select contact, RANK() OVER (
        PARTITION BY contact
        ORDER BY balance DESC
    ) AS sold
from (
select c.contact as contact,
coalesce (c.contact,'guest'),
sum(m.quantity)*-1 as sold,
rank () over (order by sum(m.quantity)*-1 desc) as sold
from fifo.customer c
join fifo.document d  
on c.id= d.customer_id
join fifo.movement m 
on d.id = m.document_id ) as sub1

create table fifo.info as
(
select m.*, d.type,d.customer_id, c.contact
from fifo.movement m 
left join fifo.document d  
on m.document_id = d.id
left join fifo.customer c  
on c.id = d.customer_id)



/*-question 1: Rank customers by quantity they purchased.
Include the customer's email address (shown as "guest" if not provided) and quantity they purchased in the report*/

select contact,
coalesce (contact,'guest')as email,
sum(quantity)*-1 as sold
from fifo.info
group by contact
order by sum(quantity)*-1 desc

/*-question2: *Write a SQL query to return HK warehouse's stock of any given time.
Define a variable for the time so user can change it easily. For example: SET @date = '2021-4-1 00:00:00'.
*/


SET @date = '2021-04-01 00:00:00';
select * from (
select coalesce(warehouse,'none') as warehouse,coalesce (sku,'none') as sku,coalesce(sum(quantity),'out of stock') as balance from fifo.movement 
where warehouse = 'HK' and sku = 'iPod'
and created_at <= @date
order by created_at DESC
) as iPod 
UNION
select * from (
select coalesce(warehouse,'none') as warehouse,coalesce (sku,'none') as sku,coalesce(sum(quantity),'out of stock') as balance from fifo.movement 
where warehouse = 'HK' and sku = 'iPhone'
and created_at <= @date
order by created_at DESC
LIMIT 1) as iPhone
UNION
select * from (
select coalesce(warehouse,'none') as warehouse,coalesce (sku,'none') as sku,coalesce(sum(quantity),'out of stock') as balance from fifo.movement 
where warehouse = 'HK' and sku = 'iMac'
and created_at <= @date
order by created_at DESC 
LIMIT 1) as iMac 

/*Question 3: Age of Inventory (Advanced level)
Show the age of the available stocks of a given time, and group the quantity by age "0-30 days", "31-60 days", "61 - 90 days" and "90 days+".
•	Age of an stock means the number of day after it enters the warehouse.
•	Stocks in a warehouse comes and goes. When deducting stock please follow the "First In First Out" rule, meaning oldest stock will be deducted first.
*/
set @date ='2021-04-01 00:00:00' ;
with transaction as (
select warehouse, sku, quantity, sum(quantity) over (partition by sku order by created_at) as balance, datediff(@date,created_at) as agedays, created_at
from fifo.info i 
where warehouse ='HK'),
no_out as (
select *
from `transaction` where balance >0),
age_category as (
select *,
		case when agedays >0 and agedays <=30 then 'Age 0-30 days'
			when agedays >30 and agedays <= 60 then 'Age 31-60 days'
			when agedays >60 and agedays <= 9 then 'Age 61-90ays'
			else 'Age 90+ days' end as age_category
from no_out)
select sku, sum(quantity) as stock,
ifnull (sum(case when age_category = 'Age 0-30 days' then quantity end),0) as 'Age 0-30 days',
ifnull (sum(case when age_category = 'Age 31-60 days' then quantity end),0) as 'Age 31-60 days',
ifnull (sum(case when age_category = 'Age 61-90ays' then quantity end),0) as 'Age 61-90ays',
ifnull (sum(case when age_category = 'Age 90+ days' then quantity end),0) as 'Age 90+ days'
from age_category
group by sku
