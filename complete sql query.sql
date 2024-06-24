create database IF NOT EXISTS product_orders;

create table data_order(
order_id int,
order_date VARCHAR(20),
ship_mode VARCHAR(20),
segment varchar(20),
country varchar(20),
city varchar(20),
state varchar(20),
postal_code VARCHAR(20),
region varchar(20),
category varchar(20),
sub_category varchar(20),
product_id varchar(20),
cost_price int,
list_price int,
quantity int,
discount_per int,
PRIMARY KEY (order_id) )
;

select * from data_order;

-- Feature Engineering

-- 1. Change the data type of the column order_date

alter table data_order
modify order_date date;
desc data_order;
update data_order
set order_date = str_to_date(order_date, "%d-%m-%Y");
select * from data_order;

select year(order_date) from data_order;

-- 2. Null value treatment of column ship_mode
select * from data_order where ship_mode= ('Not Available');
update data_order set ship_mode='NA' where ship_mode = 'Not Available';

select * from data_order where ship_mode= 'unknown';
update data_order set ship_mode='NA' where ship_mode ='unknown';
update data_order set ship_mode='NA' where ship_mode='N/A';
select * from data_order where ship_mode='NA';


-- 3. Find out Sale price, Discount and Profit and create new columns
alter table data_order
add discount_r decimal(20,5) not null;

UPDATE data_order set discount_r = list_price* (discount_per/100);
alter table data_order drop discount;

alter table data_order add sale_price decimal(20,5) not null;
update data_order set sale_price= list_price - discount_r;

alter table data_order add profit decimal (20,5);
update data_order set profit = sale_price - cost_price;

select * from data_order;

-- 4. Drop column Cost price, List price and discount Percentage
alter table data_order drop cost_price, drop list_price, drop discount_per;
select * from data_order;

-- So we have got our desired dataset now we will begin with our data analysis.

-- Total revenue
select sum(sale_price) as Total_revenue from data_order;

-- Total Profit
select sum(profit) as Total_profit from data_order;

-- Total quantity sold
select sum(quantity) as Tot_quantity_sold from data_order;

-- Segment wise revenue generation
select segment, sum(sale_price) as Total_sales from data_order
GROUP BY segment
ORDER BY Total_sales DESC;

-- City wise revenue genereation (top 10)
select city, sum(sale_price) as Total_sales from data_order
GROUP BY City
ORDER BY Total_sales DESC
limit 10;

select count(DISTINCT state) from df_orders;

-- State wise revenue generation top 10
select state, sum(sale_price) as Total_sales from data_order
GROUP BY state
ORDER BY Total_sales DESC
LIMIT 10;


-- Region wise revenue generation
SELECT region, sum(sale_price) as total_sales from data_order 
GROUP BY region
order by total_sales Desc;


-- category wise revenue generation
select category, sum(sale_price) as total_sales from data_order
group by category
order by total_sales desc;

-- sub category wise revenue generation
select sub_category, sum(sale_price) as total_sales from data_order
group by sub_category
order by total_sales desc;


-- Top 10 highest revenue generating products
select product_id, sub_category, sum(sale_price) as Total_sales from data_order
group by product_id, sub_category
order by Total_sales Desc
limit 10;


-- top 5 highest selling products in each region
select *from(
select *, RANK() over (partition by region order by total_sales DESC)
as Ranking
from(
select region, product_id, sum(sale_price) as total_sales from data_order
group by region, product_id
ORDER BY region, total_sales desc)as a)as b
where Ranking<=5;


-- Find month over month growth comparison for 2022 and 2023 sales
select distinct year(order_date) from data_order;

SELECT 
    order_month,
    SUM(CASE
        WHEN order_year = 2022 THEN sales
        ELSE 0
    END) AS sales_2022,
    SUM(CASE
        WHEN order_year = 2023 THEN sales
        ELSE 0
    END) AS sales_2023
FROM
    (SELECT 
        YEAR(order_date) AS order_year,
            MONTH(order_date) AS order_month,
            SUM(sale_price) AS sales
    FROM
        data_order
    GROUP BY YEAR(order_date) , MONTH(order_date)) AS a
GROUP BY order_month
ORDER BY order_month;


-- which month has highest sales for each category
select * from(
select * , RANK() over (PARTITION BY category ORDER BY sales DESC) as Ranking 
from(
select category, month(order_date) as month_name, year(order_date) as year_name, sum(sale_price) as sales from data_order
group by category, month_name, year_name
order by category,month_name) as a) as b
where Ranking=1
;


-- which sub category has highest growth by profit in 2023 campare to 2022

select *, ((profit_2023-profit_2022)*100/profit_2022) as growth
from(
select sub_category, 
sum(case when order_year = 2022 then total_profit else 0 end) as profit_2022,
sum(case when order_year = 2023 then total_profit else 0 end) as profit_2023
from(

select sub_category, year(order_date) as order_year,sum(profit) as total_profit from data_order
group by sub_category, order_year) as a
group by sub_category)as b
ORDER BY growth DESC
;

-- which sub category has highest growth percentage by profit in 2023 campare to 2022 wrt each region (top 3 sub categories)

select region, sub_category, profit_2022, profit_2023, growth from(
select *,
Rank() OVER (PARTITION BY region order by growth DESC) as Ranking
from(
select *, ((profit_2023-profit_2022)*100/profit_2022) as growth from(
select region,sub_category, 
sum(case when order_year=2022 then total_profit else 0 end) as profit_2022,
sum(case when order_year=2023 then total_profit else 0 end) as profit_2023
from(
select region,sub_category, year(order_date) as order_year, sum(profit) as total_profit from data_order
group by region,sub_category, order_year)as a
group by region,sub_category) as b
GROUP BY region, sub_category) as c
group by region, sub_category)as d
where Ranking<=3
;

-- which sub category has highest growth percentage by sales in 2023 campare to 2022 wrt each region (top 3 categories)

select region, sub_category, sale_2022, sale_2023, growth_per from(
select *, 
rank() over(partition by region order by growth_per DESC) as ranking
from(
select *, (((sale_2023-sale_2022)*100)/sale_2022) as growth_per
from(
select region, sub_category, 
sum(case when order_year=2022 then sales else 0 end) as sale_2022,
sum(case when order_year=2023 then sales else 0 end) as sale_2023
from(
select region, sub_category, year(order_date) as order_year, sum(sale_price) as sales from data_order
GROUP BY region, sub_category, order_year) as a
group by region, sub_category)as b) as c) as d
where ranking<= 3
;

-- which segment has highest growth percentage in profit 

select*, (((profit_2023-profit_2022)*100)/profit_2022) as growth_per
from(
select segment,
sum(case when order_year= 2022 then total_profit else 0 end) as profit_2022,
sum(case when order_year= 2023 then total_profit else 0 end) as profit_2023
from(
select segment, year(order_date) as order_year, sum(profit) as total_profit from data_order
group by segment, order_year) as a
group by segment)as b
ORDER BY growth_per DESC
;


-- which segment has highest sales_growth percentage

select*, (((sales_2023-sales_2022)*100)/sales_2022) as growth_per
from(
select segment,
sum(case when order_year= 2022 then total_sales else 0 end) as sales_2022,
sum(case when order_year= 2023 then total_sales else 0 end) as sales_2023
from(
select segment, year(order_date) as order_year, sum(sale_price) as total_sales from data_order
group by segment, order_year) as a
group by segment)as b
ORDER BY growth_per DESC
;

