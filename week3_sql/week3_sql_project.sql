CREATE INDEX idx_region   ON orders(region);
CREATE INDEX idx_customer ON orders(customer);
CREATE INDEX idx_category ON orders(category);

show index from orders;

select * from orders;


/* ================================================================================================================ */
-- find the regionwise customer sales totals
-- customer ranked #1 by region wise sales
with totals as(
select o.customer,o.region,sum(o.sales) as customerwise_region_wise_total_sales from 
orders o
group by customer,region
),
-- rank those total_sales 
ranked as(
select *, 
rank() over(partition by region order by totals.customerwise_region_wise_total_sales desc ) as top_rank_by_sales
from totals
)
select ranked.customer,ranked.region,ranked.customerwise_region_wise_total_sales
from ranked 
where top_rank_by_sales = 1;
/* ================================================================================================================*/
-- orders higher than region avg and how much higher
with sales_avg as (
select * ,avg(sales) over(partition by region) as region_avg
from orders)

select order_id, customer, region, sales, region_avg,(sales - region_avg) as above_by 
from sales_avg
where sales > region_avg;

/* ================================================================================================================ */
with cte as(
select *,
lag(sales,1,NULL) over(partition by customer order by order_date) as prev_sales,
(sales -(lag(sales,1,NULL) over(partition by customer order by order_date))) as diff from 
orders)

select order_id, customer, region, sales,prev_sales,
CASE
  WHEN diff > 0 then 'UP'
  WHEN diff < 0 then 'DOWN'
ELSE 'FLAT' END AS trend
from cte
WHERE prev_sales is NOT NULL;
/* ================================================================================================================== */
with region_sales as
(
  select region,sum(sales) total_sales_region_wise
  from orders
  group by region
),
total_company_sales as
(
select sum(sales) over () as total_company_sales
from orders
)
select 
distinct(rs.region),
rs.total_sales_region_wise,
ts.total_company_sales,
round((rs.total_sales_region_wise / ts.total_company_sales) * 100,2)  as sales_pct
from region_sales rs
cross join total_company_sales ts
order by sales_pct desc;

/* ================================================================================================================== */
with category as (
select 
category,
round((sum(profit)/sum(sales)) * 100,2) as profit_margin
from orders
group by category
order by profit_margin desc
limit 1
),
individual_profit_margin as
(
   select * , 
   round((profit/sales) * 100,2) as profit_margin,
   rank() over(partition by category order by (round((profit/sales) * 100,2)) desc) as profit_margin_rank
   from orders
)
select pm.* 
from 
category c 
inner join individual_profit_margin pm
on c.category = pm.category
where pm.profit_margin_rank = 1