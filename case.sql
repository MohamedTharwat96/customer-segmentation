/******************exploring data(Q1)******************************/
-- what is the the average total price and total number of orders and customers?
select distinct country,
                count(distinct invoice) over(partition by country) as number_of_orders,
                count(distinct customer_id) over(partition by country) as number_of_customers,
                round(avg(price*quantity) over(partition by country),2) as avg_payment
from tableRetail;

-- what are the amount of orders per invoice?
select distinct invoice,
       count(invoice) over(partition by invoice) as number_of_orders
from tableretail
order by number_of_orders desc;

-- what are the total prices of orders per invoice?
select distinct invoice,
            round(sum(price*quantity) over(partition by invoice),0) as total_price_of_orders
from tableretail
order by total_price_of_orders desc;

-- what are the amount of orders per customer?
select distinct customer_id,
       count(invoice) over(partition by customer_id) as number_of_orders
from tableretail
order by number_of_orders desc;

-- what are the total prices of orders per customer?
select distinct customer_id,
            round(sum(price*quantity) over(partition by customer_id),0) as total_price_of_orders
from tableretail
order by total_price_of_orders desc;

/******************customer segmentation(Q2)******************************/

--set the metrics             
with metrics as (
        select customer_id, 
                 max(invoicedate) as most_recent_purchase ,
                 count(distinct invoice) as frequency,
                 sum(price * quantity) as monetary
                 from tableRetail 
                 group by customer_id
),
segments as(
        select customer_id,
                 most_recent_purchase,
                 ntile(5) over (order by most_recent_purchase) as R,
                 frequency,
                 ntile(5) over (order by frequency) as F,
                 monetary,
                 ntile(5) over (order by monetary) as M
        from metrics
),
aggregate as(
       select customer_id,
                R,
                F,
                M,
              trunc( (F+M)/2) as avg_FM
        from segments
)
        select customer_id,
                 R,
                 avg_FM,
                 case when R = 5 and avg_FM = 5 then 'Champions'
                         when R = 5 and avg_FM = 4 then 'Champions'
                         when R = 4 and avg_FM = 5 then 'Champions'
                         
                         when R = 5 and avg_FM = 2 then 'Potential Loyalists'
                         when R = 4 and avg_FM = 2 then 'Potential Loyalists'
                         when R = 3 and avg_FM = 3 then 'Potential Loyalists'
                         when R = 4 and avg_FM = 3 then 'Potential Loyalists'
                         
                         when R = 5 and avg_FM = 3 then 'Loyal Customers'
                         when R = 4 and avg_FM = 4 then 'Loyal Customers'
                         when R = 3 and avg_FM = 5 then 'Loyal Customers'
                         when R = 3 and avg_FM = 4 then 'Loyal Customers'
                
                         when R = 5 and avg_FM = 1 then 'Recent Customers'
                         
                         when R = 4 and avg_FM = 1 then 'Promising'
                         when R = 3 and avg_FM = 1 then 'Promising'
                     
                         when R = 3 and avg_FM = 2 then 'Customers Needing Attention'
                         when R = 2 and avg_FM = 3 then 'Customers Needing Attention'
                         when R = 2 and avg_FM = 2 then 'Customers Needing Attention'
                         when R = 2 and avg_FM = 1 then 'Customers Needing Attention'
                     
                         when R = 2 and avg_FM = 5 then 'At Risk'
                         when R = 2 and avg_FM = 4 then 'At Risk'
                         when R = 1 and avg_FM = 3 then 'At Risk'
              
                         when R = 1 and avg_FM = 5 then 'Cannot Lose Them'
                         when R = 1 and avg_FM = 4 then 'Cannot Lose Them'
                         
                         when R = 1 and avg_FM = 2 then 'Hibernating'
                     
                         when R = 1 and avg_FM = 1 then 'Lost'
            end customer_segments
            from aggregate
            order by customer_id;
            
/********************customer data(Q3-a)****************************/

--table creation
create table customers(
        Cust_Id number(10),
        Calendar_Dt varchar2(50),
        Amt_LE number(10,3)
);

with numbered as(
        select cust_id, 
                 calendar_dt , 
                 row_number() over (partition by cust_id order by calendar_dt) as rn
        from customers
),
based as(
        select cust_id,calendar_dt,
                 to_date(calendar_dt, 'YYYY-MM-DD')-rn as base_date
        from numbered 
),
counted as(
        select cust_id,calendar_dt,
                 base_date,count(*) over(partition by cust_id,base_date)as cons_trans 
        from based 
)
        select distinct(cust_id),
                 max(cons_trans) over(partition by cust_id) as max_cons_tran
        from counted
;
/********************customer data(Q3-b)****************************/

with acumulated as (
        select cust_id ,
                 calendar_dt, 
                 amt_le ,
                 sum(amt_le) over(partition by cust_id order by to_date(calendar_dt, 'YYYY-MM-DD')) as sum_amt , 
                 row_number() over(partition by cust_id order by to_date(calendar_dt, 'YYYY-MM-DD')) rnk ,
                 min(to_date(calendar_dt, 'YYYY-MM-DD')) over(partition by cust_id) first_dt
        from customers
),
filtered as(
        select distinct cust_id , 
                 min(rnk)over(partition by cust_id) num_transactions, 
                 min(to_date(calendar_dt, 'YYYY-MM-DD') - first_dt)over(partition by cust_id) num_days
         from acumulated
         where sum_amt >= 250
         order by cust_id asc
)
         select avg(num_transactions) avg_num_transactions , 
                  avg(num_days) avg_num_days
         from filtered
;



