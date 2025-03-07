with customers as (

    select * from {{ ref('stg_customers') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

final as (
    select customers.customer_id, customers.first_name, customers.last_name, avg(amount) as avg_amt, true as expensive_buyer
    from customers, orders
    where customers.customer_id = orders.customer_id
    and orders.status = 'completed'
  group by customers.customer_id, customers.first_name, customers.last_name
  having avgAmt > 20
  order by avgAmt desc
)

select * from final

