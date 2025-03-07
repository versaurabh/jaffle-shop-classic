with customers as (

    select * from {{ ref('seed_customers') }}

),

orders as (

    select * from {{ ref('seed_orders') }}

),

payments as (

    select * from {{ ref('seed_payments') }}

),

final as (
select
    customers.customer_id,
    customers.first_name,
    customers.last_name,
    avg(amount) as avg_amt,
    true as expensive_buyer
  from
    customers,
    orders,
    payments
  where
    customers.customer_id = orders.customer_id
    and orders.status = 'completed'
    and payments.order_id = payments.order_id
  group by
    customers.customer_id,
    customers.first_name,
    customers.last_name
  having avg_amt > 20
  order by
    avg_amt desc
)

select * from final

