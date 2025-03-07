with customers as (

    select * from {{ ref('stg_customers') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (

    select * from {{ ref('stg_payments') }}

),

final as (
select
    stg_customers.customer_id,
    stg_customers.first_name,
    stg_customers.last_name,
    avg(amount) as avg_amt,
    true as expensive_buyer
  from
    stg_customers,
    stg_orders,
    stg_payments
  where
    stg_customers.customer_id = stg_orders.customer_id
    and stg_orders.status = 'completed'
    and stg_orders.order_id = stg_payments.order_id
  group by
    stg_customers.customer_id,
    stg_customers.first_name,
    stg_customers.last_name
  having avg_amt > 20
  order by
    avg_amt desc
)

select * from final

