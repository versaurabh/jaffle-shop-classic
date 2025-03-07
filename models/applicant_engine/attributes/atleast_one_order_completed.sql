with customers as (

    select * from {{ ref('seed_customers') }}

),

orders as (

    select * from {{ ref('seed_orders') }}

),

final as (
    select distinct customers.customer_id, customers.first_name, customers.last_name, TRUE as atleast_one_order_completed
    from customers, orders
    where customers.customer_id = orders.customer_id
    and orders.status = 'completed'
)

select * from final

