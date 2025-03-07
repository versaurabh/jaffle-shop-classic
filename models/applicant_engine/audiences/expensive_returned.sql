with expensive as (

    select * from {{ ref('expensive_buyer') }}

),

completed as (

    select * from {{ ref('atleast_one_order_returned') }}

),

final as (
    select distinct expensive.customer_id, expensive.first_name, expensive.last_name, TRUE as expensive_completed
    from expensive, completed
    where expensive.customer_id = completed.customer_id
)

select * from final
