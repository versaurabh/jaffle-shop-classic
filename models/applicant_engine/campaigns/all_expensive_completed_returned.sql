with expensive_completed as (

    select * from {{ ref('expensive_completed') }}

),

expensive_returned as (
    select * from {{ ref('expensive_returned') }}

),

final as (
    select expensive_completed.customer_id, expensive_completed.first_name, expensive_completed.last_name,
    expensive_completed.expensive_completed, expensive_returned.expensive_returned
    from expensive_completed, expensive_returned
    where expensive_completed.customer_id = expensive_returned.customer_id
)

select * from final

