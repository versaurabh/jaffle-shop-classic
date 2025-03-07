with expensive_completed as (

    select * from {{ ref('expensive_completed') }}

),

expensive_returned as (
    select * from {{ ref('expensive_returned') }}

),

final as (
    select * from expensive_completed, expensive_returned
    where expensive_completed.customer_id = expensive_returned.customer_id
)

select * from final

