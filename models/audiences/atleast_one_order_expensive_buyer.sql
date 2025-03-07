{% 
 set cols_needed = {
    'atleast_one_order_completed': ['atleast_one_order_completed'],
    'expensive_buyer': ['avg_amt', 'expensive_buyer'],  
 }
 
 set keys = [
    'customer_id'
 ]
%}

final as (
    select 
    {% ','.join(keys) %},    
    {% ','.joinlist(itertools.chain.from_iterable(list(cols_needed.values()))) %}
    from
    {% list(cols_needed.keys()) %}
)

select * from final

