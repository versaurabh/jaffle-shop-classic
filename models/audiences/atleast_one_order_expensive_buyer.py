def model(dbt, session):
    dbt.config(
        materialized='table'
    )

    cols_needed = {
     'atleast_one_order_completed': ['atleast_one_order_completed'],
     'expensive_buyer': ['avg_amt', 'expensive_buyer'],  
    }

    # these are DAG-aware, and return dataframes
    atleast_one_order_completed = dbt.ref("atleast_one_order_completed")
    expensive_buyer = dbt.ref("expensive_buyer")

    # your final 'select' statement
    # df = atleast_one_order_completed.join(expensive_buyer, )

    return atleast_one_order_completed