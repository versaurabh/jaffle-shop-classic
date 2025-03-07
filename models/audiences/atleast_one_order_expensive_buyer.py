def model(dbt, session):
    dbt.config(
        # submission_method="serverless_cluster",
        http_path = "/sql/1.0/warehouses/6177e39cfc5194cf",
        create_notebook=False,
    )

    cols_needed = {
     'atleast_one_order_completed': ['atleast_one_order_completed'],
     'expensive_buyer': ['avg_amt', 'expensive_buyer'],  
    }

    # these are DAG-aware, and return dataframes
    atleast_one_order_completed = dbt.ref("atleast_one_order_completed")
    expensive_buyer = dbt.ref("expensive_buyer")

    # your final 'select' statement
    df = atleast_one_order_completed.join(expensive_buyer, "customer_id")

    return df