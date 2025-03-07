from pyspark.sql.functions import col, coalesce, first

def model(dbt, session):
    # setting configuration
    dbt.config(
        materialized="table",
        cluster_id="0307-180933-gziyazvd",
    )

    dataframes = [
        "atleast_one_order_completed",
        "atleast_one_order_returned",
        "expensive_buyer",
        "expensive_completed",
        "expensive_returned",
        "all_expensive_completed_returned",
    ]

    group_key = "customer_id"

    # creating the final pivot table
    # 🔹 Align schemas by adding missing columns before union
    first_df = dataframes[0]  # Use first DF as reference schema

    aligned_dfs = []
    for df in dataframes:
        # Align missing columns
        for col_name, col_type in first_df.dtypes:
            if col_name not in df.columns:
                df = df.withColumn(col_name, lit(None).cast(col_type))
        aligned_dfs.append(df)

    # 🔹 Perform unionByName in a loop
    final_df = aligned_dfs[0]  # Start with the first DF
    for df in aligned_dfs[1:]:
        final_df = final_df.unionByName(df, allowMissingColumns=True)

    columns_to_coalesce = [col_name for col_name in final_df.columns if col_name != group_key]

    # Build aggregation expressions dynamically
    agg_exprs = [coalesce(first(col(c)), first(col(c))).alias(c) for c in columns_to_coalesce]

    # Perform groupBy and aggregation
    df_grouped = df.groupBy(group_key).agg(*agg_exprs)

    # Show Result
    df_grouped.show()

    return df_grouped
