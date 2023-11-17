from airflow.providers.jdbc.operators.jdbc import JdbcOperator

def check_table_length(table, min_rows=1000):
    jdbc_hook = JdbcOperator.get_hook('camelot_analytics_dept')

    # SQL query to count the number of rows in the table
    query = f"SELECT COUNT(*) FROM {table}"

    # Execute the query and fetch the result
    result = jdbc_hook.get_first(sql=query)
    row_count = result[0]

    # Perform check
    if row_count < min_rows:
        raise ValueError(f"Row count for your_table is below the expected minimum. Count: {row_count}")


