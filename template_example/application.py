import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings
from pyflink.table.expressions import col

# Define the main execution logic when the script is run directly
if __name__ == "__main__":
    """Template for a PyFlink Table API batch job."""

    # 1. Create Execution Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    # Optionally configure the environment (e.g., parallelism)
    # env.set_parallelism(1)
    t_env = StreamTableEnvironment.create(env)

    print("Starting PyFlink Table API template job...")

    # === Configuration (Example) ===
    # Base path inside the container
    base_path = '/opt/flink/usrlib'
    # Example paths inside the container
    # input_path = os.path.join(base_path, 'template_example', 'input.csv')
    # output_path = os.path.join(base_path, 'template_example', 'output.txt')

    # === Source ===
    # Replace with your actual source. Examples:
    # From elements:
    source_data = [("hello", 1), ("world", 2), ("flink", 3)]
    source_table = t_env.from_elements(
        source_data,
        schema=DataTypes.ROW([
            DataTypes.FIELD("word", DataTypes.STRING()),
            DataTypes.FIELD("count", DataTypes.INT())
        ])
    )

    # From CSV:
    # t_env.execute_sql(f"""
    #     CREATE TABLE my_source (
    #         field1 STRING,
    #         field2 INT
    #     ) WITH (
    #         'connector' = 'filesystem',
    #         'path' = '{input_path}', # Use container path variable
    #         'format' = 'csv'
    #     )
    # """)
    # source_table = t_env.from_path('my_source')

    # === Transformations ===
    # Replace with your desired data processing logic
    # Example: Filter data
    result_table = source_table.filter(col('count') > 1)

    # Example: Select and rename columns
    # result_table = source_table.select(col('word').alias('item'), col('count').alias('value'))

    # Example: SQL Query
    # result_table = t_env.sql_query(f"SELECT UPPER(word), count FROM {source_table} WHERE count > 1")


    # === Sink ===
    # Replace with your actual sink. Examples:
    # Print to console:
    print("\nTemplate Job Results:")
    result_table.execute().print()
    print("Template Job Submitted (check logs/UI for results).")

    # Write to CSV:
    # t_env.execute_sql(f"""
    #     CREATE TABLE my_sink (
    #         word STRING,
    #         count INT
    #     ) WITH (
    #         'connector' = 'filesystem',
    #         'path' = '{output_path}', # Use container path variable
    #         'format' = 'csv'
    #     )
    # """)
    # result_table.execute_insert('my_sink').wait() # Use wait() for batch jobs to ensure completion

    print("\nPyFlink Table API template job finished submitting.")

# Note: Removed main() function definition.