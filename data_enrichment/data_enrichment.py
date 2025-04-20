import os
import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings
from pyflink.table.expressions import col
from pyflink.table.udf import udf

# Define the main execution logic
if __name__ == "__main__":
    """Modern PyFlink Data Enrichment Example using Table API."""
    # 1. Create Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)

    # Base path inside the container
    base_path = '/opt/flink/usrlib'

    # Construct absolute paths inside the container
    dim_file_path = os.path.join(base_path, 'data_enrichment', 'dimensional_data.csv')
    input_file_path = os.path.join(base_path, 'data_enrichment', 'input_data.csv')

    # Ensure the input files exist (they should be mounted)
    # TODO: Add checks or ensure data files are present before running

    # 2. Create Source Tables using DDL
    # Use absolute paths inside the container, remove 'file://' prefix
    t_env.execute_sql(f"""
        CREATE TABLE input_source (
            json_line STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{input_file_path}',
            'format' = 'csv',
            'csv.field-delimiter' = '|' -- Assuming a delimiter unlikely to be in JSON
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE dim_source (
            attr_key STRING,
            attr_value STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{dim_file_path}',
            'format' = 'csv'
        )
    """)

    # 3. Define UDF to parse JSON and extract fields
    @udf(result_type=DataTypes.ROW([
        DataTypes.FIELD("car", DataTypes.STRING()),
        DataTypes.FIELD("attr", DataTypes.STRING())
    ]))
    def parse_json(json_str):
        try:
            data = json.loads(json_str)
            # Handle potential missing keys gracefully
            return (data.get('car', ''), data.get('attr', ''))
        except json.JSONDecodeError:
            # Handle lines that are not valid JSON
            return (None, None)

    # 4. Define the Execution Logic (Join and Format)
    input_table = t_env.from_path('input_source')
    dim_table = t_env.from_path('dim_source')

    # Apply UDF and filter out failed parses
    parsed_input = input_table.select(parse_json(col('json_line')).alias("parsed")) \
                             .filter(col("parsed").is_not_null) \
                             .select(col("parsed").get("car").alias("car"),
                                     col("parsed").get("attr").alias("attr"))

    # Join the parsed input with the dimensional data
    result_table = parsed_input.join(dim_table, parsed_input.attr == dim_table.attr_key) \
                               .select(parsed_input.car, dim_table.attr_value)

    # 5. Emit Results (Print to Console)
    print("\nData Enrichment Results:")
    result_table.execute().print()
    print("Data Enrichment Job Submitted (check logs/UI for results).")

# Removed main() function definition
