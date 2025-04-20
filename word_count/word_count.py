import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.expressions import col
from pyflink.table.udf import udtf

# Define the main execution logic when the script is run directly
if __name__ == "__main__":
    """Modern PyFlink Word Count Example using Table API."""
    # 1. Create Environment
    # Use StreamExecutionEnvironment for unified batch/streaming
    env = StreamExecutionEnvironment.get_execution_environment()
    # Create TableEnvironment
    t_env = StreamTableEnvironment.create(env)

    # Define the input data
    input_data = ["Who's there?", "I think I hear them. Stand, ho! Who's there?"]

    # 2. Create Source Table
    # Create a Table directly from the input data
    # Define the schema with a single column 'line' of type STRING
    source_table = t_env.from_elements(input_data, schema=DataTypes.ROW([DataTypes.FIELD("line", DataTypes.STRING())]))

    # 3. Define UDTF for splitting lines into words
    @udtf(result_types=[DataTypes.STRING()])
    def split(row):
        for word in row.line.lower().split():
            yield word

    # 4. Define the Execution Logic (Word Count)
    result_table = source_table \
        .flat_map(split(col('line')).alias('word')) \
        .group_by(col('word')) \
        .select(col('word'), col('word').count.alias('count'))

    # 5. Emit Results (Print to Console)
    # Instead of writing to a file, print results for simplicity
    # The execute() call triggers the job execution
    print("Word Count Results:")
    # In cluster mode, execute().print() might not behave as expected for batch jobs.
    # It's better to collect results or write to a sink.
    # For simplicity here, we keep print(), but be aware it might only show on TaskManager logs.
    result_table.execute().print()
    print("Word Count Job Submitted (check logs/UI for results).")

# Note: Removed main() function. Execution starts in the __main__ block.
