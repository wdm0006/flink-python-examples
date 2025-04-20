import os
import random
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings
from pyflink.table.expressions import col, lit
from pyflink.table.udf import udtf

__author__ = 'willmcginnis'

# Helper function to generate input data
def generate_input_file(file_path):
    """Generates a sample input file with random hashtags."""
    hashtags = ['#flink', '#python', '#apache']
    num_tweets = 100 # Reduced for faster local testing
    print(f"Generating sample input file: {file_path}")
    try:
        # Ensure directory exists before writing
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as f:
            for _ in range(num_tweets):
                f.write('Example tweet for https://github.com/wdm0006/flink-python-examples %s\n' % (random.choice(hashtags)))
        print("Sample input file generated.")
    except IOError as e:
        print(f"Error generating input file: {e}")
        # Decide if we should exit or try to continue without the file
        raise # Re-raise the exception for now

# Define the main execution logic
if __name__ == "__main__":
    """Modern PyFlink Trending Hashtags Example using Table API."""

    # Base path inside the container
    base_path = '/opt/flink/usrlib'

    # Construct paths relative to the container mount point
    input_dir = os.path.join(base_path, 'trending_hashtags')
    input_file_abs = os.path.join(input_dir, 'in.txt')

    # Generate the input file *inside the container* where Flink can access it
    # This assumes the script itself is run from the container's context
    generate_input_file(input_file_abs)

    # 1. Create Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)

    # 2. Create Source Table using DDL
    # Use text format to read whole lines
    # Path is now absolute inside the container, no 'file://' needed
    t_env.execute_sql(f"""
        CREATE TABLE hashtag_source (
            line STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{input_file_abs}',
            'format' = 'text'
        )
    """)

    # 3. Define UDTF for extracting hashtags
    @udtf(result_types=[DataTypes.STRING()])
    def extract_hashtags(row):
        for word in row.line.lower().split():
            if word.startswith('#'):
                yield word

    # 4. Define the Execution Logic (Filter, Group, Count)
    source_table = t_env.from_path('hashtag_source')

    result_table = source_table \
        .flat_map(extract_hashtags(col('line')).alias('hashtag')) \
        .group_by(col('hashtag')) \
        .select(col('hashtag'), lit(1).count.alias('count')) # Use lit(1).count for counting

    # 5. Emit Results (Print to Console)
    print("\nTrending Hashtags Results:")
    result_table.execute().print()
    print("Trending Hashtags Job Submitted (check logs/UI for results).")

# Removed main() function
