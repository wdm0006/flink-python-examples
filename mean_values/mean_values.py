import os
import random
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings
from pyflink.table.expressions import col

__author__ = 'willmcginnis'

# Helper function to generate input data
def generate_input_file(file_path):
    """Generates a sample CSV input file with random floats."""
    samples = 100 # Reduced for faster local testing
    print(f"Generating sample input file: {file_path}")
    try:
        # Ensure directory exists before writing
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as f:
            for _ in range(samples):
                f.write('%f,%f\n' % (random.random(), random.random()))
        print("Sample input file generated.")
    except IOError as e:
        print(f"Error generating input file: {e}")
        raise

# Define the main execution logic
if __name__ == "__main__":
    """Modern PyFlink Mean Values Example using Table API."""

    # Base path inside the container
    base_path = '/opt/flink/usrlib'

    # Construct paths relative to the container mount point
    input_dir = os.path.join(base_path, 'mean_values')
    input_file_abs = os.path.join(input_dir, 'in.txt')

    # Generate the input file *inside the container*
    generate_input_file(input_file_abs)

    # 1. Create Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)

    # 2. Create Source Table using DDL
    # Path is now absolute inside the container
    t_env.execute_sql(f"""
        CREATE TABLE mean_source (
            col0 DOUBLE,
            col1 DOUBLE
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{input_file_abs}',
            'format' = 'csv'
        )
    """)

    # 3. Define the Execution Logic (Calculate Mean)
    source_table = t_env.from_path('mean_source')

    # Use built-in aggregate functions via column methods (.avg)
    result_table = source_table.select(col('col0').avg.alias('mean_col0'),
                                       col('col1').avg.alias('mean_col1'))

    # 4. Emit Results (Print to Console)
    print("\nMean Values Results:")
    result_table.execute().print()
    print("Mean Values Job Submitted (check logs/UI for results).")

# Removed main() function
