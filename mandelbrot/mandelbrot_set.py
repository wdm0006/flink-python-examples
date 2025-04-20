import os
import sys
import numpy as np
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings
from pyflink.table.expressions import col
from pyflink.table.udf import udf

__author__ = 'willmcginnis/1oclockbuzz'

def check_c(real_part, imag_part, rel_tol=1e-6, max_zmag=1e6, max_iter=500):
    """Checks whether or not c is in the Mandelbrot Set.

    Accepts real and imaginary parts as separate floats.
    Returns a tuple (real, imag, magnitude) or None if not in the set or calculation fails.
    """
    try:
        c_complex = complex(float(real_part), float(imag_part))
        z = np.zeros(max_iter, dtype=complex)
        zmag = np.zeros(max_iter)

        for i in range(1, max_iter):
            z[i] = z[i-1]**2 + c_complex
            zmag[i] = np.abs(z[i])

            if np.isnan(z[i]) or zmag[i] > max_zmag:
                return None # Not in the set or diverged too quickly

            # Check for convergence (close enough to previous magnitude)
            if i > 1 and zmag[i] > 1e-12: # Avoid division by zero or near-zero
                rel_diff = np.abs((zmag[i] - zmag[i-1]) / zmag[i])
                if rel_diff < rel_tol:
                    return (real_part, imag_part, float(zmag[i]))
            elif np.abs(zmag[i] - zmag[i-1]) < rel_tol * 1e-9: # Handle convergence near zero
                 return (real_part, imag_part, float(zmag[i]))

        # If loop finishes without diverging or converging (unlikely with high max_iter)
        return (real_part, imag_part, float(zmag[max_iter-1]))

    except Exception as e:
        # Log error or handle it appropriately
        print(f"Error in check_c for ({real_part}, {imag_part}): {e}", file=sys.stderr)
        return None

def candidates(n):
    """Generates candidate complex numbers for Mandelbrot Set."""
    # Using numpy for linspace
    for r_part in np.linspace(-2, 2, n):
        for c_part in np.linspace(-1, 1, n):
            yield (float(r_part), float(c_part))

# Helper function to generate input data
def generate_input_file(file_path, n=50):
    """Generates a sample CSV input file with candidate complex numbers."""
    print(f"Generating sample input file: {file_path} with {n}x{n} candidates")
    try:
        # Ensure directory exists before writing
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as f:
            for c in candidates(n):
                f.write('%f,%f\n' % (c[0], c[1]))
        print("Sample input file generated.")
    except IOError as e:
        print(f"Error generating input file: {e}")
        raise

# Define the main execution logic
if __name__ == "__main__":
    """Modern PyFlink Mandelbrot Set Example using Table API."""

    # Base path inside the container
    base_path = '/opt/flink/usrlib'

    # Construct paths relative to the container mount point
    input_dir = os.path.join(base_path, 'mandelbrot')
    input_file_abs = os.path.join(input_dir, 'in.txt')

    # Generate the input file (adjust n for desired resolution/speed)
    # This happens when the script is executed by Flink
    generate_input_file(input_file_abs, n=30) # Reduced n for faster testing

    # 1. Create Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)

    # 2. Create Source Table using DDL
    # Path is now absolute inside the container
    t_env.execute_sql(f"""
        CREATE TABLE mandelbrot_source (
            real_part DOUBLE,
            imag_part DOUBLE
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{input_file_abs}',
            'format' = 'csv'
        )
    """)

    # 3. Define UDF wrapping the check_c logic
    # Returns ROW<real DOUBLE, imag DOUBLE, magnitude DOUBLE>
    @udf(result_type=DataTypes.ROW([
        DataTypes.FIELD("real", DataTypes.DOUBLE()),
        DataTypes.FIELD("imag", DataTypes.DOUBLE()),
        DataTypes.FIELD("mag", DataTypes.DOUBLE())
    ]))
    def check_mandelbrot_udf(real_part, imag_part):
        return check_c(real_part, imag_part)

    # 4. Define the Execution Logic (Apply UDF, Filter)
    source_table = t_env.from_path('mandelbrot_source')

    result_table = source_table \
        .select(check_mandelbrot_udf(col('real_part'), col('imag_part')).alias('m_result')) \
        .filter(col('m_result').is_not_null) # Filter out points not in the set (where UDF returned None)
        # Optional: select individual fields if needed
        # .select(col('m_result').get('real'), col('m_result').get('imag'), col('m_result').get('mag'))

    # 5. Emit Results (Print to Console)
    print("\nMandelbrot Set Results (Real, Imaginary, Magnitude):")
    # Increase default max content width for printing rows
    t_env.get_config().set("sql.result.max-content-width", "200")
    result_table.execute().print()
    print("Mandelbrot Job Submitted (check logs/UI for results).")

# Removed main() function

