import os
import sys
# Assuming each example directory contains a runnable script (e.g., word_count.py)
# and that script has a main function or can be run directly via import.
# We might need to adjust the individual scripts later.
from data_enrichment import data_enrichment
from word_count import word_count
from trending_hashtags import trending_hashtags
from mean_values import mean_values
from mandelbrot import mandelbrot_set

__author__ = 'willmcginnis'

project_directory = str(os.path.dirname(os.path.abspath(__file__)))

# Functions to run examples by calling their main logic
# (Assuming they have a main() function or similar entry point)
def run_data_enrichment():
    print("\n--- Running Data Enrichment ---")
    # Pass the project_directory as base_path
    data_enrichment.main(base_path=project_directory)

def run_word_count():
    print("\n--- Running Word Count ---")
    # Pass the project_directory as base_path (though word_count might not use it)
    word_count.main(base_path=project_directory)

def run_trending_hashtags():
    print("\n--- Running Trending Hashtags ---")
    # Pass the project_directory as base_path
    trending_hashtags.main(base_path=project_directory)

def run_mean_values():
    print("\n--- Running Mean Values ---")
    # Pass the project_directory as base_path
    mean_values.main(base_path=project_directory)

def run_mandelbrot():
    print("\n--- Running Mandelbrot Set ---")
    # Pass the project_directory as base_path
    mandelbrot_set.main(base_path=project_directory)

if __name__ == '__main__':
    # PYFLINK argument is no longer needed as we use the installed apache-flink library
    # if len(sys.argv) < 2:
    #     print("Usage: runner.py <path_to_pyflink_executable>")
    #     sys.exit(1)
    # PYFLINK = sys.argv[1] # Removed

    print('RUNNING MANDELBROT SET EXAMPLE')
    run_mandelbrot()

    print('RUNNING DATA ENRICHMENT EXAMPLE')
    run_data_enrichment()

    print('RUNNING WORD COUNT EXAMPLE')
    run_word_count()

    print('RUNNING TRENDING HASHTAG EXAMPLE')
    run_trending_hashtags()

    print('RUNNING MEAN VALUES EXAMPLE')
    run_mean_values()

    print("\n--- All Examples Finished ---")
