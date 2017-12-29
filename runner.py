import os
import sys

__author__ = 'willmcginnis'

project_directory = str(os.path.dirname(os.path.abspath(__file__)))


def run_data_enrichment():
    cmd = '%s %s %s' % (
        PYFLINK,
        project_directory + os.sep + 'data_enrichment' + os.sep + 'data_enrichment.py',
        ' - %s' % (project_directory, )
    )
    os.system(cmd)


def run_word_count():
    cmd = '%s %s %s' % (
        PYFLINK,
        project_directory + os.sep + 'word_count' + os.sep + 'word_count.py',
        ' - %s' % (project_directory, )
    )
    os.system(cmd)


def run_trending_hashtags():
    cmd = '%s %s %s' % (
        PYFLINK,
        project_directory + os.sep + 'trending_hashtags' + os.sep + 'trending_hashtags.py',
        ' - %s' % (project_directory, )
    )
    os.system(cmd)


def run_mean_values():
    cmd = '%s %s %s' % (
        PYFLINK,
        project_directory + os.sep + 'mean_values' + os.sep + 'mean_values.py',
        ' - %s' % (project_directory, )
    )
    os.system(cmd)


def run_mandelbrot():
    cmd = '%s %s %s' % (
        PYFLINK,
        project_directory + os.sep + 'mandelbrot' + os.sep + 'mandelbrot_set.py',
        ' - %s' % (project_directory, )
    )
    os.system(cmd)


if __name__ == '__main__':
    PYFLINK = sys.argv[1]

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
