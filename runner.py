import os

__author__ = 'willmcginnis'


PYFLINK = 'pyflink3'
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


if __name__ == '__main__':
    print('RUNNING DATA ENRICHMENT EXAMPLE')
    run_data_enrichment()

    print('RUNNING WORD COUNT EXAMPLE')
    run_word_count()

    print('RUNNING TRENDING HASHTAG EXAMPLE')
    run_trending_hashtags()
