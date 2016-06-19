import os
import random
import sys

from flink.plan.Environment import get_environment
from flink.plan.Constants import WriteMode
from flink.functions.GroupReduceFunction import GroupReduceFunction

__author__ = 'willmcginnis'


class Adder(GroupReduceFunction):
    def reduce(self, iterator, collector):
        count, word = iterator.next()
        count += sum([x[0] for x in iterator])
        collector.collect((count, word))

if __name__ == "__main__":
    # get the base path out of the runtime params
    base_path = sys.argv[1]

    # setup paths to input and output files on disk
    input_file = 'file://' + base_path + '/trending_hashtags/in.txt'
    output_file = 'file://' + base_path + '/trending_hashtags/out.txt'

    # remove the output file, if there is one there already
    if os.path.isfile(output_file):
        os.remove(output_file)

    # likewise, generate the input file given some parameters.
    hashtags = ['#flink', '#python', '#apache']
    num_tweets = 1000
    with open(input_file.replace('file://', ''), 'w') as f:
        for tweet in range(num_tweets):
            f.write('Example tweet for https://github.com/wdm0006/flink-python-examples %s\n' % (random.choice(hashtags)))

    # set up the environment with a text file source
    env = get_environment()
    data = env.read_text(input_file)

    data \
        .flat_map(lambda x, c: [(1, word) for word in x.lower().split()]) \
        .filter(lambda x: '#' in x[1]) \
        .group_by(1) \
        .reduce_group(Adder(), combinable=True) \
        .map(lambda y: (str(y[0]), y[1])) \
        .write_csv(output_file, line_delimiter='\n', field_delimiter=',', write_mode=WriteMode.OVERWRITE)

    env.execute(local=True)
