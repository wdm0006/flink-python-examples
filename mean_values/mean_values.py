import os
import random
import sys

from flink.plan.Environment import get_environment
from flink.plan.Constants import INT, STRING, WriteMode, FLOAT
from flink.functions.ReduceFunction import ReduceFunction

__author__ = 'willmcginnis'


class MeanReducer(ReduceFunction):
    def reduce(self, x, y):
        return (x[0] + y[0],
                x[1] + y[1],
                x[2] + y[2])

if __name__ == "__main__":
    # get the base path out of the runtime params
    base_path = sys.argv[1]

    # setup paths to input and output files on disk
    input_file = 'file://' + base_path + '/mean_values/in.txt'
    output_file = 'file://' + base_path + '/mean_values/out.txt'

    # remove the output file, if there is one there already
    if os.path.isfile(output_file):
        os.remove(output_file)

    # likewise, generate the input file given some parameters.
    samples = 1000
    with open(input_file.replace('file://', ''), 'w') as f:
        for _ in range(samples):
            f.write('%f,%f\n' % (random.random(), random.random(), ))

    # set up the environment with a text file source
    env = get_environment()
    data = env.read_csv(input_file, types=[FLOAT, FLOAT])

    # add a 1 in the 0 index for counting the number of samples, then reduce to get all sums and divide for means
    data \
        .map(lambda x: (1, x[0], x[1]), (INT, FLOAT, FLOAT)) \
        .group_by(0) \
        .reduce(MeanReducer()) \
        .map(lambda x: (x[1]/x[0], x[2]/x[0]), (FLOAT, FLOAT)) \
        .map(lambda x: 'mean of col 0: %f\nmean of col 1: %f' % (x[0], x[1]), STRING) \
        .write_text(output_file, write_mode=WriteMode.OVERWRITE)

    env.execute(local=True)