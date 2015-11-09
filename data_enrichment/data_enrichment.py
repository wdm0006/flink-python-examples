import os
import json
import sys
from flink.plan.Environment import get_environment
from flink.plan.Constants import INT, STRING, WriteMode
from flink.functions.GroupReduceFunction import GroupReduceFunction

__author__ = 'willmcginnis'


class Adder(GroupReduceFunction):
    def reduce(self, iterator, collector):
        count, word = iterator.next()
        count += sum([x[0] for x in iterator])
        collector.collect((count, word))


def json_to_tuple(js, fields):
    return tuple([str(js.get(f, '')) for f in fields])

if __name__ == "__main__":
    # get the base path out of the runtime params
    base_path = sys.argv[1]

    # setup paths to input and output files on disk
    dim_file = 'file://' + base_path + '/data_enrichment/dimensional_data.csv'
    input_file = 'file://' + base_path + '/data_enrichment/input_data.csv'
    output_file = 'file://' + base_path + '/data_enrichment/out.txt'

    # remove the output file, if there is one there already
    if os.path.isfile(output_file):
        os.remove(output_file)

    # set up the environment with a text file source
    env = get_environment()
    input_data = env.read_text(input_file)
    dimensional_data = env.read_csv(dim_file, types=[STRING, STRING])

    input_data \
        .map(lambda x: json_to_tuple(json.loads(x), ['car', 'attr']), (STRING, STRING)) \
        .join(dimensional_data).where(1).equal_to(0) \
        .map(lambda x: 'This %s is %s' % (x[0][0], x[1][1]), STRING) \
        .write_text(output_file, write_mode=WriteMode.OVERWRITE)

    env.execute(local=True)