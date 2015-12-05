import os
import sys
from flink.plan.Environment import get_environment
from flink.plan.Constants import INT, STRING, WriteMode
from flink.functions.GroupReduceFunction import GroupReduceFunction

if __name__ == "__main__":
    # get the base path out of the runtime params
    args = sys.argv[1:]

    # set up the environment with a source (in this case from a text file
    env = get_environment()
    data = env.read_text('foo')

    # build the job flow
    data \
        .map(lambda x: x, STRING) \
        .write_csv('bar', line_delimiter='\n', field_delimiter=',')

    # execute
    env.execute(local=True)