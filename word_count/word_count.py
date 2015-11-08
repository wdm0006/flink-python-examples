from flink.plan.Environment import get_environment
from flink.plan.Constants import INT, STRING, WriteMode
from flink.functions.GroupReduceFunction import GroupReduceFunction


class Adder(GroupReduceFunction):
    def reduce(self, iterator, collector):
        count, word = iterator.next()
        count += sum([x[0] for x in iterator])
        collector.collect((count, word))

if __name__ == "__main__":
    output_file = 'file:///.../out.txt'
    print(output_file)

    env = get_environment()
    data = env.from_elements("Who's there? I think I hear them. Stand, ho! Who's there?")

    data \
        .flat_map(lambda x, c: [(1, word) for word in x.lower().split()], (INT, STRING)) \
        .group_by(1) \
        .reduce_group(Adder(), (INT, STRING), combinable=True) \
        .map(lambda y: 'Count: %s Word: %s' % (y[0], y[1]), STRING) \
        .write_text(output_file, write_mode=WriteMode.OVERWRITE)

    env.execute(local=True)