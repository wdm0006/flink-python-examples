import os
import json
import sys
import numpy as np
from flink.plan.Environment import get_environment
from flink.plan.Constants import INT, STRING, FLOAT, WriteMode
from flink.functions.GroupReduceFunction import GroupReduceFunction

__author__ = 'willmcginnis/1oclockbuzz'


def check_c(c, rel_tol=1e-6, max_zmag=1e6):
    """Checks whether or not c is in the Mandelbrot Set

    from http://1oclockbuzz.com/2015/11/24/pyspark-and-the-mandelbrot-set-overkill-indeed/
    """

    import numpy as np

    c_complex = (float(c[0]) + float(c[1])*1j)

    z = np.zeros(500, dtype=complex)
    zmag = np.zeros(500)

    for i in range(1, 500):
        z[i] = z[i-1]**2 + c_complex
        zmag[i] = np.abs(z[i])
        if np.isnan(z[i]) or zmag[i] > max_zmag:
            return (-999, -999, -999)

        rel_diff = np.abs((zmag[i] - zmag[i-1])/zmag[i])
        if rel_diff < rel_tol:
            return (c[0], c[1], zmag[i])

    return (c[0], c[1], zmag[i])


def candidates(n):
    """Generates candidate complex numbers for Mandelbrot Set"""
    for r_part in np.linspace(-2, 2, n):
        for c_part in np.linspace(-1, 1, n):
            yield (r_part, c_part)


if __name__ == "__main__":
    # get the base path out of the runtime params
    base_path = sys.argv[1]

    # we have to hard code in the path to the output because of gotcha detailed in readme
    output_file = 'file://' + base_path + '/mandelbrot/out.txt'
    input_file = 'file://' + base_path + '/mandelbrot/in.txt'

    # remove the output file, if there is one there already
    if os.path.isfile(output_file):
        os.remove(output_file)

    with open(input_file.replace('file://', ''), 'w') as f:
        for c in candidates(50):
            f.write('%f,%f\n' % (c[0], c[1]))

    # set up the environment with a text file source
    env = get_environment()
    data = env.read_csv(input_file, types=[FLOAT, FLOAT])

    data \
        .map(lambda x: check_c(x), (FLOAT, FLOAT, FLOAT)) \
        .filter(lambda x: True if x[0] != -999 else False) \
        .map(lambda x: '%s + %sj, %s' % (x[0], x[1], x[2]), STRING) \
        .write_text(output_file, write_mode=WriteMode.OVERWRITE)

    # execute the plan locally.
    env.execute(local=True)

