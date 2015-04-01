import functools
import math
import multiprocessing as mc
import random
import pylibmc as pmc


def populate(addr, count, value_size=4096):
    client = pmc.Client([addr], binary=True)
    with open('/dev/urandom') as f:
        for i in range(count):
            client[str(i)] = f.read(value_size)


def pareto_rand(min, alpha):
    return min / math.pow(random.random(), 1.0 / alpha)


def slap(addr, count, key_limit):
    client = pmc.Client([addr], binary=True)
    for _ in range(count):
        idx = int(pareto_rand(1, 0.07)) % key_limit
        client[str(idx)]


def parallel_slap(addr, count, key_limit, concurrency):
    handles = []
    for _ in range(concurrency):
        p = mc.Process(
            target=functools.partial(
                slap,
                addr,
                count,
                key_limit,
            )
        )
        handles.append(p)
    map(lambda x: x.start(), handles)
    map(lambda x: x.join(), handles)
