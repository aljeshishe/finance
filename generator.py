import itertools
from functools import partial

from utils import as_list


def generator(task, **kwargs):
    keys = []
    values = []
    for k, v in kwargs.items():
        keys.append(k)
        values.append(as_list(v))
    args_list = [dict(zip(keys, items)) for items in itertools.product(*values)]
    for args in args_list:
        yield partial(task, **args)

