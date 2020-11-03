#! /usr/bin/env python3
import sys
from functools import partial
from itertools import combinations

import jinja2

import generator_conf

DIR = sys.argv[1]
TEMPLATE_FILE = sys.argv[2]

templateLoader = jinja2.FileSystemLoader(searchpath=DIR)
templateEnv = jinja2.Environment(loader=templateLoader)


def all_combinations(l):
    return [x for n in range(len(l) + 1) for x in combinations(l, n)]


def generated_banner():
    return "THIS FILE IS GENERATED FROM '{0}'. Make changes to that file instead of this one.".format(TEMPLATE_FILE)


def indent(n, s):
    return s.replace("\n", "\n" + " " * (n * 4))


def nested_statements(layers, *args, **kwargs):
    if layers:
        outer, *inners = layers

        def inner(depth, *args, **kwargs):
            s = nested_statements(inners, *args, **kwargs)
            return indent(depth, s)

        return outer(inner, *args, **kwargs)
    raise RuntimeError("The last layer must not call inner.")


templateEnv.globals.update(
    combinations=combinations,
    all_combinations=all_combinations,
    generated_banner=generated_banner,
    nested_statements=nested_statements,
    partial=partial,
    indent=indent,
    **generator_conf.exports
)
template = templateEnv.get_template(TEMPLATE_FILE)
print(template.render())
