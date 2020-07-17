#! /usr/bin/env python3
import jinja2
import sys

from itertools import combinations

DIR = sys.argv[1]
TEMPLATE_FILE = sys.argv[2]

templateLoader = jinja2.FileSystemLoader(searchpath=DIR)
templateEnv = jinja2.Environment(loader=templateLoader)

def all_combinations(l):
    return [x for n in range(len(l) + 1) for x in combinations(l, n)]

templateEnv.globals.update(combinations=combinations, all_combinations=all_combinations)
template = templateEnv.get_template(TEMPLATE_FILE)
print(template.render())
