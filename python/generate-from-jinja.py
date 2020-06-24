#! /usr/bin/env python3
import jinja2
import sys

DIR = sys.argv[1]
TEMPLATE_FILE = sys.argv[2]

templateLoader = jinja2.FileSystemLoader(searchpath=DIR)
templateEnv = jinja2.Environment(loader=templateLoader)
template = templateEnv.get_template(TEMPLATE_FILE)
print(template.render())
