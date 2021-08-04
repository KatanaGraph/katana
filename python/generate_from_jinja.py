#! /usr/bin/env python3
import sys
from functools import lru_cache, partial
from itertools import combinations

import generator_conf
import jinja2


def all_combinations(l):
    return [x for n in range(len(l) + 1) for x in combinations(l, n)]


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


def run(root_dir, template_file, output_file):
    def generated_banner():
        return "THIS FILE IS GENERATED FROM '{0}'. Make changes to that file instead of this one.".format(template_file)

    template_env = _get_jinja_environment(root_dir)

    template_env.globals.update(
        combinations=combinations,
        all_combinations=all_combinations,
        generated_banner=generated_banner,
        nested_statements=nested_statements,
        partial=partial,
        indent=indent,
        **generator_conf.exports,
    )
    template = template_env.get_template(str(template_file))
    output = template.render()
    if output_file:
        try:
            with open(output_file, "rt", encoding="UTF-8") as f:
                old_output = f.read()
        except IOError:
            old_output = None

        if output == old_output:
            # print(f"{output_file} does not need to be updated.")
            return False

        with open(output_file, "wt", encoding="UTF-8") as f:
            # print(f"Writing {output_file}.")
            print(template.render(), file=f, end="")
            return True
    else:
        print(output, end="")
        return None


# Cache jinja environments to allow caching inside the environment. But don't cache many in case they get big.
@lru_cache(2)
def _get_jinja_environment(root_dir):
    template_loader = jinja2.FileSystemLoader(searchpath=[str(root_dir), str(f"{root_dir}/katana")])
    template_env = jinja2.Environment(loader=template_loader)
    return template_env


if __name__ == "__main__":
    DIR = sys.argv[1]
    TEMPLATE_FILE = sys.argv[2]
    run(DIR, TEMPLATE_FILE, None)
