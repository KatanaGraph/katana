#!/usr/bin/env python3
#
# Script to check the output of algorithms:
# Author: Daniel Mawhirter (daniel@mawhirter.com)
#
# Expects json files, intended for ordered results from querying

import argparse
import os
import json
import sys
import logging
import traceback

def compare(resultfile, truthfile):
  correct = True
  try:
    with open(resultfile) as result:
      result_dicts = [ json.loads(line.strip()) for line in result ]
  except FileNotFoundError:
    logging.error("missing result file %s", resultfile)
    return False

  try:
    with open(truthfile) as truth:
      truth_dicts = [ json.loads(line.strip()) for line in truth ]
  except FileNotFoundError:
    logging.error("missing truth file %s", truthfile)
    return False

  try:
    if len(result_dicts) != len(truth_dicts):
      logging.error("length mismatch: result=%s truth=%s", len(result_dicts), len(truth_dicts))
      correct = False
    for r,t in zip(result_dicts, truth_dicts):
      if r != t:
        logging.error("first mismatch: %s VS %s", r, t)
        correct = False
        break
    if not correct:
      left = [ dict(sorted(a.items())) for a in result_dicts if a not in truth_dicts ]
      logging.error("only in results: %s of %s items (first 10):\n%s", len(left), len(result_dicts),
                    '\n'.join([ str(x) for x,_ in zip(left, range(10)) ]))
      right = [ dict(sorted(b.items())) for b in truth_dicts if b not in result_dicts ]
      logging.error("only in truth: %s of %s items (first 10):\n%s", len(right), len(truth_dicts),
                    '\n'.join([ str(x) for x,_ in zip(right, range(10)) ]))
  except Exception as e:
    logging.error("exception: %s", str(e))
    traceback.print_exc()
    correct = False
  finally:
    return correct

def main(querylist, resultdir, truthdir, delete):
  successes = 0
  failures = 0
  with open(querylist) as queries_file:
    for line in queries_file:
      if line[0].startswith('#'):
        continue
      query_args = line.strip().split()
      query_name = query_args[0]
      if len(query_args) == 1:
        query_filename = os.path.join(resultdir, query_name + ".json")
        truth_filename = os.path.join(truthdir, query_name + ".truth_json")
      elif len(query_args) == 2:
        param_name = query_args[1]
        query_filename = os.path.join(resultdir, query_name + "_" + param_name + ".json")
        truth_filename = os.path.join(truthdir, query_name + "_" + param_name + ".truth_json")
      else:
        print("query args unclear:", query_args)
        return 1
      ret = compare(query_filename, truth_filename)
      if ret:
        successes += 1
        if delete:
          os.remove(query_filename)
      else:
        failures += 1
        logging.error('mismatch with query args %s', str(query_args))
  if failures > 0:
    print("\nFAILED (", failures, "/", (successes+failures), ")\n")
    return 1
  else:
    print("\nSUCCESS (", successes, ")\n")
    return 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check graph output results as json")

    parser.add_argument(
        "-delete",
        "-d",
        type=bool,
        nargs=1,
        default=False,
        help="delete the generated output files",
    )
    parser.add_argument(
        "-querylist",
        help="list of queries being tested"
    )
    parser.add_argument(
        "-resultdir",
        help="prefix for files containing json of results"
    )
    parser.add_argument(
        "-truthdir",
        help="directory containing json files of ground truth"
    )

    parsed_arguments = parser.parse_args()

    print("Starting comparison...")
    ret = main(parsed_arguments.querylist, parsed_arguments.resultdir,
               parsed_arguments.truthdir, parsed_arguments.delete)
    if parsed_arguments.delete:
        countfile = parsed_arguments.resultdir + ".count"
        if os.path.exists(countfile):
          os.remove(countfile)

    if ret:
        sys.exit(1)
