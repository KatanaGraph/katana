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

def compare(resultfile, truthfile):
  try:
    with open(resultfile) as result:
      result_lines = [ line.strip() for line in result ]
    with open(truthfile) as truth:
      truth_lines = [ line.strip() for line in truth ]
    if len(result_lines) != len(truth_lines):
      logging.error("length mismatch: result=%s truth=%s", len(result_lines), len(truth_lines))
      return False
    for r,t in zip(result_lines, truth_lines):
      jr = json.loads(r)
      jt = json.loads(t)
      if jr != jt:
        logging.error("mismatch: %s VS %s", jr, jt)
        return False
    return True
  except FileNotFoundError:
    logging.error("a file is missing!")
    return False
  except Exception as e:
    logging.error("exception: %s", str(e))
    return False

def main(querylist, resultprefix, truthdir, delete):
  successes = 0
  failures = 0
  with open(querylist) as queries_file:
    for query_name in queries_file:
      query_name = query_name.strip()
      query_filename = resultprefix + "_" + query_name + ".json"
      truth_filename = truthdir + "/" + query_name + ".truth"
      ret = compare(query_filename, truth_filename)
      if ret:
        successes += 1
        if delete:
          os.remove(query_filename)
      else:
        failures += 1
        logging.error('mismatch at query %s', query_name)
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
        "-resultprefix",
        help="prefix for files containing json of results"
    )
    parser.add_argument(
        "-truthdir",
        help="directory containing json files of ground truth"
    )

    parsed_arguments = parser.parse_args()

    print("Starting comparison...")
    ret = main(parsed_arguments.querylist, parsed_arguments.resultprefix,
               parsed_arguments.truthdir, parsed_arguments.delete)
    if parsed_arguments.delete:
        countfile = parsed_arguments.resultprefix + ".count"
        if os.path.exists(countfile):
          os.remove(countfile)

    if ret:
        sys.exit(1)
