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

def main(resultjson, truthdir):
  failures = 0
  successes = 0
  with open(resultjson) as results_file:
    results = json.load(results_file)
    for query in results:
      with open(truthdir + "/" + query + ".truth") as truth_file:
        truth = json.load(truth_file)
        if results[query] != truth:
          print('Error at', query)
          failures += 1
        else:
          successes += 1
  if failures > 0:
    print("\nFAILED (", failures, "/", (successes+failures), ")\n")
  else:
    print("\nSUCCESS (", successes, ")\n")
  return failures

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
        "-resultjson",
        help="file containing json of results"
    )
    parser.add_argument(
        "-truthdir",
        help="directory containing json files of ground truth"
    )

    parsed_arguments = parser.parse_args()

    print("Starting comparison...")
    ret = main(parsed_arguments.resultjson, parsed_arguments.truthdir)
    if parsed_arguments.delete:
        os.remove(parsed_arguments.resultjson)
        countfile = parsed_arguments.resultjson.replace('.json', '.count')
        os.remove(countfile)

    if ret:
        sys.exit(1)
