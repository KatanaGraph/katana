import argparse
import contextlib
import json
import os
import sys
import time
from collections import namedtuple
from datetime import datetime
import pytest

import numpy as np
import pytz
from pyarrow import Schema

import katana


def GenerateArgs(json_output, input_dir="./", graph="GAP-road", app="bfs", source_nodes="", trails=1, num_sources=4, thread_spin=False, threads=None):

    parser = argparse.ArgumentParser(
        description="Benchmark performance of routines")
    parser.add_argument("--input-dir", default=input_dir,
                        help="Path to the input directory (default: %(default)s)")

    parser.add_argument(
        "--threads",
        type=int,
        default=threads,
        help="Number of threads to use (default: query sinfo). Should match max threads.",
    )
    parser.add_argument("--thread-spin", default=thread_spin,
                        action="store_true", help="Busy wait for work in thread pool.")

    parser.add_argument(
        "--json-output", default=json_output, help="Path at which to save performance data in JSON")

    parser.add_argument(
        "--graph",
        default=graph,
        choices=["GAP-road", "GAP-kron", "GAP-twitter",
                 "GAP-web", "GAP-urand", "rmat15"],
        help="Graph name (default: %(default)s)",
    )
    parser.add_argument(
        "--application",
        default=app,
        choices=["bfs", "sssp", "cc", "bc", "pagerank",
                 "tc", "jaccard", "kcore", "louvain", "all"],
        help="Application to run (default: %(default)s)",
    )
    parser.add_argument("--source-nodes", default=source_nodes,
                        help="Source nodes file(default: %(default)s)")
    parser.add_argument("--trials", type=int, default=1,
                        help="Number of trials (default: %(default)s)")
    parser.add_argument("--num-sources", type=int, default=4,
                        help="Number of sources (default: %(default)s)")

    return parser


def GenerateGroundTruth(args):
    now = datetime.now(pytz.timezone("US/Central"))
    data = {
        "graph": type(args.graph),
        "threads": type(args.threads),
        "thread-spin": type(args.thread_spin),
        "source-nodes": type(args.source_nodes),
        "trials": type(args.trials),
        "num-sources": type(args.num_sources),
        "routines": type({}),
        "duration": type(0),
        "datetime": type(now.strftime("%d/%m/%Y %H:%M:%S")),
    }
    return data


def MainTest():
    pass


def GenerateTest():
    pass


if __name__ == "__main__":

    all_args = [
        {"json_output": "../../../bench_statistics.json",
         "input_dir": "../../inputs/v24/propertygraphs",
         "graph": "GAP-road",
         "app": "all",
         "source_nodes": "",
         "trails": 1,
         "num_sources": 4,
         "thread_spin": False,
         "threads": None},

        {"json_output": "../../../bench_statistics.json",
         "input_dir": "../../inputs/v24/propertygraphs",
         "graph": "GAP-road",
         "app": "all",
         "source_nodes": "",
         "trails": 10,
         "num_sources": 16,
         "thread_spin": True,
         "threads": 1},
    ]
    all_apps = ["tc", "cc", "kcore", "bfs", "sssp",
                "jaccard", "bc", "louvain", "pagerank", "all"]

    all_graphs = ["GAP-road", "rmat15", "GAP-kron",
                  "GAP-twitter", "GAP-web", "GAP-urand"]

    args = GenerateArgs(**all_args[0])
    ground_truth = GenerateGroundTruth(args)

    print(args)
    print(ground_truth)
