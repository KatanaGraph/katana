#!/usr/bin/python

import sys
import os
import io
import queue
import multiprocessing
import threading
import fnmatch

# Parse command-line arguments
if len(sys.argv) == 1:
    print('Usage: check_cpp_format.py [-fix] <paths>')
    sys.exit()
i_arg = 1
fix = False
if sys.argv[i_arg] == '-fix':
    fix = True
    i_arg += 1
roots = []
for root in sys.argv[i_arg:]:
    roots.append(root)

# Parse environment variables
prune_paths = set()
if 'PRUNE_PATHS' in os.environ:
    last_token = io.StringIO()
    in_quotes = False
    for ch in os.environ['PRUNE_PATHS']:
        if ch in ["'", '"']:
            if last_token.tell() != 0:
                prune_paths.add(last_token.getvalue())
                last_token = io.StringIO()
            in_quotes = not in_quotes
        elif ch == ' ':
            if in_quotes:
                last_token.write(ch)
            elif last_token.tell() != 0:
                prune_paths.add(last_token.getvalue())
                last_token = io.StringIO()
            # else tokens are separated by multiple spaces
        else:
            last_token.write(ch)
    if last_token.tell() != 0:
        prune_paths.add(last_token.getvalue())
        last_token = io.StringIO()
clang_format = os.environ.get('CLANG_FORMAT', 'clang-format')

# Hard-coded constants
prune_names = {'build*', '.#*'}
file_suffixes = {'.cpp', '.h', '.cu', '.cuh'}


# Create the task queue
tasks = queue.SimpleQueue()
check_failed = False

# Launch consumer threads
def consumer_entry():
    global check_failed
    while True:
        item = tasks.get()
        if item is None:
            tasks.put(None)
            return
        if fix:
            print('fixing ', item)
            cmd = clang_format + ' -style=file -i ' + item
        else:
            cmd = '{} -style=file -output-replacements-xml "{}" | grep \'<replacement \' > /dev/null'.format(clang_format, item)
        exit_code = os.system(cmd)
        if not fix and exit_code == 0:
            check_failed = True
            print(item, ' NOT OK')

threads = []
for i in range(multiprocessing.cpu_count()):
    cur_thread = threading.Thread(target=consumer_entry)
    threads.append(cur_thread)
    cur_thread.start()

# Traverse the directory trees
def needs_formatting(file_name: str) -> bool:
    for suffix in file_suffixes:
        if file_name.endswith(suffix):
            return True
    return False

for root in roots:
    for dir_path, dir_names, file_names in os.walk(root):
        to_remove = []
        for dir_name in dir_names:
            removing = False
            for prune_name in prune_names:
                if fnmatch.fnmatch(dir_name, prune_name):
                    to_remove.append(dir_name)
                    removing = True
                    break
            if removing:
                continue
            full_path = os.path.join(dir_path, dir_name)
            for prune_path in prune_paths:
                if fnmatch.fnmatch(full_path, prune_path):
                    to_remove.append(dir_name)
                    break
        for dir_name in to_remove:
            dir_names.remove(dir_name)
        for file_name in file_names:
            pruned = False
            for prune_name in prune_names:
                if fnmatch.fnmatch(file_name, prune_name):
                    pruned = True
                    break
            if pruned or not needs_formatting(file_name):
                continue
            tasks.put(os.path.join(dir_path, file_name))

# Finalize the processing
tasks.put(None)
for thread in threads:
    thread.join()

# Return proper exit code
sys.exit(1 if check_failed else 0)
