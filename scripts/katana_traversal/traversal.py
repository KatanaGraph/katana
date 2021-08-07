from abc import ABC, abstractmethod
import fnmatch
import io
import multiprocessing
import os
import queue
import subprocess
import sys
import threading
import typing


class FileVisitor(ABC):
    def __init__(self, consumer):
        self.consumer = consumer

    # Returns False if the check has failed for this file
    @abstractmethod
    def visit(self, file_path: str) -> bool:
        pass

    @abstractmethod
    def needs_visiting(self, file_name: str) -> bool:
        pass


class Pivot:
    def __init__(self, file_visitor: FileVisitor):
        self.file_visitor = file_visitor
        self.tasks = queue.Queue


class Consumer:
    def __init__(self, pivot: Pivot):
        self.pivot = pivot
        self.thread = threading.Thread(target=self.entry)
        self.check_failed = False

    def entry(self):
        while True:
            item = self.pivot.tasks.get()
            if item is None:
                self.pivot.tasks.put(None)
                return
            if not self.pivot.file_visitor.visit(item):
                self.check_failed = True


class Launcher:
    def __init__(self, pivot: Pivot):
        self.pivot = pivot

    # Returns False if some check has failed, i.e. answers the question whether everything is correct
    def run(self) -> bool:
        consumers = []
        for i in range(multiprocessing.cpu_count()):
            cur_cons = Consumer(self.pivot)
            consumers.append(cur_cons)
            cur_cons.thread.start()

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
                    if pruned or not self.pivot.file_visitor.needs_visiting(file_name):
                        continue
                    self.pivot.tasks.put(os.path.join(dir_path, file_name))

        # Finalize the processing
        self.pivot.tasks.put(None)
        check_failed = False
        for consumer in consumers:
            consumer.thread.join()
            check_failed = check_failed or consumer.check_failed

        return not check_failed
