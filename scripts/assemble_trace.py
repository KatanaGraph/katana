#!/usr/bin/env python3

import argparse
import fileinput
import json


class Reference:
    def __init__(self, ref_type, trace_id, span_id):
        self.ref_type = ref_type
        self.trace_id = trace_id
        self.span_id = span_id

    def get_json(self):
        ref = dict()
        ref["refType"] = self.ref_type
        ref["traceID"] = self.trace_id
        ref["spanID"] = self.span_id

        return ref


class Tag:
    def __init__(self, tag, value="", tag_type=""):
        if isinstance(tag, str):
            self.key = tag
            self.tag_type = tag_type
            self.value = value
        else:
            self.key = tag["name"]
            self.tag_type = "string"
            self.value = tag["value"]
            if isinstance(self.value, bool):
                self.tag_type = "bool"
            elif isinstance(self.value, int):
                self.tag_type = "int64"
            elif isinstance(self.value, float):
                self.tag_type = "float64"

    def get_json(self):
        tag = dict()
        tag["key"] = self.key
        tag["type"] = self.tag_type
        tag["value"] = self.value

        return tag


class Log:
    def __init__(self, log, tag_fields):
        fields = []
        fields.append(Tag("message", log["msg"], "string"))
        for field in tag_fields:
            fields.append(Tag(field))
        fields.append(Tag("max_mem_gb", log["max_mem_gb"], "float64"))
        fields.append(Tag("mem_gb", log["mem_gb"], "float64"))
        fields.append(Tag("arrow_mem_gb", log["arrow_mem_gb"], "float64"))

        self.timestamp = log["timestamp_us"]
        self.fields = fields

    def correct_timestamp(self, start_timestamp):
        self.timestamp += start_timestamp

    def get_json(self):
        log = dict()
        log["timestamp"] = self.timestamp

        fields = []
        for field in self.fields:
            fields.append(field.get_json())
        log["fields"] = fields

        return log


class Span:
    # pylint: disable=too-many-instance-attributes
    def __init__(self, trace_id, span_id, host_id):
        self.trace_id = trace_id
        self.span_id = span_id
        self.host_id = host_id
        self.name = ""
        self.start = -1
        self.finish = -1
        self.refs = []
        self.tags = []
        self.logs = []

    def update_span_data(self, span_data, timestamp):
        if "span_name" in span_data:
            self.name = span_data["span_name"]
            self.start = timestamp
            parent = span_data["parent_id"]
            if parent != "null":
                self.refs.append(Reference("CHILD_OF", self.trace_id, parent))
        if "finished" in span_data:
            self.finish = timestamp

    def add_tags(self, tags):
        for tag in tags:
            self.tags.append(Tag(tag))

    def add_log(self, log, tags):
        self.logs.append(Log(log, tags))

    def correct_timestamps(self, start_timestamp):
        self.start += start_timestamp
        self.finish += start_timestamp
        for log in self.logs:
            log.correct_timestamp(start_timestamp)

    def get_json(self):
        span = dict()
        span["traceID"] = self.trace_id
        span["spanID"] = self.span_id
        span["flags"] = 1
        span["operationName"] = self.name

        refs = []
        for ref in self.refs:
            refs.append(ref.get_json())
        span["references"] = refs

        span["startTime"] = self.start
        span["duration"] = self.finish - self.start

        tags = []
        for tag in self.tags:
            tags.append(tag.get_json())
        span["tags"] = tags

        logs = []
        for log in self.logs:
            logs.append(log.get_json())
        span["logs"] = logs

        span["processID"] = str(self.host_id)
        span["warnings"] = None

        return span


class Process:
    def __init__(self, name):
        self.name = name
        self.tags = []

    def add_tag(self, tag):
        self.tags.append(tag)

    def get_json(self):
        process = dict()
        process["serviceName"] = "katana"

        tags = []
        for tag in self.tags:
            tags.append(tag.get_json())
        process["tags"] = tags

        return process


class Trace:
    def __init__(self, trace_id):
        self.trace_id = trace_id
        self.spans = dict()
        self.processes = dict()

    def update_trace_data(self, host_id, trace_data):
        if str(host_id) not in self.processes:
            process = Process(str(host_id))
            process.add_tag(Tag("host_id", host_id, "int64"))
            process.add_tag(Tag("hardware_threads", trace_data["hardware_threads"], "int64"))
            process.add_tag(Tag("ram_gb", trace_data["ram_gb"], "int64"))
            process.add_tag(Tag("hostname", trace_data["hostname"], "string"))
            process.add_tag(Tag("num_hosts", trace_data["hosts"], "int64"))
            self.processes[str(host_id)] = process

    def get_json(self):
        trace = dict()
        trace["traceID"] = self.trace_id

        spans = []
        for _, span in self.spans.items():
            spans.append(span.get_json())
        trace["spans"] = spans

        processes = dict()
        for process_id, process in self.processes.items():
            processes[process_id] = process.get_json()
        trace["processes"] = processes

        trace["warnings"] = None

        return trace


def get_traces_json(traces):
    traces_json = dict()

    trace_jsons = []
    for _, trace in traces.items():
        trace_jsons.append(trace.get_json())
    traces_json["data"] = trace_jsons

    traces_json["total"] = 0
    traces_json["limit"] = 0
    traces_json["offset"] = 0
    traces_json["errors"] = None

    return traces_json


def assemble_traces(args):
    traces = dict()

    stream = len(args.log) == 0
    for line in fileinput.input(args.log[0] if not stream else ("-",)):
        try:
            json_line = json.loads(line.strip())
            trace_id = json_line["trace_id"]
            if trace_id not in traces.keys():
                traces[trace_id] = Trace(trace_id)
            trace = traces[trace_id]

            if "host_data" in json_line:
                trace.update_trace_data(json_line["host"], json_line["host_data"])

            span_id = json_line["span_data"]["span_id"]
            if span_id not in trace.spans.keys():
                trace.spans[span_id] = Span(trace_id, span_id, json_line["host"])
            span = trace.spans[span_id]

            if "log" in json_line:
                span.update_span_data(json_line["span_data"], json_line["log"]["timestamp_us"])
                tags = []
                if "tags" in json_line:
                    tags = json_line["tags"]
                span.add_log(json_line["log"], tags)
            elif "tags" in json_line:
                span.add_tags(json_line["tags"])
        except ValueError:
            pass
    return traces


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("log", metavar="FILE", nargs="*", help="Log to read, if none provided reads from stdin")
    parser.add_argument("--outfile", metavar="FILE", help="File to write assembled trace to")
    args = parser.parse_args()

    traces = assemble_traces(args)
    traces_json = get_traces_json(traces)

    with open(args.outfile, "w") as outfile:
        outfile.write(json.dumps(traces_json, indent=2))
