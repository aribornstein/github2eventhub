#!/usr/bin/python3

# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for full license information.

import requests
import json
import os
import sys
import time
import datetime
import traceback
import argparse
from azure.eventhub import EventHubClient, Sender, EventData, EventHubError

def log(m):
    print("{} | {}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3], m)) # replace this with logging 

def debug(message):
    # log("{} | {}".format("DEBUG", message))
    pass # replace this with logging

def info(message):
    log("{} | {}".format("INFO", message)) # replace this with logging

def error(message, err):
    log("{} | {} | {}".format("ERROR", message, err)) # replace this with logging

class Monitor:
    interval = 60

    def __init__(self, report_cb=None):
        self.lut = time.time()
        self.events_sent = 0
        self.requests_issued = 0
        self.report_cb = report_cb or print

    def reset(self):
        self.lut = time.time()
        self.events_sent = 0
        self.requests_issued = 0

    def report(self):
        now = time.time()
        if now - self.lut >= 60:
            self._report()
            self.reset()

    def _report(self):
        msg = "Monitor: events sent : {} calls made :{}".format(self.events_sent, self.requests_issued)
        self.report_cb(msg) # bad practice replace this with logging 


class SlidingCache:
    def __init__(self, max_size=500):
        self.prev = set()
        self.current = set()
        self.max_size = max_size

    def add(self, item):
        if item not in self:
            if len(self.current) > self.max_size:
                self.prev = self.current
                self.current = set()
            self.current.add(item)

    def __contains__(self, item):
        return item in self.current or item in self.prev


class BufferedEventHubSender:
    # eventhub max size is 262144, just to be on the safe side, reduce this to 250000
    def __init__(self, sender, flush_cb, serializer=json.dumps, item_seperator="\n", max_size=250000):
        self.max_size = max_size
        self.buffer = ""
        self.item_count = 0
        self.sender = sender
        self.flush_cb = flush_cb
        self.item_seperator = item_seperator
        self.serializer = serializer

    def push(self, item):
        _item = "{}{}".format(self.serializer(item), self.item_seperator)
        if len(_item.encode("utf-8")) > self.max_size:
            raise EventHubError(
                "Item {} is to big ({}) where as limit is {}. Ignoring.".format(
                    _item, len(_item.encode("utf-8")), self.max_size
                )
            )

        if len((self.buffer + _item).encode("utf-8")) > self.max_size:
            self.flush()

        self.buffer += _item
        self.item_count += 1

    def flush(self):
        try:
            start = time.time()
            self.sender.send(EventData(self.buffer))
            self.flush_cb(time.time() - start, self.item_count)
        except Exception as e:
            raise EventHubError("lost the following {} records:\n {}".format(self.item_count, self.buffer))

        self.buffer = ""
        self.item_count = 0


def run(eh_address, eh_user, eh_key, gh_endpoint, gh_token):
    info("STARTED github to eventhub")

    headers = {"Authorization": "token {}".format(gh_token)}
    monitor = Monitor(info)

    eh_client = EventHubClient(eh_address, debug=False, username=eh_user, password=eh_key)

    sender = None
    try:
        sender = BufferedEventHubSender(
            eh_client.add_sender(),
            flush_cb=lambda took, count: debug(
                "EVENTHUB REQUEST | took: {} sec, sent {} records to {}.".format(took, count, eh_client.eh_name)
            ),
        )
        failed = eh_client.run()
        if failed:
            raise EventHubError("Couldn't connect to EH {}".format(eh_client.eh_name))
    except EventHubError as e:
        error("EventHubError", e.message)

    seconds_per_request = round(
        1.0 / (5000 / 60 / 60), 2
    )  # requests / minutes / seconds = requests per sec, ^-1=secs per request

    cache = SlidingCache()
    events = 0

    loop = True
    while loop:
        loop_start_time = time.time()
        monitor.report()

        try:
            resp = requests.get(gh_endpoint, headers=headers)
            resp.raise_for_status()

            monitor.requests_issued += 1
            data = sorted(resp.json(), key=lambda x: x["id"])
            payload = ""

            debug("GITHUB REQUEST | took {} sec, got {} events.".format(resp.elapsed.total_seconds(), len(data)))

            for d in data:
                if d["id"] not in cache:
                    try:
                        sender.push(d)
                    except EventHubError as e:
                        error("EventHubError", e.message)

                    monitor.events_sent += 1
                    cache.add(d.get("id"))

            cycle_took = time.time() - loop_start_time
            delay = seconds_per_request - cycle_took
            debug("CYCLE DONE | took {}, waiting for {}".format(cycle_took, max(delay, 0)))
            if delay > 0:
                time.sleep(delay)

        except requests.HTTPError as e:
            if resp.status_code in [429, 403]:
                time_to_wait = int(
                    float(resp.headers.get("X-RateLimit-Reset", 60)) - datetime.datetime.utcnow().timestamp()
                )
                info("waiting for {}".format(time_to_wait))
                if time_to_wait > 0:
                    time.sleep(time_to_wait)
            error("HTTP EXCEPTION", repr(e))
        except EventHubError as e:
            error("Failed to send events to eventhub, skipping", repr(e))
        except Exception as e:
            error("UNEXPECTED ERROR", repr(e))
            traceback.print_exc()

    os.kill(os.getpid(), 9)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    address_help = "'amqps://<mynamespace>.servicebus.windows.net/myeventhub'"
    parser.add_argument('--eh_address', help=address_help)
    parser.add_argument('--eh_user')
    parser.add_argument('--eh_key')
    parser.add_argument('--gh_endpoint')
    parser.add_argument('--gh_token')

    args = parser.parse_args()

    run(args.eh_address, args.eh_user, args.eh_key, args.gh_endpoint, args.gh_token)