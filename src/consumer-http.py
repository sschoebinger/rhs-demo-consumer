#!/usr/bin/env python
#
# Copyright (c) Robert Bosch GmbH. All rights reserved.
# Copyright (c) Microsoft Corporation. All rights reserved.
# Copyright 2016 Confluent Inc.
# Licensed under the MIT License.
# Licensed under the Apache License, Version 2.0
#
# Original Confluent sample modified for use with Azure Event Hubs for Apache Kafka Ecosystems
# Original Microsoft sample modified for use with Bosch RHS Service
#!/usr/bin/env python

import os
import sys
import json
import getopt
import logging
import datetime
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from functools import partial
from pprint import pformat

from azure.identity import DefaultAzureCredential
from confluent_kafka import Consumer, KafkaException

# ─────────────────────────────────────────────────────────────────────────────
# OAuth2 & Kafka Setup
# ─────────────────────────────────────────────────────────────────────────────

def oauth_cb(cred, namespace_fqdn, _config):
    token = cred.get_token(f"https://{namespace_fqdn}/.default")
    return token.token, token.expires_on

def stats_cb(stats_json_str):
    stats = json.loads(stats_json_str)
    print(f"\nKAFKA Stats: {pformat(stats)}\n")

def print_usage_and_exit(program_name):
    sys.stderr.write(f"Usage: {program_name} [options..] <eventhubs-namespace> <group> <topic1> <topic2> ..\n")
    sys.stderr.write("""
Options:
  -T <intvl>   Enable client statistics at specified interval (ms)
""")
    sys.exit(1)

# ─────────────────────────────────────────────────────────────────────────────
# HTTP Server Handler
# ─────────────────────────────────────────────────────────────────────────────

API_TOKEN = os.environ.get("API_TOKEN", "changeme")
PORT = os.environ.get("PORT", 8000)

class RequestHandler(BaseHTTPRequestHandler):
    def _set_response(self):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
    
    def _unauthorized(self):
        self.send_response(403)
        self.end_headers()
        self.wfile.write(b"Forbidden: Invalid or missing API token")

    def _check_token(self):
        # Option 1: Authorization header
        auth = self.headers.get("Authorization")
        if auth and auth.startswith("Bearer "):
            return auth.split(" ", 1)[1] == API_TOKEN

        # Option 2: Query parameter ?api_key=...
        from urllib.parse import urlparse, parse_qs
        query = parse_qs(urlparse(self.path).query)
        return query.get("api_key", [None])[0] == API_TOKEN

    def do_GET(self):
        if not self._check_token():
            return self._unauthorized()
        self._set_response()
        self.wfile.write(b'{"type":"FeatureCollection","features":[\n')
        for i, msg in enumerate(messages.values()):
            self.wfile.write(msg.decode("utf-8").encode("utf-8"))
            if i < len(messages) - 1:
                self.wfile.write(b",\n")
        self.wfile.write(b"\n]}")

# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # CLI arguments
    optlist, argv = getopt.getopt(sys.argv[1:], "T:")
    if len(argv) < 3:
        print_usage_and_exit(sys.argv[0])

    namespace, group, topics = argv[0], argv[1], argv[2:]

    # Azure authentication
    cred = DefaultAzureCredential()

    # Kafka config
    conf = {
        "bootstrap.servers": f"{namespace}:9093",
        "group.id": group,
        "session.timeout.ms": 6000,
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "OAUTHBEARER",
        "oauth_cb": partial(oauth_cb, cred, namespace)
    }

    # Statistics configuration
    for opt, val in optlist:
        if opt == "-T":
            try:
                intval = int(val)
                if intval <= 0:
                    raise ValueError
                conf["stats_cb"] = stats_cb
                conf["statistics.interval.ms"] = intval
            except ValueError:
                sys.stderr.write(f"Invalid interval value for -T: {val}\n")
                sys.exit(1)

    # Logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("consumer")

    # In-memory store for messages
    messages = {}

    # Start HTTP server
    server = HTTPServer(("", int(PORT)), RequestHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print(f"HTTP server running at http://localhost:{PORT}")

    # Kafka Consumer
    consumer = Consumer(conf, logger=logger)

    def on_assign(consumer, partitions):
        print("Assigned to partitions:", partitions)

    consumer.subscribe(topics, on_assign=on_assign)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            decoded = msg.value().decode("utf-8")
            try:
                parsed = json.loads(decoded)
                props = parsed.get("properties", {})
                msg_id = props.get("id", "unknown_key")
                expiry_time = props.get("expiryTime", 0)
                is_cancelled = props.get("isCancelled", False)

                if is_cancelled:
                    messages.pop(msg_id, None)
                else:
                    messages[msg_id] = msg.value()
            except json.JSONDecodeError as e:
                logging.error("JSON decode error: %s", e)

            # Clean expired messages
            now = datetime.datetime.now()
            for key, value in list(messages.items()):
                try:
                    parsed = json.loads(value.decode("utf-8"))
                    expiry = parsed.get("properties", {}).get("expiryTime", 0)
                    if expiry:
                        expiry_dt = datetime.datetime.fromtimestamp(expiry / 1000.0)
                        if expiry_dt < now:
                            logging.info("Removing expired message: %s", parsed)
                            messages.pop(key, None)
                except Exception as e:
                    logging.error("Failed to process expiry for key %s: %s", key, e)

    except KeyboardInterrupt:
        print("Aborted by user.")
    finally:
        consumer.close()