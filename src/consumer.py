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
 
import datetime
from azure.identity import DefaultAzureCredential
from confluent_kafka import Consumer, KafkaException
import sys
import getopt
import json
import logging
from functools import partial
from pprint import pformat
 
 
def stats_cb(stats_json_str):
    stats_json = json.loads(stats_json_str)
    print("\nKAFKA Stats: {}\n".format(pformat(stats_json)))
 
 
def oauth_cb(cred, namespace_fqdn, config):
 
    access_token = cred.get_token("https://%s/.default" % namespace_fqdn)
    return access_token.token, access_token.expires_on
 
 
def print_usage_and_exit(program_name):
    sys.stderr.write(
        "Usage: %s [options..] <eventhubs-namespace> <group> <topic1> <topic2> ..\n"
        % program_name
    )
    options = """
 Options:
  -T <intvl>   Enable client statistics at specified interval (ms)
"""
    sys.stderr.write(options)
    sys.exit(1)
 
 
if __name__ == "__main__":
    optlist, argv = getopt.getopt(sys.argv[1:], "T:")
    if len(argv) < 3:
        print_usage_and_exit(sys.argv[0])
 
    namespace = argv[0]
    group = argv[1]
    topics = argv[2:]
 
    # Azure credential
    # See https://docs.microsoft.com/en-us/azure/developer/python/sdk/authentication-overview
    cred = DefaultAzureCredential()
 
    # Consumer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    conf = {
        "bootstrap.servers": "%s:9093" % namespace,
        "group.id": group,
        "session.timeout.ms": 6000,
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
        # Required OAuth2 configuration properties
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "OAUTHBEARER",
        # the resulting oauth_cb must accept a single `config` parameter, so we use partial to bind the namespace/identity to our function
        "oauth_cb": partial(oauth_cb, cred, namespace),
    }
 
    # Check to see if -T option exists
    for opt in optlist:
        if opt[0] != "-T":
            continue
        try:
            intval = int(opt[1])
        except ValueError:
            sys.stderr.write("Invalid option value for -T: %s\n" % opt[1])
            sys.exit(1)
 
        if intval <= 0:
            sys.stderr.write(
                "-T option value needs to be larger than zero: %s\n" % opt[1]
            )
            sys.exit(1)
 
        conf["stats_cb"] = stats_cb
        conf["statistics.interval.ms"] = int(opt[1])
 
    # Create logger for consumer (logs will be emitted when poll() is called)
    logger = logging.getLogger("consumer")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)-15s %(levelname)-8s %(message)s")
    )
    logger.addHandler(handler)

    messages = []
    # key value store for storing messages
    messages = {}  # Use a dictionary if you want to store messages with keys
    # get all values from the messages

    logging.basicConfig(level=logging.INFO)
    # start http server to serve the messages
    from http.server import BaseHTTPRequestHandler, HTTPServer
    PORT = 8000
    class Handler(BaseHTTPRequestHandler):
        def _set_response(self):
            self.send_response(200)
            #self.send_header('Content-type', 'json/application')
            self.send_header('Content-type', 'text/html')
            self.end_headers()

        def do_GET(self):
            # logging.info("GET request,\nPath: %s\nHeaders:\n%s\n", str(self.path), str(self.headers))
            self._set_response()
            self.wfile.write("{\"type\":\"FeatureCollection\",\"features\":[\n".encode('utf-8'))
            for i, message in enumerate(messages.values()):
                self.wfile.write((message.decode("utf-8") + (",\n" if i < len(messages) - 1 else "\n")).encode('utf-8'))
            self.wfile.write("]}".encode('utf-8'))
            # log content


        def do_POST(self):
            content_length = int(self.headers['Content-Length']) # <--- Gets the size of data
            post_data = self.rfile.read(content_length) # <--- Gets the data itself
            logging.info("POST request,\nPath: %s\nHeaders:\n%s\n\nBody:\n%s\n",
                    str(self.path), str(self.headers), post_data.decode('utf-8'))

            self._set_response()
            self.wfile.write("POST request for {}".format(self.path).encode('utf-8'))
    httpd = HTTPServer(("", PORT), Handler)
    print(f"Serving at port {PORT}")

    import threading
    server_thread = threading.Thread(target=httpd.serve_forever)
    server_thread.daemon = True  # This will allow the program to exit even if the server is running
    server_thread.start()

    # Set the last write time to now
    last_write = datetime.datetime.now()
 
    # Create Consumer instance
    # Hint: try debug='fetch' to generate some log messages
    c = Consumer(conf, logger=logger)
 
    def print_assignment(consumer, partitions):
        print("Assignment:", partitions)
 
    # Subscribe to topics
    c.subscribe(topics, on_assign=print_assignment)
 
    # Read messages from Kafka, print to stdout
    try:
        while True:
            msg = c.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                # Proper message
                sys.stderr.write(
                    "%% %s [%d] at offset %d with key %s:\n"
                    % (msg.topic(), msg.partition(), msg.offset(), str(msg.key()))
                )
                # show the message in the console
                # print(msg.value())
                #if messages has the same key, update the value
            
                json_message = json.loads(msg.value().decode("utf-8"))
                # get properties from the message
                # print("Message properties:", json_message.get("properties", {}))
                # extract the expiryTime from the properties
                expiryTime = json_message.get("properties", {}).get("expiryTime", 0)
                isCancelled = json_message.get("properties", {}).get("isCancelled", False)
                key = json_message.get("properties", {}).get("id", "unknown_key")
                if isCancelled:
                    if key in messages:
                        messages.pop(key, None)
                else:
                    messages[key] = msg.value()
            # loop over all messages dict
            for key, value in list(messages.items()):
                # parse the message as json
                try:
                    json_message = json.loads(value.decode("utf-8"))
                    # get properties from the message
                    # print("Message properties:", json_message.get("properties", {}))
                    # extract the expiryTime from the properties
                    expiryTime = json_message.get("properties", {}).get("expiryTime", 0)
                    # print(expiryTime)
                    # print(datetime.datetime.fromtimestamp(expiryTime / 1000.0))
                    # if expiryTime is in the past remove the message from the list
                    if expiryTime > 0:
                        expiryTime = datetime.datetime.fromtimestamp(expiryTime / 1000.0)
                        if expiryTime < datetime.datetime.now():
                            print(expiryTime, "is in the past, removing message")
                            logging.info("Removing expired message: %s", json_message)
                            messages.pop(key, None)
                    #print(json_message.get("properties.expiryTime", "No expiryTime found"))
                except json.JSONDecodeError as e:
                    logging.error("Failed to decode message as JSON: %s", e)

            # check if we need to write to file
            # if last_write + datetime.timedelta(minutes=10) < datetime.datetime.now():
            #     last_write = datetime.datetime.now()
            #     # skip write if the message list is empty
            #     if len(messages) == 0:
            #         continue
            #     # write the messages to a json file
            #     with open(datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")+".json", "a") as f:
            #         f.write("{\"type\":\"FeatureCollection\",\"features\":[\n")
            #         for i, message in enumerate(messages):
            #             f.write(message.decode("utf-8") + (",\n" if i < len(messages) - 1 else "\n"))
            #         f.write("]}")
            #     messages = []
    except KeyboardInterrupt:
        sys.stderr.write("%% Aborted by user\n")

    finally:
        # Close down consumer to commit final offsets.
        c.close()
    