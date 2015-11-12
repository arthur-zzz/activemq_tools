#!/usr/bin/env python
# TODO: account for fewer messages than specified in arg so script doesn't hang open waiting for messages

__author__ = "Dan Small"
__version__ = "1.0.0"
__license__ = "GPL"

# This tool simply performs a forced move of a designated number of messages from one queue to another.

import sys
import optparse
from stompest.config import StompConfig
from stompest.protocol import StompSpec
from stompest.sync import Stomp

cliargs = optparse.OptionParser()
cliargs.add_option('-b', '--broker', dest='broker', help='Hostname of ActiveMQ broker')
cliargs.add_option('-f', '--from-queue', dest='from_queue', help='Queue name to move messages out of')
cliargs.add_option('-t', '--to-queue', dest='to_queue', help='Queue name to move message to')
cliargs.add_option('-n', '--num-messages', dest='num_messages', help='Number of messages to move')

(options, args) = cliargs.parse_args()

broker = options.broker
from_queue = options.from_queue
to_queue = options.to_queue
num_messages = options.num_messages

config = StompConfig('tcp://{0}:61613'.format(broker))
client = Stomp(config)
client.connect()
client.subscribe(from_queue, {StompSpec.ACK_HEADER: StompSpec.ACK_CLIENT_INDIVIDUAL})

for i in range(num_messages):
    frame = client.receiveFrame()
    print 'body: {0}'.format(frame.body)
    print 'headers: {0}'.format(frame.headers)
    print 'Sending message to {0}'.format(to_queue)
    client.ack(frame)
    client.send(to_queue, body=frame.body, headers=frame.headers)

client.disconnect()
sys.exit()
