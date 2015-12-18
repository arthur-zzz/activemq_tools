#!/usr/bin/python

__author__ = "Dan Small"
__version__ = "1.0.0"
__license__ = "GPL"

# This tool simply moves all messages in a queue to another queue.

import sys
import optparse
import json
import stompest.error
from stompest.config import StompConfig
from stompest.protocol import StompSpec
from stompest.sync import Stomp

cliargs = optparse.OptionParser()
cliargs.add_option('-b', '--broker', dest='broker', default='localhost', help='Hostname of ActiveMQ broker')
cliargs.add_option('-p', '--port', dest='port', default='61614', help='STOMP port on the broker')
cliargs.add_option('-f', '--from-queue', dest='from_queue', default='test.alpha',
                   help='Queue name to move messages out of')
cliargs.add_option('-t', '--to-queue', dest='to_queue', default='test.beta', help='Queue name to move message to')
cliargs.add_option('-c', '--close', dest='tmout', default=5,
                   help='Amount of time in seconds to wait for a message before exiting')

(options, args) = cliargs.parse_args()

broker = options.broker
port = options.port
from_queue = options.from_queue
to_queue = options.to_queue
tmout = int(options.tmout)
config = StompConfig('tcp://{0}:{1}'.format(broker, port))
client = Stomp(config)

try:
    client.connect()
except stompest.error.StompConnectTimeout:
    print('Unable to connect to {0}:{1}'.format(broker, port))
    client.disconnect()
    sys.exit(1)
else:
    print('STOMP connection established to {0}:{1}'.format(broker, port))

client.subscribe(from_queue, {StompSpec.ACK_HEADER: StompSpec.ACK_CLIENT_INDIVIDUAL})

while client.canRead(timeout=tmout):
    frame = client.receiveFrame()
    try:
        payload = json.loads(frame.body)
    except json.JSONDecoder:
        print("Failed to decode message body: frame.body was {}".format(frame.body))
        with open('/opt/apache-activemq/sendfail/malformed', 'a+') as mf:
            mf.write('{0}\n{1}'.format(frame.headers, frame.body))
            mf.close()
        continue
    headers = frame.headers
    print('Sending the following message to {} : '.format(to_queue))
    print('body: {}'.format(payload))
    print('headers: {}'.format(frame.headers))
    try:
        client.ack(frame)
    except stompest.error.StompConnectionError:
        print('Unable to send ACK to {}'.format(broker))
    try:
        headers['persistent'] = 'true'
        client.send(to_queue, body=json.dumps(payload), headers=headers)
    except stompest.error.StompError:
        print('Sending message to {} failed. Writing message locally at /opt/apache-activemq/sendfail'.format(broker))
        with open('/opt/apache-activemq/sendfail/{}\n'.format(from_queue), 'a+') as sf:
            sf.write(json.dumps(payload))

client.disconnect()
