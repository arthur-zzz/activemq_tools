#!/usr/bin/env python

import optparse
import json
import logging
from twisted.internet import reactor, defer
from stompest.async import Stomp
from stompest.config import StompConfig
from stompest.async.listener import SubscriptionListener
from stompest.protocol import StompSpec


class IncrementTransformer(object):

    def __init__(self, amq_url='tcp://localhost:61613', in_queue='/queue/dlq.test', out_queue='/queue/test',
                 error_queue='/queue/error.test'):
        self.config = StompConfig(amq_url)
        self.IN_QUEUE = in_queue
        self.OUT_QUEUE = out_queue
        self.ERROR_QUEUE = error_queue

    @defer.inlineCallbacks
    def run(self):
        client = yield Stomp(self.config).connect()
        headers = {
            # client-individual mode is necessary for concurrent processing
            StompSpec.ACK_HEADER: StompSpec.ACK_CLIENT_INDIVIDUAL,
            # the max number of messages the broker will let you work on at the same time
            'activemq.prefetchSize': '100',
        }
        client.subscribe(
            self.IN_QUEUE, headers, listener=SubscriptionListener(self.addone, errorDestination=self.ERROR_QUEUE)
        )

    def addone(self, client, frame):
        data = json.loads(frame.body)
        headers = frame.headers
        if 'retry_count' in headers:
            if int(headers['retry_count']) < 10:
                retry_count = str(int(headers['retry_count']) + 1)
                print "Delivery attempt {0} for message: {1}".format(headers['retry_count'], headers['message-id'])
                client.send(self.OUT_QUEUE, json.dumps(data), headers={'retry_count': retry_count})
            elif int(headers['retry_count']) >= 10:
                print "Max delivery attempts exceeded. Sending {0} to error queue.".format(headers['message-id'])
                client.send(self.ERROR_QUEUE, json.dumps(data), headers={'retry_count': 11})
        elif 'retry_count' not in headers:
            client.send(self.OUT_QUEUE, json.dumps(data), headers={'retry_count': 1})
        else:
            client.send(self.ERROR_QUEUE, json.dumps(data), headers={'delivery_error': "too many retries"})

if __name__ == '__main__':
    cliargs = optparse.OptionParser()
    cliargs.add_option('-b', '--broker', dest='broker', help='Hostname of ActiveMQ broker (default = localhost')
    cliargs.add_option('-p', '--port', dest='port', help='STOMP port number on broker (default = 61613)')
    cliargs.add_option('-f', '--frm-queue', dest='frm_queue', help='Name of queue to monitor')
    cliargs.add_option('-t', '--to-queue', dest='to_queue', help='Name of destination queue')
    (options, args) = cliargs.parse_args()

    BROKER = '{0}{1}{2}'.format(options.broker, ":", options.port)
    IN_QUEUE = '/queue/{0}'.format(options.frm_queue)
    OUT_QUEUE = '/queue/{0}'.format(options.to_queue)
    ERROR_QUEUE = '/queue/error.{0}'.format(options.to_queue)

    logging.basicConfig(level=logging.DEBUG)
    IncrementTransformer(amq_url='tcp://{0}'.format(BROKER), in_queue=IN_QUEUE, out_queue=OUT_QUEUE,
                         error_queue=ERROR_QUEUE).run()
    reactor.run()
