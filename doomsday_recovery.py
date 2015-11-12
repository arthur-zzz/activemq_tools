#!/usr/bin/env python

__author__ = "Dan Small"
__license__ = "GPL"
__version__ = "1.0.0"

# This tool was created for use in an environment where a JMS producer application would write its message to a local
# file in the event that it could not connect to a broker. Invoked manually or by cron, this script reads the contents
# of the doomsday directory, publishes the message to the broker, and writes a timestamped copy of the message to an
# archive message log file.

import optparse
import logging
import os
import time
import datetime
import json
from stompest.sync import Stomp
from stompest.config import StompConfig


class DoomsdayRecovery(object):

    def __init__(self, amqurl='localhost:61613', path='/var/activemq/doomsday/'):
        self.config = StompConfig(amqurl)
        self.path = path

    def run(self):
        files = os.listdir(self.path)
        for queue_file in files:
            archive_file = '{0}/archive/{1}'.format(self.path, queue_file)
            if queue_file.startswith('archive'):
                pass
            else:
                with open("{0}/{1}".format(self.path, queue_file), 'r') as qf:
                    get_lines = list(qf)
                    for line in get_lines:
                        dts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        client = Stomp(config=self.config)
                        client.connect()
                        print "Sending message {0} to queue {1}".format(line, queue_file)
                        with open(archive_file, 'a') as af:
                            af.write("{0} Sent message: {1} to queue {2}\n".format(dts, line, queue_file))
                        client.send(queue_file, json.dumps(line))
                        client.disconnect()

    def cleanup(self):
        files = os.listdir(self.path)
        for queue_file in files:
            qf = "{0}/{1}".format(self.path, queue_file)
            file_mod_time = os.stat(qf).st_mtime
            if (time.time() - file_mod_time) >= 2 and queue_file.startswith('archive') is False:
                os.remove(qf)
            elif queue_file.startswith('archive'):
                pass
            else:
                print """Queue file {0} modified since last message sent. Check for additional undelivered
                         messages or clean up the file manually.""".format(queue_file)
                pass

if __name__ == '__main__':
    cliargs = optparse.OptionParser()
    cliargs.add_option('-b', '--broker', dest='broker', help='Hostname of ActiveMQ broker (default: localhost')
    cliargs.add_option('-p', '--port', dest='port', help='Connector STOMP port (default: 61613)')

    (options, args) = cliargs.parse_args()

    broker = 'tcp://{0}:{1}'.format(options.broker, options.port)

    logging.basicConfig(level=logging.DEBUG)
    DoomsdayRecovery(amqurl=broker).run()
    time.sleep(3)
    DoomsdayRecovery().cleanup()
