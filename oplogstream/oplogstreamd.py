#!/usr/bin/env python
import glob
import os
import sys
import locale

import logging
import logging.handlers
import daemon
import watcher
import ConfigParser
from handlers import QueueHandler

# manage configuration
try:
    path = os.path.realpath(__file__)
    path = os.path.dirname(os.path.dirname(path))

    config = ConfigParser.ConfigParser()

    if os.path.exists('/etc/oplogstream/conf.d/'):
        configPath = '/etc/oplogstream/conf.d/'
    elif os.path.exists('/etc/oplogstream/config.cfg'):
        configPath = '/etc/oplogstream/config.cfg'
    else:
        configPath = path + '/config.cfg'

    if not os.access(configPath, os.R_OK):
        print('Unable to read configuration file at %s' % configPath)
        sys.exit(1)

    if os.path.isdir(configPath):
        for conf in glob.glob(os.path.join(configPath, ".cfg")):
            config.read(conf)
    else:
        config.read(configPath)

except ConfigParser.Error:
    print("Configuration file not found or corrupted.")
    sys.exit(1)

try:
    logFile = config.get('Main', 'logFile')
except ConfigParser.NoOptionError:
    logFile = '/tmp/oplogstreamd.log'

try:
    pidFile = config.get('Main', 'pidFile')
except ConfigParser.NoOptionError:
    pidFile = '/tmp/oplogstreamd.pid'

# Set up logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

handler = logging.handlers.RotatingFileHandler(logFile)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)

class Oplogstreamd(daemon.Daemon):
    def run(self):
        handler = QueueHandler(**dict(config.items('RabbitMQ')))
        watcher.OplogWatcher(handler, **dict(config.items('MongoDB'))).start()


def main():
    # check access to pid file
    if os.access(pidFile, os.W_OK):
        print('Could not write pid file.')
        sys.exit(1)

    # define system pipes

    # instantiate custom daemon class
    d = Oplogstreamd(
        pidfile=pidFile, stdout='/tmp/oplogstream_stdout.txt', stderr='/tmp/oplogstream_stderr.txt', verbose=1
    )
    # figure out operation
    # do action

    def act(action):
         return {
             'start': d.start, 'stop': d.stop, 'restart': d.restart, 'run': d.run
         }[action]

    if len(sys.argv) > 1:
        act(sys.argv[1])
    else:
        print 'Unknown command. Use `oplogstreamd %s`' % '|' . join(actions.keys())


if __name__ == '__main__':
    main()

