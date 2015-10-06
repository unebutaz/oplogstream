#!/usr/bin/env python
import glob
import os
import sys
import locale
import logging
import logging.handlers
import daemon
import ConfigParser

from handlers import MultithreadedQueue, OpFilter
from watcher import OplogWatcher

CONFIG_MAIN_SECTION = 'Main'
CONFIG_FILTER_SECTION = 'Filter'
CONFIG_MONGO_SECTION = 'MongoDB'
CONFIG_RABBIT_SECTION = 'RabbitMQ'

DEFAULT_OUT_FILE = '/tmp/oplogstreamd.out'

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
        configPath = os.path.join(path, 'config.cfg')

    userConfig = os.path.join(os.getcwd(), 'config.cfg')
    if not os.access(configPath, os.R_OK) and not os.path.exists(userConfig):
        print('Could not find configuration file.')
        sys.exit(1)

    if os.path.isdir(configPath):
        for conf in glob.glob(os.path.join(configPath, ".cfg")):
            config.read(conf)
    else:
        config.read(configPath)

    config.read(userConfig)

except ConfigParser.Error as err:
    print(err.message)
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
        mongodb_options = dict(config.items(CONFIG_MONGO_SECTION))
        rabbitmq_options = dict(config.items(CONFIG_RABBIT_SECTION))

        threads = config.getint(CONFIG_MAIN_SECTION, 'threads') \
            if config.has_option(CONFIG_MAIN_SECTION, 'threads') else 1

        q_size = config.getint(CONFIG_MAIN_SECTION, 'q_size') \
            if config.has_option(CONFIG_MAIN_SECTION, 'q_size') else 32768

        op_handler = MultithreadedQueue(threads=threads, q_size=q_size, **rabbitmq_options)

        if config.has_section(CONFIG_FILTER_SECTION) and len(config.items(CONFIG_FILTER_SECTION)) > 0:
            filter_options = {}
            for k, v in config.items(CONFIG_FILTER_SECTION):
                filter_options[k] = map(lambda s: s.strip(), v.split(','))
            op_handler.add_filter(OpFilter(**filter_options))

        oplog_watcher = OplogWatcher(op_handler, **mongodb_options)
        oplog_watcher.start()


def main():
    locale.setlocale(locale.LC_ALL)
    # check access to pid file
    # define system pipes
    # instantiate custom daemon class
    daemon = Oplogstreamd(pidfile=pidFile, stdout=DEFAULT_OUT_FILE, stderr=DEFAULT_OUT_FILE, verbose=0)
    action_map = {
        'start': daemon.start,
        'stop': daemon.stop,
        'restart': daemon.restart,
        'run': daemon.run
    }

    if len(sys.argv) > 1 and sys.argv[1] in action_map:
        action_map[sys.argv[1]]()
    else:
        print 'Unknown command. Use `oplogstreamd %s`' % '|' . join(action_map.keys())


if __name__ == '__main__':
    main()
