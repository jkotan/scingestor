#   This file is part of scingestor - Scientific Catalog Dataset Ingestor
#
#    Copyright (C) 2021-2021 DESY, Jan Kotanski <jkotan@mail.desy.de>
#
#    nexdatas is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    nexdatas is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with scingestor.  If not, see <http://www.gnu.org/licenses/>.
#
import os
import time
import signal
import sys
import json
import threading

from .datasetWatcher import DatasetWatcher
from .logger import logger


class BeamtimeWatcher:
    """ Beamtime Watcher
    """

    def __init__(self, delay=5):
        """ constructor

        :param delay: time delay
        :type delay: :obj:`str`
        """
        self.delay = delay
        signal.signal(signal.SIGTERM, self._signal_handle)

        self.beamtime_dirs = [
            "/home/jkotan/gpfs/current",
            "/home/jkotan/gpfs/comissioning",
            "/home/jkotan/gpfs/local",
        ]

        self.bt_prefix = "beamtime-metadata-"
        self.bt_postfix = ".json"

        self.dataset_watchers = {}
        self.dataset_lock = threading.Lock()

    def find_bt_files(self, path, prefix, postfix):
        files = []
        try:
            if os.path.isdir(path):
                files = [fl for fl in os.listdir(path)
                         if (fl.startswith(prefix)
                             and fl.endswith(postfix))]
        except Exception as e:
            logger.warning(str(e))
        return files

    def start(self):
        """ start beamtime watcher
        """
        try:
            for path in self.beamtime_dirs:
                files = self.find_bt_files(
                    path, self.bt_prefix, self.bt_postfix)
                for bt in files:
                    ffn = os.path.abspath(os.path.join(path, bt))
                    with self.dataset_lock:
                        with open(ffn) as fl:
                            btmd = json.load(fl)
                            if ffn not in self.dataset_watchers.keys():
                                self.dataset_watchers[ffn] =  \
                                    DatasetWatcher(path, btmd)
                                self.dataset_watchers[ffn].start()
                                logger.info('Starting %s' % ffn)
                            # logger.info(str(btmd))
                logger.info('Files of %s: %s' % (path, files))
            while True:
                time.sleep(self.delay)
                logger.info('Bt Tick')
        except KeyboardInterrupt:
            logger.warning('Keyboard interrupt (SIGINT) received...')
            self.stop()

    def stop(self):
        """ stop beamtime watcher
        """
        logger.info('Cleaning up...')
        for ffn, dsw in self.dataset_watchers.items():
            logger.info('Stopping %s' % ffn)
            dsw.stop()
            dsw.join()
        sys.exit(0)

    def _signal_handle(self, sig, _):
        """ handle SIGTERM

        :param sig: signal name, i.e. 'SIGINT', 'SIGHUP', 'SIGALRM', 'SIGTERM'
        :type sig: :obj:`str`
        """
        logger.warning('SIGTERM received...')
        self.stop()


def main():
    """ main function
    """
    bw = BeamtimeWatcher()
    bw.start()
