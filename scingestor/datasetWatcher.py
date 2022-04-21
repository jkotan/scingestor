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
import threading
import glob

from .scanWatcher import ScanWatcher
from .logger import logger


class DatasetWatcher(threading.Thread):
    """ Beamtime Watcher
    """

    def __init__(self, path, meta, delay=5):
        """ constructor

        :param delay: time delay
        :type delay: :obj:`str`
        """
        threading.Thread.__init__(self)
        self.__path = path
        self.__meta = meta
        self.beamtimeId = meta["beamtimeId"]
        self.delay = delay
        self.running = True
        self.ds_pattern = "scicat-datasets-{bt}.lst"
        self.ids_pattern = "scicat-ingested-datasets-{bt}.lst"

        self.scan_watchers = {}
        self.scan_lock = threading.Lock()

        self.datasets = self.ds_pattern.format(bt=self.beamtimeId)
        self.idatasets = self.ids_pattern.format(bt=self.beamtimeId)
        self.glds_pattern = os.path.join(
            self.__path, "**", self.datasets)

    def run(self):
        """ dataset watcher thread
        """
        try:
            files = glob.glob(self.glds_pattern, recursive=True)
            logger.info("Dataset files: %s" % files)
            for ffn in files:
                with self.scan_lock:
                    if ffn not in self.scan_watchers.keys():
                        ifn = ffn[:-(len(self.datasets))] + self.idatasets
                        self.scan_watchers[ffn] = ScanWatcher(
                            ffn, ifn, self.beamtimeId)
                        self.scan_watchers[ffn].start()
                        logger.info('Starting %s' % ffn)
                        # logger.info(str(btmd))
            while self.running:
                time.sleep(self.delay)
                logger.info('Dt Tac')
        finally:
            self.stop()

    def stop(self):
        """ stop the watcher
        """
        self.running = False
        for ffn, scw in self.scan_watchers.items():
            logger.info('Stopping %s' % ffn)
            scw.running = False
            scw.join()
