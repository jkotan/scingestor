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

from .logger import logger


class ScanWatcher(threading.Thread):
    """ Beamtime Watcher
    """

    def __init__(self, dsfile, idsfile, beamtimeId, delay=5):
        """ constructor

        :param delay: time delay
        :type delay: :obj:`str`
        """
        threading.Thread.__init__(self)
        self.__dsfile = dsfile
        self.__idsfile = idsfile
        self.__bid = beamtimeId
        self.delay = delay
        self.running = True
        self.sc_ingested = []
        self.sc_waiting = []

    def run(self):
        """ dataset watcher thread
        """
        with open(self.__dsfile, "r") as dsf:
            scans = [sc.strip() for sc in dsf.read().split("\n")
                     if sc.strip()]
        if os.path.isfile(self.__idsfile):
            with open(self.__idsfile, "r") as idsf:
                self.sc_ingested = [
                    sc.strip() for sc in idsf.read().split("\n")
                    if sc.strip()]
        self.sc_waiting = [sc for sc in scans
                           if sc not in self.sc_ingested]

        logger.info('Scans waiting: %s' % str(self.sc_waiting))
        logger.info('Scans ingested: %s' % str(self.sc_ingested))
        while self.running:
            time.sleep(self.delay)
            logger.info('Sc Tick')
