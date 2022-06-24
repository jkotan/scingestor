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
#
#
import os
import time
import threading
import inotifyx

from .logger import get_logger


class DatasetWatcher(threading.Thread):
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
        # (:obj:`str`) beamtime id
        self.__bid = beamtimeId
        # (:obj:`float`) delay time for ingestion in s
        self.delay = delay
        # (:obj:`bool`) running loop flag
        self.running = True
        # (:obj:`list`<:obj:`str`>) ingested scan names
        self.sc_ingested = []
        # (:obj:`list`<:obj:`str`>) waiting scan names
        self.sc_waiting = []
        # (:obj:`int`) notifier ID
        self.notifier = None
        # (:obj:`dict` <:obj:`int`, :obj:`str`>) watch description paths
        self.wd_to_path = {}

        # (:obj:`float`) timeout value for inotifyx get events
        self.timeout = 1

    def _start_notifier(self, path):
        """ start notifier

        :param path: beamtime file subpath
        :type path: :obj:`str
        """
        self.notifier = inotifyx.init()
        self._add_path(path)

    def _add_path(self, path):
        """ add path to notifier

        :param path: beamtime file path
        :type path: :obj:`str`
        """
        try:
            watch_descriptor = inotifyx.add_watch(
                self.notifier, path,
                inotifyx.IN_ALL_EVENTS |
                inotifyx.IN_CLOSE_WRITE | inotifyx.IN_DELETE |
                inotifyx.IN_MOVE_SELF |
                inotifyx.IN_ALL_EVENTS |
                inotifyx.IN_MOVED_TO | inotifyx.IN_MOVED_FROM)
            self.wd_to_path[watch_descriptor] = path
            get_logger().info('DatasetWatcher: Adding watch: %s %s' % (
                self.__dsfile, self.__idsfile))
        except Exception as e:
            get_logger().warning('%s: %s' % (path, str(e)))

    def _stop_notifier(self):
        """ stop notifier
        """
        for wd in list(self.wd_to_path.keys()):
            try:
                inotifyx.rm_watch(self.notifier, wd)
            except Exception as e:
                get_logger().warning(
                    'ScanDirWatcher: %s' % str(e))

            path = self.wd_to_path.pop(wd, None)
            get_logger().info(
                'ScanDirWatcher: '
                'Removing watch %s: %s' % (str(wd), path))

    def run(self):
        """ scandir watcher thread
        """
        self._start_notifier(self.__dsfile)
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

        get_logger().info(
            'DatasetWatcher: Scans waiting: %s' % str(self.sc_waiting))
        get_logger().info(
            'DatasetWatcher: Scans ingested: %s' % str(self.sc_ingested))
        for scan in self.sc_waiting:

            get_logger().info(
                'DatasetWatcher: Ingesting: %s %s' % (
                            self.__dsfile, scan))
            self.sc_ingested.append(scan)
            with open(self.__idsfile, 'a+') as f:
                f.write("%s\n" % scan)

        try:
            while self.running:
                events = inotifyx.get_events(self.notifier, self.timeout)

                get_logger().debug('Sc Talk')

                self.sc_waiting = []
                for event in events:

                    if event.wd in self.wd_to_path.keys():
                        get_logger().debug(
                            'Ds: %s %s %s' % (event.name,
                                              event.get_mask_description(),
                                              self.wd_to_path[event.wd]))
                        masks = event.get_mask_description().split("|")
                        if "IN_CLOSE_WRITE" in masks:
                            if event.name:
                                ffn = os.path.join(
                                    self.wd_to_path[event.wd], event.name)
                            else:
                                ffn = self.wd_to_path[event.wd]
                            if ffn is not None and ffn == self.__dsfile:
                                get_logger().debug(
                                    'DatasetWatcher: Changed %s' % ffn)
                                with open(self.__dsfile, "r") as dsf:
                                    scans = [sc.strip()
                                             for sc in dsf.read().split("\n")
                                             if sc.strip()]
                                if os.path.isfile(self.__idsfile):
                                    with open(self.__idsfile, "r") as idsf:
                                        self.sc_ingested = [
                                            sc.strip()
                                            for sc in idsf.read().split("\n")
                                            if sc.strip()]
                                self.sc_waiting = [
                                    sc for sc in scans
                                    if sc not in self.sc_ingested]
                # time.sleep(self.delay)

                for scan in self.sc_waiting:

                    get_logger().info(
                        'DatasetWatcher: Ingesting: %s %s' % (
                            self.__dsfile, scan))
                    self.sc_ingested.append(scan)
                    with open(self.__idsfile, 'a+') as f:
                        f.write("%s\n" % scan)

        finally:
            self.stop()

    def stop(self):
        """ stop the watcher
        """
        self.running = False
        time.sleep(0.2)
        self._stop_notifier()
        # if os.path.isfile(self.__idsfile):
        #     with open(self.__idsfile, "r") as idsf:
        #         self.sc_ingested = [
        #             sc.strip()
        #             for sc in idsf.read().split("\n")
        #             if sc.strip()]
        #     for scan in self.sc_ingested:
        #         get_logger().info(
        #             'DatasetWatcher: Reingesting: %s %s ' % (
        #                 self.__idsfile, scan))
