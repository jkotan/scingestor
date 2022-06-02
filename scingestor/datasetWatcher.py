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
from .logger import get_logger

import inotifyx


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

        self.notifier = None
        self.wd_to_path = {}
        self.wd_to_bpath = {}

        self.scan_watchers = {}
        self.scan_lock = threading.Lock()
        self.timeout = 1

        self.datasets = self.ds_pattern.format(bt=self.beamtimeId)
        self.idatasets = self.ids_pattern.format(bt=self.beamtimeId)
        self.glds_pattern = os.path.join(
            self.__path, "**", self.datasets)

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
            get_logger().info('DatasetWatcher: Starting Dataset %s: %s'
                              % (str(watch_descriptor), path))
        except Exception as e:
            get_logger().warning('%s: %s' % (path, str(e)))
            # self._add_base_path(path)

    def _add_base_path(self, path):
        """ add base path to notifier

        :param path: base file path
        :type path: :obj:`str`
        """
        failing = True
        bpath = path
        while failing:
            try:
                bpath, _ = os.path.split(bpath)

                if not bpath:
                    bpath = os.path.abspath()
                watch_descriptor = inotifyx.add_watch(
                    self.notifier, bpath,
                    inotifyx.IN_CREATE | inotifyx.IN_CLOSE_WRITE
                    | inotifyx.IN_MOVED_TO
                    | inotifyx.IN_MOVE_SELF
                    | inotifyx.IN_DELETE
                    | inotifyx.IN_ALL_EVENTS
                )
                failing = False
                self.wd_to_bpath[watch_descriptor] = bpath
                get_logger().info('DatasetWatcher: Starting base %s: %s'
                                  % (str(watch_descriptor), bpath))
                self.wait_for_dirs[bpath] = path
            except Exception as e:
                get_logger().warning('%s: %s' % (bpath, str(e)))
                if bpath == '/':
                    failing = False

    def _stop_notifier(self):
        """ start notifier
        """
        for wd in list(self.wd_to_path.keys()):
            try:
                inotifyx.rm_watch(self.notifier, wd)
            except Exception as e:
                get_logger().warning(
                    'DatasetWatcher: %s' % str(e))

            path = self.wd_to_path.pop(wd, None)
            get_logger().info(
                'DatasetWatcher: '
                'Stopping notifier %s: %s' % (str(wd), path))
        for wd in list(self.wd_to_bpath.keys()):
            try:
                inotifyx.rm_watch(self.notifier, wd)
            except Exception as e:
                get_logger().warning(
                    'DatasetWatcher: %s' % str(e))
            path = self.wd_to_bpath.pop(wd)
            get_logger().info(
                'DatasetWatcher: '
                'Stopping notifier %s: %s' % (str(wd), path))

    def run(self):
        """ dataset watcher thread
        """
        try:
            self._start_notifier(self.__path)
            files = glob.glob(self.glds_pattern, recursive=True)
            get_logger().debug("Dataset files: %s" % files)
            for ffn in files:
                with self.scan_lock:
                    if ffn not in self.scan_watchers.keys():
                        ifn = ffn[:-(len(self.datasets))] + self.idatasets
                        self.scan_watchers[ffn] = ScanWatcher(
                            ffn, ifn, self.beamtimeId)
                        self.scan_watchers[ffn].start()
                        get_logger().info(
                            'DatasetWatcher: Starting %s' % ffn)
                        # get_logger().info(str(btmd))
            while self.running:
                # time.sleep(self.delay)
                events = inotifyx.get_events(self.notifier, self.timeout)
                get_logger().debug('Dt Tac')
                for event in events:
                    if event.wd in self.wd_to_path.keys():
                        get_logger().debug(
                            'Bt: %s %s %s' % (event.name,
                                              event.get_mask_description(),
                                              self.wd_to_path[event.wd]))
        # except Exception as e:
        #     get_logger().warn(str(e))
        #     raise
        finally:
            self.stop()

    def stop(self):
        """ stop the watcher
        """
        self.running = False
        time.sleep(0.2)
        self._stop_notifier()
        for ffn, scw in self.scan_watchers.items():
            get_logger().info(
                'DatasetWatcher: Stopping %s' % ffn)
            scw.running = False
            scw.join()
