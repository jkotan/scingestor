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

from .datasetWatcher import DatasetWatcher
from .logger import get_logger

import inotifyx


class ScanDirWatcher(threading.Thread):
    """ Beamtime Watcher
    """

    def __init__(self, path, meta, bpath, delay=5):
        """ constructor

        :param path: scan dir path
        :type path: :obj:`str`
        :param meta: beamtime configuration
        :type meta: :obj:`dict` <:obj:`str`,`any`>
        :param delay: time delay
        :type delay: :obj:`int`
        """
        threading.Thread.__init__(self)
        # (:obj:`str`) scan dir path
        self.__path = path
        # (:obj:`str`) beamtime path and file name
        self.__bpath = bpath
        # (:obj:`dict` <:obj:`str`,`any`>) beamtime configuration
        self.__meta = meta
        # (:obj:`str`) beamtime id
        self.beamtimeId = meta["beamtimeId"]
        # (:obj:`float`) delay time for ingestion in s
        self.delay = delay
        # (:obj:`bool`) running loop flag
        self.running = True
        # (:obj:`str`) scicat dataset file pattern
        self.ds_pattern = "scicat-datasets-{bt}.lst"
        # (:obj:`str`) indested scicat dataset file pattern
        self.ids_pattern = "scicat-ingested-datasets-{bt}.lst"

        # (:obj:`int`) notifier ID
        self.notifier = None
        # (:obj:`dict` <:obj:`int`, :obj:`str`>) watch description paths
        self.wd_to_path = {}

        # (:obj:`dict` <(:obj:`str`, :obj:`str`),
        #                :class:`scanDirWatcher.ScanDirWatcher`>)
        #        dataset watchers instances for given path and beamtime file
        self.dataset_watchers = {}
        # (:class:`threading.Lock`) dataset watcher dictionary lock
        self.dataset_lock = threading.Lock()
        # (:obj:`float`) timeout value for inotifyx get events
        self.timeout = 1

        # (:obj:`dict` <(:obj:`str`, :obj:`str`),
        #                :class:`scanDirWatcher.ScanDirWatcher`>)
        #        scandir watchers instances for given path and beamtime file
        self.scandir_watchers = {}
        # (:class:`threading.Lock`) scandir watcher dictionary lock
        self.scandir_lock = threading.Lock()

        # (:obj:`str`) datasets file name
        self.dslist_filename = self.ds_pattern.format(bt=self.beamtimeId)
        # (:obj:`str`) ingescted datasets file name
        self.idslist_filename = self.ids_pattern.format(bt=self.beamtimeId)
        # (:obj:`str`) datasets file name
        self.dslist_fullname = os.path.join(self.__path, self.dslist_filename)

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
            get_logger().info('ScanDirWatcher: Starting ScanDir %s: %s'
                              % (str(watch_descriptor), path))
        except Exception as e:
            get_logger().warning('%s: %s' % (path, str(e)))

    def _stop_notifier(self):
        """ start notifier
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
                'Stopping notifier %s: %s' % (str(wd), path))

    def _lunch_scandir_watcher(self, paths):
        """ lunch scandir watcher

        :param path: list of subdirectories
        :type path: :obj:`list`<:obj:`str`>
        """
        for path in paths:
            try:
                with self.scandir_lock:
                    if (path, self.__bpath) \
                       not in self.scandir_watchers.keys():
                        self.scandir_watchers[(path, self.__bpath)] =  \
                            ScanDirWatcher(
                                path, self.__meta, self.__bpath)
                        get_logger().info(
                            'ScanDirWatcher: Create ScanDirWatcher %s %s'
                            % (path, self.__bpath))
                        self.scandir_watchers[(path, self.__bpath)].start()
            except Exception as e:
                get_logger().warning(
                    "%s cannot be watched: %s" % (path, str(e)))

    def run(self):
        """ scandir watcher thread
        """
        try:
            self._start_notifier(self.__path)
            get_logger().debug("ScanDir file:  %s " % (self.dslist_fullname))
            if os.path.isfile(self.dslist_fullname):
                with self.dataset_lock:
                    fn = self.dslist_fullname
                    if fn not in self.dataset_watchers.keys():
                        ifn = fn[:-(len(self.dslist_filename))] + \
                            self.idslist_filename
                        self.dataset_watchers[fn] = DatasetWatcher(
                            fn, ifn, self.beamtimeId)
                        self.dataset_watchers[fn].start()
                        get_logger().info(
                            'ScanDirWatcher: Starting %s' % fn)
                        # get_logger().info(str(btmd))

            subdirs = [it.path for it in os.scandir(self.__path)
                       if it.is_dir()]
            self._lunch_scandir_watcher(subdirs)

            while self.running:
                # time.sleep(self.delay)
                events = inotifyx.get_events(self.notifier, self.timeout)
                get_logger().debug('Dt Tac')
                for event in events:
                    if event.wd in self.wd_to_path.keys():
                        get_logger().debug(
                            'Sd: %s %s %s' % (event.name,
                                              event.get_mask_description(),
                                              self.wd_to_path[event.wd]))
                        masks = event.get_mask_description().split("|")
                        if "IN_ISDIR" in masks and (
                                "IN_CREATE" in masks or "IN_MOVE_TO" in masks):
                            npath = os.path.join(
                                self.wd_to_path[event.wd], event.name)
                            self._lunch_scandir_watcher([npath])
                        elif "IN_CREATE" in masks or "IN_MOVE_TO" in masks:
                            fn = os.path.join(
                                self.wd_to_path[event.wd], event.name)
                            with self.dataset_lock:
                                if fn not in self.dataset_watchers.keys() and \
                                   fn == self.dslist_fullname:
                                    ifn = fn[:-(len(self.dslist_filename))] + \
                                        self.idslist_filename
                                    self.dataset_watchers[fn] = DatasetWatcher(
                                        fn, ifn, self.beamtimeId)
                                    self.dataset_watchers[fn].start()
                                    get_logger().info(
                                        'ScanDirWatcher: Starting %s' % fn)

                        # elif "IN_DELETE_SELF" in masks:
                        #     "remove scandir watcher"
                        #     # self.wd_to_path[event.wd]

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
        for fn, scw in self.dataset_watchers.items():
            get_logger().info(
                'ScanDirWatcher: Stopping %s' % fn)
            scw.running = False
            scw.join()
        for pf, dsw in self.scandir_watchers.items():
            path, fn = pf
            get_logger().info('ScanDirWatcher: '
                              'Stopping %s' % fn)
            dsw.running = False
            dsw.join()
