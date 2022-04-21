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
# import time
import signal
import sys
import json
import threading

from .datasetWatcher import DatasetWatcher
from .logger import logger

import inotifyx


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
            "/home/jkotan/gpfs/commissioning",
            # "/home/jkotan/gpfs/comissioning/raw",
            "/home/jkotan/gpfs/local",
        ]
        self.wait_for_dirs = {}

        self.notifier = None
        self.wd_to_path = {}
        self.wd_to_bpath = {}

        self.bt_prefix = "beamtime-metadata-"
        self.bt_postfix = ".json"

        self.dataset_watchers = {}
        self.dataset_lock = threading.Lock()
        self.timeout = 1

    def find_bt_files(self, path, prefix, postfix):
        """ find beamtime files

        :param path: beamtime directory
        :type path: :obj:`str`
        :param prefix: file name prefix
        :type prefix: :obj:`str`
        :param postfix: file name postfix
        :type postfix: :obj:`str`
        :returns: list of found files
        :rtype: :obj:`list` <:obj:`str`>
        """
        files = []
        try:
            if os.path.isdir(path):
                files = [fl for fl in os.listdir(path)
                         if (fl.startswith(prefix)
                             and fl.endswith(postfix))]
        except Exception as e:
            logger.warning(str(e))
        return files

    def _start_notifier(self, paths):
        """ start notifier

        :param paths: beamtime file paths
        :type paths: :obj:`list` <:obj:`str`>
        """
        self.notifier = inotifyx.init()

        for path in paths:
            self._add_path(path)

    def _add_path(self, path):
        """ add path to notifier

        :param path: beamtime file path
        :type path: :obj:`str`
        """
        try:
            watch_descriptor = inotifyx.add_watch(
                self.notifier, path,
                inotifyx.IN_CLOSE_WRITE | inotifyx.IN_DELETE |
                inotifyx.IN_MOVE_SELF |
                inotifyx.IN_ALL_EVENTS |
                inotifyx.IN_MOVED_TO | inotifyx.IN_MOVED_FROM)
            self.wd_to_path[watch_descriptor] = path
            logger.info('Starting %s: %s'
                        % (str(watch_descriptor), path))
        except Exception as e:
            logger.warning('%s: %s' % (path, str(e)))
            self._add_base_path(path)

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
                logger.info('Starting base %s: %s'
                            % (str(watch_descriptor), bpath))
                self.wait_for_dirs[bpath] = path
            except Exception as e:
                logger.warning('%s: %s' % (bpath, str(e)))
                if bpath == '/':
                    failing = False

    def _stop_notifier(self):
        """ start notifier
        """
        for wd in list(self.wd_to_path.keys()):
            inotifyx.rm_watch(self.notifier, wd)
            self.wd_to_path.pop(wd)
            logger.info('Stoping notifier %s' % str(wd))
        for wd in list(self.wd_to_bpath.keys()):
            inotifyx.rm_watch(self.notifier, wd)
            self.wd_to_bpath.pop(wd)
            logger.info('Stoping notifier %s' % str(wd))

    def start(self):
        """ start beamtime watcher
        """
        try:
            self._start_notifier(self.beamtime_dirs)

            for path in self.beamtime_dirs:
                files = self.find_bt_files(
                    path, self.bt_prefix, self.bt_postfix)

                self._lunch_dataset_watcher(path, files)
                logger.info('Files of %s: %s' % (path, files))

            while True:
                # time.sleep(self.delay)
                events = inotifyx.get_events(self.notifier, self.timeout)
                logger.info('Bt Tic')
                for event in events:
                    if event.wd in self.wd_to_path.keys():
                        logger.info(
                            'Bt: %s %s %s' % (event.name,
                                              event.get_mask_description(),
                                              self.wd_to_path[event.wd]))
                        masks = event.get_mask_description().split("|")
                        if "IN_IGNORED" in masks or \
                           "IN_MOVE_FROM" in masks or \
                           "IN_DELETE" in masks or \
                           "IN_MOVE_SELF" in masks:
                            path = self.wd_to_path.pop(event.wd)
                            logger.info('Removed %s' % path)
                            self._add_path(path)
                        elif "IN_CREATE" in masks or \
                             "IN_MOVE_TO" in masks:

                            files = [fl for fl in [event.name]
                                     if (fl.startswith(self.bt_prefix) and
                                         fl.endswith(self.bt_postfix))]
                            if files:
                                self._lunch_dataset_watcher(
                                    self.wd_to_path[event.wd], files)
                            else:
                                path = self.wd_to_path.pop(event.wd)
                                files = self.find_bt_files(
                                    path, self.bt_prefix, self.bt_postfix)

                                self._lunch_dataset_watcher(path, files)

                            logger.info('Start beamtime %s' % event.name)
                        # elif "IN_DELETE" in masks or \
                        #      "IN_MOVE_MOVE" in masks:
                        #     " remove dataset_watcher "

                    if event.wd in self.wd_to_bpath.keys():
                        logger.info(
                            'BB: %s %s %s' % (event.name,
                                              event.get_mask_description(),
                                              self.wd_to_bpath[event.wd]))
                        # if event.name is not None:
                        bpath = self.wd_to_bpath.pop(event.wd)
                        # npath = os.path.join(bpath, event.name)
                        if "IN_IGNORED" not in \
                           event.get_mask_description().split():
                            inotifyx.rm_watch(self.notifier, event.wd)
                        path = self.wait_for_dirs.pop(bpath)
                        self._add_path(path)

        except KeyboardInterrupt:
            logger.warning('Keyboard interrupt (SIGINT) received...')
            self.stop()

    def _lunch_dataset_watcher(self, path, files):
        """ lunch dataset watcher

        :param path: base file path
        :type path: :obj:`str`
        :param path: beamtime files
        :type path: :obj:`list`<:obj:`str`>
        """
        for bt in files:
            ffn = os.path.abspath(os.path.join(path, bt))
            try:
                with self.dataset_lock:
                    with open(ffn) as fl:
                        btmd = json.load(fl)
                        if ffn in self.dataset_watchers.keys():
                            self.dataset_watchers[ffn].stop()
                        self.dataset_watchers[ffn] =  \
                            DatasetWatcher(path, btmd)
                        self.dataset_watchers[ffn].start()
                        logger.info('Starting %s' % ffn)
            except Exception as e:
                logger.warn("%s cannot be watched: %s" % (ffn, str(e)))

    def stop(self):
        """ stop beamtime watcher
        """
        logger.info('Cleaning up...')
        self._stop_notifier()
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
