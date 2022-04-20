import os
import time
import logging
import signal
import sys
import json
import threading
import glob


# sudo ln -sf ~/ndts/ingestor/scicat-dataset-ingestor.service \
#  /etc/systemd/system/scicat-dataset-ingestor.service

# sudo systemctl daemon-reload
# sudo systemctl enable scicat-dataset-ingestor.service
#
# sudo systemctl start scicat-dataset-ingestor.service
# sudo systemctl stop scicat-dataset-ingestor.service
# sudo journalctl -u scicat-dataset-ingestor.service


def init_logger(name=__name__):
    """ init logger
    """
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    stdout_handler = logging.StreamHandler()
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.setFormatter(
        logging.Formatter('%(levelname)8s : %(message)s'))
    logger.addHandler(stdout_handler)
    return logger


logger = init_logger("BeamtimeWatcher")


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
                logger.info('Dt Tick')
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


if __name__ == '__main__':
    main()
