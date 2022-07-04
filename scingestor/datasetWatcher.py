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
import glob
import json
import inotifyx
import subprocess
import requests
from .logger import get_logger


class DatasetWatcher(threading.Thread):
    """ Beamtime Watcher
    """

    def __init__(self, path, dsfile, idsfile, beamtimeId, beamtimefile,
                 ingestorcred, scicat_url, delay=5):
        """ constructor

        :param path: scan dir path
        :type path: :obj:`str`
        :param dsfile: file with a dataset list
        :type dsfile: :obj:`str`
        :param dsfile: file with a ingester dataset list
        :type dsfile: :obj:`str`
        :param beamtimeId: beamtime id
        :type beamtimeId: :obj:`str`
        :param beamtimefile: beamtime filename
        :type beamtimefile: :obj:`str`
        :param ingestorcred: ingestor credential
        :type ingestorcred: :obj:`str`
        :param scicat_url: scicat_url
        :type scicat_url: :obj:`str`
        :param delay: time delay
        :type delay: :obj:`str`
        """
        threading.Thread.__init__(self)
        # (:obj:`str`) file with a dataset list
        self.__dsfile = dsfile
        # (:obj:`str`) file with a ingested dataset list
        self.__idsfile = idsfile
        # (:obj:`str`) scan path dir
        self.__path = path
        # (:obj:`str`) beamtime id
        self.__bid = beamtimeId
        # (:obj:`str`) beamtime id
        self.__bfile = beamtimefile
        # (:obj:`str`) beamtime id
        self.__incd = ingestorcred
        # (:obj:`str`) scicat_url
        self.__scicat_url = scicat_url
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
        # (:obj:`str`) http icat
        self.http = "https://icat-science3d.desy.de/"

        # (:obj:`str`) raw dataset scan postfix
        self.__scanpostfix = ".scan*"
        # (:obj:`str`) origin datablock scan postfix
        self.__datablockpostfix = ".origdatablock*"

        # (:obj:`str`) nexus dataset shell command
        self.__datasetcommandnxs = "nxsfileinfo metadata " \
            " -o {scanpath}/{scanname}{scpostfix}.json " \
            " -b {beamtimefile} -p {beamtimeid} " \
            "{scanpath}/{scanname}.nxs"
        # (:obj:`str`) datablock shell command
        self.__datasetcommand = "nxsfileinfo metadata " \
            " -o {scanpath}/{scanname}{scpostfix}.json " \
            " -b {beamtimefile} -p {beamtimeid}"
        # (:obj:`str`) datablock shell command
        self.__datablockcommand = "nxsfileinfo origdatablock " \
            " -p {beamtimeid}/{scanname} " \
            " -o {scanpath}/{scanname}{dbpostfix}.json " \
            " {scanpath}/{scanname}"

        # (:obj:`dict` <:obj:`str`, :obj:`str`>) command format parameters
        self.__dctfmt = {
            "scanname": None,
            "scanpath": self.__path,
            "beamtimeid": self.__bid,
            "beamtimefile": self.__bfile,
            "scpostfix": self.__scanpostfix.replace("*", ""),
            "dbpostfix": self.__datablockpostfix.replace("*", ""),
        }
        get_logger().debug(
            'DatasetWatcher: Parameters: %s' % str(self.__dctfmt))

        # (:obj:`float`) timeout value for inotifyx get events
        self.timeout = 1

        # (:obj:`dict` <:obj:`str`, :obj:`str`>) request headers
        self.__headers = {'Content-Type': 'application/json',
                          'Accept': 'application/json'}

        # (:obj:`str`) token url
        # self.__tokenurl = "http://www-science3d.desy.de:3000/api/v3/" \
        #       "Users/login"
        if not self.__scicat_url.endswith("/"):
            self.__scicat_url = self.__scicat_url + "/"
        self.__tokenurl = self.__scicat_url +  "Users/login"
        # get_logger().info(
        #     'DatasetWatcher: LOGIN %s' % self.__tokenurl)
        # (:obj:`str`) dataset url
        # self.__dataseturl = "http://www-science3d.desy.de:3000/api/v3/" \
        #    "Datasets"
        self.__dataseturl = self.__scicat_url + "Datasets"
        # (:obj:`str`) origdatablock url
        # self.__dataseturl = "http://www-science3d.desy.de:3000/api/v3/" \
        #     "OrigDatablocks"
        self.__dataseturl = self.__scicat_url + "OrigDatablocks"

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

    def _generate_rawdataset_metadata(self, scan):
        """ generate raw dataset metadata

        :param scan: scan name
        :type scan: :obj:`str
        :returns: a file name of generate file
        :rtype: :obj:`str
        """
        nxsmasterfile = "{scanpath}/{scanname}.nxs".format(**self.__dctfmt)
        if os.path.isfile(nxsmasterfile):
            get_logger().info(
                'DatasetWatcher: Generating nxs metadata: %s %s' % (
                    scan,
                    "{scanpath}/{scanname}{scpostfix}.json".format(
                        **self.__dctfmt)))
            get_logger().debug(
                'DatasetWatcher: Generating datablock command: %s ' % (
                    self.__datasetcommandnxs.format(**self.__dctfmt)))
            subprocess.run(
                self.__datasetcommandnxs.format(**self.__dctfmt).split())
        else:
            get_logger().info(
                'DatasetWatcher: Generating metadata: %s %s' % (
                    scan,
                    "{scanpath}/{scanname}{scpostfix}.json".format(
                        **self.__dctfmt)))
            get_logger().debug(
                'DatasetWatcher: Generating datablock command: %s ' % (
                    self.__datasetcommand.format(**self.__dctfmt)))
            subprocess.run(
                self.__datasetcommand.format(**self.__dctfmt).split())

        rdss = glob.glob(
            "{scan}{postfix}.json".format(
                scan=scan, postfix=self.__scanpostfix))
        if rdss and rdss[0]:
            return rdss[0]
        return ""

    def _generate_origdatablock_metadata(self, scan):
        """ generate origdatablock metadata

        :param scan: scan name
        :type scan: :obj:`str
        :returns: a file name of generate file
        :rtype: :obj:`str
        """
        get_logger().info(
            'DatasetWatcher: Generating origdatablock metadata: %s %s' % (
                scan,
                "{scanpath}/{scanname}{dbpostfix}.json".format(
                    **self.__dctfmt)))
        get_logger().debug(
            'DatasetWatcher: Generating origdatablock command: %s ' % (
                self.__datablockcommand.format(**self.__dctfmt)))
        subprocess.run(
            self.__datablockcommand.format(**self.__dctfmt).split())
        odbs = glob.glob(
            "{scan}{postfix}.json".format(
                scan=scan, postfix=self.__datablockpostfix))
        if odbs and odbs[0]:
            return odbs[0]
        return ""

    def _get_token(self):
        """ provides ingestor token

        :returns: ingestor token
        :rtype: :obj:`str
        """
        try:
            response = requests.post(
                self.__tokenurl, headers=self.__headers,
                json={"username": "ingestor", "password": self.__incd})
            if response.ok:
                return json.loads(response.content)["id"]
            else:
                raise Exception("%s" % response.text)
        except Exception as e:
            get_logger().error(
                'DatasetWatcher: %s' % (str(e)))
        return ""

    def _ingest_dataset(self, metadata, token):
        """ ingests dataset

        :param metadata: metadata in json string
        :type metadata: :obj:`str
        :param token: ingestor token
        :type token: :obj:`str
        :returns: a file name of generate file
        :rtype: :obj:`str
        """
        try:
            response = requests.post(
                "%s?access_token=%s" % (self.__dataseturl, token),
                headers=self.__headers,
                data=metadata)
            if response.ok:
                return True
            else:
                raise Exception("%s" % response.text)

        except Exception as e:
            get_logger().error(
                'DatasetWatcher: %s' % (str(e)))
        return False

    def _ingest_datablock(self, metadata, token):
        """ ingets origdatablock
        """
        try:
            response = requests.post(
                "%s?access_token=%s" % (self.__datablockurl, token),
                headers=self.__headers,
                data=metadata)
            if response.ok:
                return True
            else:
                raise Exception("%s" % response.text)
        except Exception as e:
            get_logger().error(
                'DatasetWatcher: %s' % (str(e)))
        return False

    def _ingest_rawdataset_metadata(self, metafile, token):
        """ ingest raw dataset metadata

        :param metafile: metadata file name
        :type metafile: :obj:`str
        :returns: dataset id
        :rtype: :obj:`str
        """
        try:
            with open(metafile) as fl:
                smt = fl.read()
                mt = json.loads(smt)
            if mt["proposalId"] != self.__bid:
                raise Exception(
                    "Wrong SC proposalId %s for DESY beamtimeId %s in %s"
                    % (mt["pid"], self.__bid, metafile))
            if not mt["pid"].startswith("%s/" % self.__bid):
                raise Exception(
                    "Wrong pid %s for DESY beamtimeId %s in  %s"
                    % (mt["pid"], self.__bid, metafile))
            status = self._ingest_dataset(token, smt)
            if status:
                return mt["pid"]
        except Exception as e:
            get_logger().error(
                'DatasetWatcher: %s' % (str(e)))
        return None

    def _ingest_origdatablock_metadata(self, metafile, pid, token):
        """ ingest origdatablock metadata

        :param metafile: metadata file name
        :type metafile: :obj:`str
        :param pid: dataset id
        :type pid: :obj:`str
        :returns: dataset id
        :rtype: :obj:`str
        """
        try:
            with open(metafile) as fl:
                smt = fl.read()
                mt = json.loads(smt)
            if not mt["datasetId"].startswith("%s/" % self.__bid):
                raise Exception(
                    "Wrong datasetId %s for DESY beamtimeId %s in  %s"
                    % (mt["pid"], self.__bid, metafile))
            if mt["datasetId"] != pid:
                raise Exception(
                    "Wrong datasetId %s for DESY beamtimeId %s in %s"
                    % (mt["pid"], self.__bid, metafile))
            token = self._get_token()
            status = self._ingest_origdatablock(token, smt)
            if status:
                return mt["datasetId"]
        except Exception as e:
            get_logger().error(
                'DatasetWatcher: %s' % (str(e)))
        return ""

    def ingest(self, scan, token):
        """ ingest scan

        :param scan: scan name
        :type scan: :obj:`str
        """
        get_logger().info(
            'DatasetWatcher: Ingesting: %s %s' % (
                self.__dsfile, scan))

        self.__dctfmt["scanname"] = scan

        rdss = glob.glob(
            "{scan}{postfix}.json".format(
                scan=scan, postfix=self.__scanpostfix))
        if rdss and rdss[0]:
            rds = rdss[0]
        else:
            rds = self._generate_rawdataset_metadata(scan)

        odbs = glob.glob(
            "{scan}{postfix}.json".format(
                scan=scan, postfix=self.__datablockpostfix))
        if odbs and odbs[0]:
            odb = odbs[0]
        else:
            odb = self._generate_origdatablock_metadata(scan)
        scstatus = None
        dbstatus = None
        if rds and odb:
            if rds and rds[0]:
                pid = self._ingest_rawdataset_metadata(rds, token)
            if odb and odb[0] and pid:
                dbstatus = self._ingest_origdatablock_metadata(
                    odb, pid, token)

        if scstatus and dbstatus:
            ctime = time.time()
        else:
            ctime = 0
        self.sc_ingested.append([scan, str(ctime)])
        with open(self.__idsfile, 'a+') as f:
            f.write("%s %s\n" % (scan, ctime))

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
                    sc.strip().split(" ") for sc in idsf.read().split("\n")
                    if sc.strip()]
        ingested = [sc[0] for sc in self.sc_ingested]
        self.sc_waiting = [sc for sc in scans
                           if sc not in ingested]

        get_logger().info(
            'DatasetWatcher: Scans waiting: %s' % str(self.sc_waiting))
        get_logger().info(
            'DatasetWatcher: Scans ingested: %s' % str(self.sc_ingested))
        if self.sc_waiting:
            time.sleep(self.delay)
        if self.sc_waiting:
            token = self._get_token()
            for scan in self.sc_waiting:
                self.ingest(scan, token)

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
                                            sc.strip().split(" ")
                                            for sc in idsf.read().split("\n")
                                            if sc.strip()]
                                ingested = [sc[0] for sc in self.sc_ingested]
                                self.sc_waiting = [
                                    sc for sc in scans if sc not in ingested]

                if self.sc_waiting:
                    time.sleep(self.delay)
                    token = self._get_token()
                    for scan in self.sc_waiting:
                        self.ingest(scan, token)

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
