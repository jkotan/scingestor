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
import glob
import json
import subprocess
import requests

from .logger import get_logger


class DatasetIngestor:

    """ Dataset Ingestor
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
        # (:obj:`str`) file with a dataset list
        self.__dsfile = dsfile
        # (:obj:`str`) file with a ingested dataset list
        self.__idsfile = idsfile
        # (:obj:`str`) file with a ingested dataset list
        self.__idsfiletmp = "%s%s" % (idsfile, ".tmp")
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

        # (:obj:`list`<:obj:`str`>) ingested scan names
        self.__sc_ingested = []
        # (:obj:`list`<:obj:`str`>) waiting scan names
        self.__sc_waiting = []
        # (:obj:`dict`<:obj:`str`, :obj:`list`<:obj:`str`>>)
        #   ingested scan names
        self.__sc_ingested_map = {}

        # (:obj:`str`) raw dataset scan postfix
        self.__scanpostfix = ".scan*"
        # (:obj:`str`) origin datablock scan postfix
        self.__datablockpostfix = ".origdatablock*"

        # (:obj:`str`) nexus dataset shell command
        self.__datasetcommandnxs = "nxsfileinfo metadata " \
            " -o {scanpath}/{scanname}{scpostfix}.json " \
            " -b {beamtimefile} -p {beamtimeid}/{scanname} " \
            "{scanpath}/{scanname}.nxs"
        # (:obj:`str`) datablock shell command
        self.__datasetcommand = "nxsfileinfo metadata " \
            " -o {scanpath}/{scanname}{scpostfix}.json " \
            " -b {beamtimefile} -p {beamtimeid}/{scanname}"
        # (:obj:`str`) datablock shell command
        self.__datablockcommand = "nxsfileinfo origdatablock " \
            " -p {beamtimeid}/{scanname} " \
            " -o {scanpath}/{scanname}{dbpostfix}.json " \
            " {scanpath}/{scanname}"
        # (:obj:`str`) datablock shell command
        self.__datablockmemcommand = "nxsfileinfo origdatablock " \
            " -p {beamtimeid}/{scanname} " \
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
            'DatasetIngestor: Parameters: %s' % str(self.__dctfmt))

        # (:obj:`float`) timeout value for inotifyx get events in s
        self.timeout = 0.1
        # (:obj:`float`) time to recheck the dataset list
        self.checktime = 100

        # (:obj:`dict` <:obj:`str`, :obj:`str`>) request headers
        self.__headers = {'Content-Type': 'application/json',
                          'Accept': 'application/json'}

        # self.__tokenurl = "http://www-science3d.desy.de:3000/api/v3/" \
        #       "Users/login"
        if not self.__scicat_url.endswith("/"):
            self.__scicat_url = self.__scicat_url + "/"
        # (:obj:`str`) token url
        self.__tokenurl = self.__scicat_url + "Users/login"
        # get_logger().info(
        #     'DatasetIngestor: LOGIN %s' % self.__tokenurl)
        # (:obj:`str`) dataset url
        # self.__dataseturl = "http://www-science3d.desy.de:3000/api/v3/" \
        #    "Datasets"
        self.__dataseturl = self.__scicat_url + "Datasets"
        # (:obj:`str`) origdatablock url
        # self.__dataseturl = "http://www-science3d.desy.de:3000/api/v3/" \
        #     "OrigDatablocks"
        self.__datablockurl = self.__scicat_url + "OrigDatablocks"

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
                'DatasetIngestor: Generating nxs metadata: %s %s' % (
                    scan,
                    "{scanpath}/{scanname}{scpostfix}.json".format(
                        **self.__dctfmt)))
            get_logger().debug(
                'DatasetIngestor: Generating dataset command: %s ' % (
                    self.__datasetcommandnxs.format(**self.__dctfmt)))
            subprocess.run(
                self.__datasetcommandnxs.format(**self.__dctfmt).split())
        else:
            get_logger().info(
                'DatasetIngestor: Generating metadata: %s %s' % (
                    scan,
                    "{scanpath}/{scanname}{scpostfix}.json".format(
                        **self.__dctfmt)))
            get_logger().debug(
                'DatasetIngestor: Generating dataset command: %s ' % (
                    self.__datasetcommand.format(**self.__dctfmt)))
            subprocess.run(
                self.__datasetcommand.format(**self.__dctfmt).split())

        rdss = glob.glob(
            "{scanpath}/{scanname}{scpostfix}.json".format(
                        **self.__dctfmt))
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
            'DatasetIngestor: Generating origdatablock metadata: %s %s' % (
                scan,
                "{scanpath}/{scanname}{dbpostfix}.json".format(
                    **self.__dctfmt)))
        get_logger().debug(
            'DatasetIngestor: Generating origdatablock command: %s ' % (
                self.__datablockcommand.format(**self.__dctfmt)))
        subprocess.run(
            self.__datablockcommand.format(**self.__dctfmt).split())
        odbs = glob.glob(
            "{scanpath}/{scanname}{dbpostfix}.json".format(
                    **self.__dctfmt))
        if odbs and odbs[0]:
            return odbs[0]
        return ""

    def _regenerate_origdatablock_metadata(self, scan):
        """o generate origdatablock metadata

        :param scan: scan name
        :type scan: :obj:`str
        :returns: a file name of generate file
        :rtype: :obj:`str
        """
        mfilename = "{scanpath}/{scanname}{dbpostfix}.json".format(
            **self.__dctfmt)
        get_logger().info(
            'DatasetIngestor: Checking origdatablock metadata: %s %s' % (
                scan, mfilename))
        get_logger().debug(
            'DatasetIngestor: Checking origdatablock command: %s ' % (
                self.__datablockcommand.format(**self.__dctfmt)))
        dmeta = None
        try:
            with open(mfilename, "r") as mf:
                meta = mf.read()
                dmeta = json.loads(meta)
        except Exception as e:
            get_logger().warning('%s: %s' % (scan, str(e)))

        if dmeta is None:
            subprocess.run(
                self.__datablockcommand.format(**self.__dctfmt).split())
        else:
            result = subprocess.run(
                self.__datablockmemcommand.format(**self.__dctfmt).split(),
                text=True, capture_output=True)
            nwmeta = str(result.stdout)
            try:
                dnwmeta = json.loads(nwmeta)
            except Exception as e:
                get_logger().warning('%s: %s' % (scan, str(e)))
                dnwmeta = None
            if dnwmeta is not None:
                if self._metadataEqual(dmeta, dnwmeta):
                    with open(mfilename, "w") as mf:
                        mf.write()
                        dmeta = json.loads(meta)

        odbs = glob.glob(
            "{scanpath}/{scanname}{dbpostfix}.json".format(
                    **self.__dctfmt))
        if odbs and odbs[0]:
            return odbs[0]
        return ""

    def _metadataEqual(self, dct, dct2, skip=None, parent=None):
        parent = parent or ""
        if len(list(dct.keys())) != len(list(dct2.keys())):
            return False
        status = True
        for k, v in dct.items():
            if parent:
                node = "%s.%s" % (parent, k)
            else:
                node = k

            if k not in dct2.keys():
                status = False
                break

            if not skip or node not in skip:

                if isinstance(v, dict):
                    if not self._areMetadataEqual(v, dct2[k], skip, node):
                        status = False
                        break
                else:
                    if v != dct2[k]:
                        status = False
                        break

        return status

    def get_token(self):
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
                'DatasetIngestor: %s' % (str(e)))
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
                'DatasetIngestor: %s' % (str(e)))
        return False

    def _ingest_origdatablock(self, metadata, token):
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
                'DatasetIngestor: %s' % (str(e)))
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
                    % (mt["proposalId"], self.__bid, metafile))
            if not mt["pid"].startswith("%s/" % self.__bid):
                raise Exception(
                    "Wrong pid %s for DESY beamtimeId %s in  %s"
                    % (mt["pid"], self.__bid, metafile))
            status = self._ingest_dataset(smt, token)
            if status:
                return mt["pid"]
        except Exception as e:
            get_logger().error(
                'DatasetIngestor: %s' % (str(e)))
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
            status = self._ingest_origdatablock(smt, token)
            if status:
                return mt["datasetId"]
        except Exception as e:
            get_logger().error(
                'DatasetIngestor: %s' % (str(e)))
        return ""

    def ingest(self, scan, token):
        """ ingest scan

        :param scan: scan name
        :type scan: :obj:`str`
        :param token: access token
        :type token: :obj:`str`
        """
        get_logger().info(
            'DatasetIngestor: Ingesting: %s %s' % (
                self.__dsfile, scan))

        self.__dctfmt["scanname"] = scan

        rdss = glob.glob(
            "{scanpath}/{scan}{postfix}.json".format(
                scan=scan, postfix=self.__scanpostfix,
                scanpath=self.__dctfmt["scanpath"]))
        if rdss and rdss[0]:
            rds = rdss[0]
        else:
            rds = self._generate_rawdataset_metadata(scan)

        odbs = glob.glob(
            "{scanpath}/{scan}{postfix}.json".format(
                scan=scan, postfix=self.__datablockpostfix,
                scanpath=self.__dctfmt["scanpath"]))
        if odbs and odbs[0]:
            odb = odbs[0]
        else:
            odb = self._generate_origdatablock_metadata(scan)
        dbstatus = None

        if rds and odb:
            if rds and rds[0]:
                pid = self._ingest_rawdataset_metadata(rds, token)
            if odb and odb[0] and pid:
                dbstatus = self._ingest_origdatablock_metadata(
                    odb, pid, token)
        if pid and dbstatus:
            ctime = time.time()
        else:
            ctime = 0
        self.__sc_ingested.append([scan, str(ctime)])
        with open(self.__idsfile, 'a+') as f:
            f.write("%s %s\n" % (scan, ctime))

    def reingest(self, scan, token):
        """ ingest scan

        :param scan: scan name
        :type scan: :obj:`str`
        :param token: access token
        :type token: :obj:`str`
        """
        get_logger().info(
            'DatasetIngestor: Checking: %s %s' % (
                self.__dsfile, scan))

        reingest_dataset = False
        reingest_origdatablock = False
        self.__dctfmt["scanname"] = scan
        rdss = glob.glob(
            "{scanpath}/{scan}{postfix}.json".format(
                scan=scan, postfix=self.__scanpostfix,
                scanpath=self.__dctfmt["scanpath"]))

        if rdss and rdss[0]:
            rds = rdss[0]
            mtm = os.path.getmtime(rds)
            # print(self.__sc_ingested_map.keys())

            if scan in self.__sc_ingested_map.keys():
                get_logger().debug("DS Timestamps: %s %s %s %s" % (
                    scan,
                    mtm, self.__sc_ingested_map[scan][-1],
                    mtm > self.__sc_ingested_map[scan][-1]))

            if scan not in self.__sc_ingested_map.keys() \
               or mtm > self.__sc_ingested_map[scan][-1]:
                reingest_dataset = True
        else:
            rds = self._generate_rawdataset_metadata(scan)
            get_logger().debug("DS No File: %s True" % (scan))
            reingest_dataset = True

        odbs = glob.glob(
            "{scanpath}/{scan}{postfix}.json".format(
                scan=scan, postfix=self.__datablockpostfix,
                scanpath=self.__dctfmt["scanpath"]))
        if odbs and odbs[0]:
            odb = odbs[0]
            mtm = os.path.getmtime(odb)
            self._regenerate_origdatablock_metadata(scan)

            if scan in self.__sc_ingested_map.keys():
                get_logger().debug("DB Timestamps: %s %s %s %s" % (
                    scan,
                    mtm, self.__sc_ingested_map[scan][-1],
                    mtm > self.__sc_ingested_map[scan][-1]))

            if scan not in self.__sc_ingested_map.keys() \
               or mtm > self.__sc_ingested_map[scan][-1]:
                reingest_origdatablock = True
        else:
            odb = self._generate_origdatablock_metadata(scan)
            get_logger().debug("DB No File: %s True" % (scan))
            reingest_origdatablock = True
        dbstatus = None

        pid = None
        if rds and odb:
            if rds and rds[0] and reingest_dataset:
                pid = self._ingest_rawdataset_metadata(rds, token)
                get_logger().info(
                    "DatasetIngestor: Ingest dataset: %s" % (rds))
            if odb and odb[0] and pid and reingest_origdatablock:
                dbstatus = self._ingest_origdatablock_metadata(
                    odb, pid, token)
                get_logger().info(
                    "DatasetIngestor: Ingest origdatablock: %s" % (odb))
        if (pid and reingest_dataset) or (dbstatus and reingest_origdatablock):
            ctime = time.time()
            # get_logger().debug("Ingest TS New")
        elif scan in self.__sc_ingested_map.keys():
            ctime = self.__sc_ingested_map[scan][-1]
            # get_logger().debug("Ingest TS Old")
        else:
            ctime = 0
            # get_logger().debug("Ingest TS 0")
        self.__sc_ingested.append([scan, str(ctime)])
        with open(self.__idsfiletmp, 'a+') as f:
            f.write("%s %s\n" % (scan, ctime))

    def check_list(self, reingest=False):
        """ update waiting and ingested datasets
        """
        with open(self.__dsfile, "r") as dsf:
            scans = [sc.strip()
                     for sc in dsf.read().split("\n")
                     if sc.strip()]
        if os.path.isfile(self.__idsfile):
            with open(self.__idsfile, "r") as idsf:
                self.__sc_ingested = [
                    sc.strip().split(" ")
                    for sc in idsf.read().split("\n")
                    if sc.strip()]
        if not reingest:
            ingested = [sc[0] for sc in self.__sc_ingested]
            self.__sc_waiting = [
                sc for sc in scans if sc not in ingested]
        else:
            self.__sc_waiting = [sc for sc in scans]
            self.__sc_ingested_map = {}
            for sc in self.__sc_ingested:
                try:
                    if len(sc) > 1 and float(sc[-1]) > 0:
                        sc[-1] = float(sc[-1])
                        self.__sc_ingested_map[sc[0]] = sc
                except Exception:
                    pass

    def waiting_datasets(self):
        """ provides waitings datasets

        :returns: waitings datasets list
        :rtype: :obj:`list` <:obj:`str`>
        """
        return list(self.__sc_waiting)

    def clear_waiting_datasets(self):
        """ clear waitings datasets
        """
        self.__sc_waiting = []

    def clear_tmpfile(self):
        """ clear waitings datasets
        """
        if os.path.exists(self.__idsfiletmp):
            os.remove(self.__idsfiletmp)

    def update_from_tmpfile(self):
        """ clear waitings datasets
        """
        os.rename(self.__idsfiletmp, self.__idsfile)

    def ingested_datasets(self):
        """ provides ingested datasets

        :returns:  ingested datasets list
        :rtype: :obj:`list` <:obj:`str`>
        """
        return list(self.__sc_ingested)
