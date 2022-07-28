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
import glob
import json
import subprocess
import requests

from .logger import get_logger


class DatasetIngestor:

    """ Dataset Ingestor
    """

    def __init__(self, configuration,
                 path, dsfile, idsfile, meta, beamtimefile,
                 delay=5):
        """ constructor

        :param configuration: dictionary with the ingestor configuration
        :type configuration: :obj:`dict` <:obj:`str`, `any`>
        :param path: scan dir path
        :type path: :obj:`str`
        :param dsfile: file with a dataset list
        :type dsfile: :obj:`str`
        :param dsfile: file with a ingester dataset list
        :type dsfile: :obj:`str`
        :param meta: beamtime configuration
        :type meta: :obj:`dict` <:obj:`str`,`any`>
        :param beamtimefile: beamtime filename
        :type beamtimefile: :obj:`str`
        :param doiprefix: doiprefix
        :type doiprefix: :obj:`str`
        :param ingestorcred: ingestor credential
        :type ingestorcred: :obj:`str`
        :param scicat_url: scicat_url
        :type scicat_url: :obj:`str`
        :param delay: time delay
        :type delay: :obj:`str`
        """
        # (:obj:`dict` <:obj:`str`, `any`>) ingestor configuration
        self.__config = configuration or {}
        # (:obj:`str`) file with a dataset list
        self.__dsfile = dsfile
        # (:obj:`str`) file with a ingested dataset list
        self.__idsfile = idsfile
        # (:obj:`str`) file with a ingested dataset list
        self.__idsfiletmp = "%s%s" % (idsfile, ".tmp")
        # (:obj:`str`) scan path dir
        self.__path = path
        # (:obj:`str`) beamtime id
        self.__bid = meta["beamtimeId"]
        # (:obj:`str`) beamline name
        self.__bl = meta["beamline"]
        # (:obj:`str`) beamtime id
        self.__bfile = beamtimefile

        bpath, _ = os.path.split(beamtimefile)
        # (:obj:`str`) relative scan path to beamtime path
        self.__relpath = os.path.relpath(path, bpath)

        # (:obj:`str`) doi prefix
        self.__doiprefix = "10.3204"
        # (:obj:`str`) beamtime id
        self.__incd = None
        # (:obj:`str`) scicat url
        self.__scicat_url = "http://localhost:8881"

        if "doiprefix" in self.__config.keys():
            self.__doiprefix = self.__config["doi_prefix"]
        if "ingestor_credential_file" in self.__config.keys():
            with open(self.__config["ingestor_credential_file"]) as fl:
                self.__incd = fl.read().strip()
        if "scicat_url" in self.__config.keys():
            self.__scicat_url = self.__config["scicat_url"]

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
            " -r {relpath} " \
            "{scanpath}/{scanname}.nxs"
        # (:obj:`str`) datablock shell command
        self.__datasetcommand = "nxsfileinfo metadata " \
            " -o {scanpath}/{scanname}{scpostfix}.json " \
            " -r {relpath} " \
            " -b {beamtimefile} -p {beamtimeid}/{scanname}"
        # (:obj:`str`) datablock shell command
        self.__datablockcommand = "nxsfileinfo origdatablock " \
            " -s *.pyc,*.origdatablock.json,*.scan.json,*~ " \
            " -p {doiprefix}/{beamtimeid}/{scanname} " \
            " -c {beamtimeid}-clbt,{beamtimeid}-dmgt,{beamline}dmgt" \
            " -o {scanpath}/{scanname}{dbpostfix}.json " \
            " {scanpath}/{scanname}"
        # (:obj:`str`) datablock shell command
        self.__datablockmemcommand = "nxsfileinfo origdatablock " \
            " -s *.pyc,*.origdatablock.json,*.scan.json,*~ " \
            " -c {beamtimeid}-clbt,{beamtimeid}-dmgt,{beamline}dmgt" \
            " -p {doiprefix}/{beamtimeid}/{scanname} " \
            " {scanpath}/{scanname}"

        # (:obj:`dict` <:obj:`str`, :obj:`str`>) command format parameters
        self.__dctfmt = {
            "scanname": None,
            "scanpath": self.__path,
            "relpath": self.__relpath,
            "beamtimeid": self.__bid,
            "beamline": self.__bl,
            "doiprefix": self.__doiprefix,
            "beamtimefile": self.__bfile,
            "scpostfix": self.__scanpostfix.replace("*", ""),
            "dbpostfix": self.__datablockpostfix.replace("*", ""),
        }
        get_logger().debug(
            'DatasetIngestor: Parameters: %s' % str(self.__dctfmt))

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
        #    "RawDatasets"
        self.__dataseturl = self.__scicat_url + "RawDatasets"
        # (:obj:`str`) origdatablock url
        # self.__dataseturl = "http://www-science3d.desy.de:3000/api/v3/" \
        #     "OrigDatablocks"
        self.__datablockurl = self.__scicat_url + "OrigDatablocks"

        self.__withoutsm = [
            "techniques",
            "classification",
            "createdBy",
            "updatedBy",
            "datasetlifecycle",
            "numberOfFiles",
            "size",
            "createdAt",
            "updatedAt",
            "history",
            "creationTime",
            "version",
            "scientificMetadata",
            "endTime"
        ]

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

    def _regenerate_origdatablock_metadata(self, scan, force=False):
        """o generate origdatablock metadata

        :param scan: scan name
        :type scan: :obj:`str`
        :param force: force flag
        :type force: :obj:`bool`
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
            # print("M2", dnwmeta)
            if dnwmeta is not None:
                if not self._metadataEqual(dmeta, dnwmeta) or force:
                    with open(mfilename, "w") as mf:
                        mf.write(nwmeta)

        odbs = glob.glob(
            "{scanpath}/{scanname}{dbpostfix}.json".format(
                    **self.__dctfmt))
        if odbs and odbs[0]:
            return odbs[0]
        return ""

    def _metadataEqual(self, dct, dct2, skip=None, parent=None):
        """ compare two dictionaries if metdatdata is equal

        :param dct: first metadata dictionary
        :type dct: :obj:`dct` <:obj:`str`, `any`>
        :param dct2: second metadata dictionary
        :type dct2: :obj:`dct` <:obj:`str`, `any`>
        :param skip: a list of keywords to skip
        :type skip: :obj:`list` <:obj:`str`>
        :param parent: the parent metadata dictionary to use in recursion
        :type parent: :obj:`dct` <:obj:`str`, `any`>
        """
        parent = parent or ""
        w1 = [("%s.%s" % (parent, k) if parent else k)
              for k in dct.keys()
              if (not skip or
                  (("%s.%s" % (parent, k) if parent else k)
                   not in skip))]
        w2 = [("%s.%s" % (parent, k) if parent else k)
              for k in dct2.keys()
              if (not skip or
                  (("%s.%s" % (parent, k) if parent else k)
                   not in skip))]
        if len(w1) != len(w2):
            get_logger().debug(
                'DatasetIngestor: %s != %s' % (
                    list(w1), list(w2)))
            return False
        status = True
        for k, v in dct.items():
            if parent:
                node = "%s.%s" % (parent, k)
            else:
                node = k

            if not skip or node not in skip:

                if k not in dct2.keys():
                    get_logger().debug(
                        'DatasetIngestor: %s not in %s'
                        % (k,  dct2.keys()))
                    status = False
                    break
                if isinstance(v, dict):
                    if not self._metadataEqual(v, dct2[k], skip, node):
                        status = False
                        break
                else:
                    if v != dct2[k]:
                        get_logger().debug(
                            'DatasetIngestor %s: %s != %s'
                            % (k, v,  dct2[k]))

                        status = False
                        break

        return status

    def get_token(self):
        """ provides ingestor token

        :returns: ingestor token
        :rtype: :obj:`str`
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

    def _ingest_dataset(self, metadata, token, mdct):
        """ ingests dataset

        :param metadata: metadata in json string
        :type metadata: :obj:`str
        :param token: ingestor token
        :type token: :obj:`str
        :param mdct: metadata in dct
        :type mdct: :obj:`dct` <:obj:`str`, `any`>
        :returns: a file name of generate file
        :rtype: :obj:`str
        """
        try:
            pid = "%s/%s" % (self.__doiprefix, mdct["pid"])
            # check if dataset with the pid exists
            get_logger().info(
                'DatasetIngestor: Check if dataset exists: %s' % (pid))
            resexists = requests.get(
                "{url}/{pid}/exists?access_token={token}".format(
                    url=self.__dataseturl,
                    pid=pid.replace("/", "%2F"),
                    token=token))
            if resexists.ok:
                exists = json.loads(resexists.content)["exists"]
                if not exists:
                    # post the new dataset since it does not exist
                    get_logger().info(
                        'DatasetIngestor: Post the dataset: %s' % (pid))
                    response = requests.post(
                        "%s?access_token=%s" % (self.__dataseturl, token),
                        headers=self.__headers,
                        data=metadata)
                    if response.ok:
                        return mdct["pid"]
                    else:
                        raise Exception("%s" % response.text)
                else:
                    # find dataset by pid
                    get_logger().info(
                        'DatasetIngestor: Find the dataset by id: %s' % (pid))
                    resds = requests.get(
                        "{url}/{pid}?access_token={token}".format(
                            url=self.__dataseturl,
                            pid=pid.replace("/", "%2F"),
                            token=token))
                    if resds.ok:
                        dsmeta = json.loads(resds.content)
                        mdic = dict(mdct)
                        mdic["pid"] = pid
                        if not self._metadataEqual(
                                dsmeta, mdic, skip=self.__withoutsm):
                            # create a new dataset since
                            # core metadata of dataset were changed
                            # find a new pid
                            pexist = True
                            npid = pid
                            ipid = mdct["pid"]
                            while pexist:
                                spid = npid.split("/")
                                if len(spid) > 3:
                                    try:
                                        ver = int(spid[-1])
                                        spid[-1] = str(ver + 1)
                                    except Exception:
                                        spid.append("2")
                                else:
                                    spid.append("2")
                                npid = "/".join(spid)
                                if len(spid) > 0:
                                    ipid = "/".join(spid[1:])
                                resexists = requests.get(
                                    "{url}/{pid}/exists?access_token={token}"
                                    .format(
                                        url=self.__dataseturl,
                                        pid=npid.replace("/", "%2F"),
                                        token=token))
                                if resexists.ok:
                                    pexist = json.loads(
                                        resexists.content)["exists"]
                                else:
                                    raise Exception("%s" % resexists.text)

                            mdic["pid"] = ipid
                            nmeta = json.dumps(mdic)
                            get_logger().info(
                                'DatasetIngestor: '
                                'Post the dataset with a new pid: %s' % (npid))

                            # post the dataset with the new pid
                            response = requests.post(
                                "%s?access_token=%s"
                                % (self.__dataseturl, token),
                                headers=self.__headers,
                                data=nmeta)
                            if response.ok:
                                return mdic["pid"]
                            else:
                                raise Exception("%s" % response.text)
                        else:
                            if "scientificMetadata" in dsmeta.keys() and \
                               "scientificMetadata" in mdic.keys():
                                smmeta = dsmeta["scientificMetadata"]
                                smnmeta = mdic["scientificMetadata"]
                                nmeta = json.dumps(mdic)
                                if not self._metadataEqual(smmeta, smnmeta):
                                    get_logger().info(
                                        'DatasetIngestor: '
                                        'Patch scientificMetadata of dataset:'
                                        ' %s' % (pid))
                                    response = requests.patch(
                                        "{url}/{pid}?access_token={token}"
                                        .format(
                                            url=self.__dataseturl,
                                            pid=pid.replace("/", "%2F"),
                                            token=token),
                                        headers=self.__headers,
                                        data=nmeta)
                                    if response.ok:
                                        return mdct["pid"]
                                    else:
                                        raise Exception("%s" % response.text)
                    else:
                        raise Exception("%s" % resds.text)
            else:
                raise Exception("%s" % response.text)
        except Exception as e:
            get_logger().error(
                'DatasetIngestor: %s' % (str(e)))
        return None

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

    def _get_pid(self, metafile):
        """ ingest raw dataset metadata

        :param metafile: metadata file name
        :type metafile: :obj:`str
        """
        pid = None
        try:
            with open(metafile) as fl:
                smt = fl.read()
                mt = json.loads(smt)
                pid = mt["pid"]
        except Exception as e:
            get_logger().error(
                'DatasetIngestor: %s' % (str(e)))

        return pid

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
            if not mt["pid"].startswith("%s/" % (self.__bid)):
                raise Exception(
                    "Wrong pid %s for DESY beamtimeId %s in  %s"
                    % (mt["pid"], self.__bid, metafile))
            status = self._ingest_dataset(smt, token, mt)
            if status:
                return status
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
            if not mt["datasetId"].startswith(
                    "%s/%s/" % (self.__doiprefix, self.__bid)):
                raise Exception(
                    "Wrong datasetId %s for DESY beamtimeId %s in  %s"
                    % (mt["pid"], self.__bid, metafile))
            if mt["datasetId"] != "%s/%s" % (self.__doiprefix, pid):
                mt["datasetId"] = "%s/%s" % (self.__doiprefix, pid)
                smt = json.dumps(mt)
                with open(metafile, "w") as mf:
                    mf.write(smt)
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
        mtmds = 0
        if rds:
            mtmds = os.path.getmtime(rds)

        odbs = glob.glob(
            "{scanpath}/{scan}{postfix}.json".format(
                scan=scan, postfix=self.__datablockpostfix,
                scanpath=self.__dctfmt["scanpath"]))
        if odbs and odbs[0]:
            odb = odbs[0]
        else:
            odb = self._generate_origdatablock_metadata(scan)
        mtmdb = 0
        if odb:
            mtmdb = os.path.getmtime(odb)
        dbstatus = None

        pid = None
        if rds and odb:
            if rds and rds[0]:
                pid = self._ingest_rawdataset_metadata(rds, token)
            if odb and odb[0] and pid:
                if pid is None and rdss and rdss[0]:
                    pid = self._get_pid(rdss[0])
                dbstatus = self._ingest_origdatablock_metadata(
                    odb, pid, token)
        if pid is None:
            mtmds = 0
        if dbstatus is None:
            mtmdb = 0
        self.__sc_ingested.append([scan, str(mtmds), str(mtmdb)])
        with open(self.__idsfile, 'a+') as f:
            f.write("%s %s %s\n" % (scan, mtmds, mtmdb))

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
            get_logger().debug("MAP: %s" % (self.__sc_ingested_map))

            if scan in self.__sc_ingested_map.keys():
                get_logger().debug("DS Timestamps: %s %s %s %s" % (
                    scan,
                    mtm, self.__sc_ingested_map[scan][-2],
                    mtm > self.__sc_ingested_map[scan][-2]))
            if scan not in self.__sc_ingested_map.keys() \
               or mtm > self.__sc_ingested_map[scan][-2]:
                reingest_dataset = True
        else:
            rds = self._generate_rawdataset_metadata(scan)
            get_logger().debug("DS No File: %s True" % (scan))
            reingest_dataset = True
        mtmds = 0
        if rds:
            mtmds = os.path.getmtime(rds)

        odbs = glob.glob(
            "{scanpath}/{scan}{postfix}.json".format(
                scan=scan, postfix=self.__datablockpostfix,
                scanpath=self.__dctfmt["scanpath"]))
        if odbs and odbs[0]:
            odb = odbs[0]

            mtm0 = os.path.getmtime(odb)
            if scan not in self.__sc_ingested_map.keys() \
               or mtm0 > self.__sc_ingested_map[scan][-1]:
                reingest_origdatablock = True
            if scan in self.__sc_ingested_map.keys():
                get_logger().debug("DB0 Timestamps: %s %s %s %s %s" % (
                    scan,
                    mtm0, self.__sc_ingested_map[scan][-1],
                    mtm0 - self.__sc_ingested_map[scan][-1],
                    reingest_origdatablock)
                )
            self._regenerate_origdatablock_metadata(
                scan, reingest_origdatablock)
            mtm = os.path.getmtime(odb)

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
        mtmdb = 0
        if odb:
            mtmdb = os.path.getmtime(odb)
        dbstatus = None
        pid = None
        if rds and odb:
            if rds and rds[0] and reingest_dataset:
                pid = self._ingest_rawdataset_metadata(rds, token)
                get_logger().info(
                    "DatasetIngestor: Ingest dataset: %s" % (rds))
                oldpid = self._get_pid(rds)
                if pid and oldpid != pid:
                    # get_logger().info("PID %s %s %s" % (scan,pid,oldpid))
                    odb = self._generate_origdatablock_metadata(scan)
                    reingest_origdatablock = True
            if odb and odb[0] and reingest_origdatablock:
                if pid is None and rdss and rdss[0]:
                    pid = self._get_pid(rdss[0])
                dbstatus = self._ingest_origdatablock_metadata(
                    odb, pid, token)
                get_logger().info(
                    "DatasetIngestor: Ingest origdatablock: %s" % (odb))
        if (pid and reingest_dataset):
            pass
        elif scan in self.__sc_ingested_map.keys():
            mtmds = self.__sc_ingested_map[scan][-2]
        else:
            mtmds = 0
        if (dbstatus and reingest_origdatablock):
            pass
        elif scan in self.__sc_ingested_map.keys():
            mtmdb = self.__sc_ingested_map[scan][-1]
        else:
            mtmdb = 0
        self.__sc_ingested.append([scan, str(mtmds), str(mtmdb)])
        with open(self.__idsfiletmp, 'a+') as f:
            f.write("%s %s %s\n" % (scan, mtmds, mtmdb))

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
                    if len(sc) > 2 and float(sc[-1]) > 0 \
                       and float(sc[-2]) > 0:
                        sc[-1] = float(sc[-1])
                        sc[-2] = float(sc[-2])
                        self.__sc_ingested_map[sc[0]] = sc
                except Exception as e:
                    get_logger().debug("%s" % str(e))

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
