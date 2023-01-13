#!/usr/bin/env python
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
# Authors:
#     Jan Kotanski <jan.kotanski@desy.de>
#

import unittest
import os
import sys
import threading
import shutil
import json
import time
import uuid

from scingestor import datasetIngest


try:
    from .SciCatTestServer import SciCatTestServer, SciCatMockHandler
except Exception:
    from SciCatTestServer import SciCatTestServer, SciCatMockHandler


try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO


def myinput(w, text):
    myio = os.fdopen(w, 'w')
    myio.write(text)
    myio.close()


class mytty(object):

    def __init__(self, underlying):
        #        underlying.encoding = 'cp437'
        self.__underlying = underlying

    def __getattr__(self, name):
        return getattr(self.__underlying, name)

    def isatty(self):
        return True

    def __del__(self):
        self.__underlying.close()


# test fixture
class DatasetIngestTest(unittest.TestCase):

    # constructor
    # \param methodName name of the test method
    def __init__(self, methodName):
        unittest.TestCase.__init__(self, methodName)

        self.maxDiff = None

    def myAssertDict(self, dct, dct2, skip=None, parent=None):
        parent = parent or ""
        self.assertTrue(isinstance(dct, dict))
        self.assertTrue(isinstance(dct2, dict))
        if len(list(dct.keys())) != len(list(dct2.keys())):
            print(list(dct.keys()))
            print(list(dct2.keys()))
        self.assertEqual(
            len(list(dct.keys())), len(list(dct2.keys())))
        for k, v in dct.items():
            if parent:
                node = "%s.%s" % (parent, k)
            else:
                node = k
            if k not in dct2.keys():
                print("%s not in %s" % (k, dct2))
            self.assertTrue(k in dct2.keys())
            if not skip or node not in skip:
                if isinstance(v, dict):
                    self.myAssertDict(v, dct2[k], skip, node)
                else:
                    self.assertEqual(v, dct2[k])

    def setUp(self):
        self.starthttpserver()

    def starthttpserver(self):
        self.__server = SciCatTestServer(('', 8881), SciCatMockHandler)

        self.__thread = threading.Thread(None, self.__server.run)
        self.__thread.start()

    def stophttpserver(self):
        if self.__server is not None:
            self.__server.shutdown()
        if self.__thread is not None:
            self.__thread.join()
        self.__thread = None
        self.__server = None

    def tearDown(self):
        self.stophttpserver()

    def runtest(self, argv, pipeinput=None):
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = mystdout = StringIO()
        sys.stderr = mystderr = StringIO()
        old_argv = sys.argv
        sys.argv = argv

        if pipeinput is not None:
            r, w = os.pipe()
            new_stdin = mytty(os.fdopen(r, 'r'))
            old_stdin, sys.stdin = sys.stdin, new_stdin
            # tm = threading.Timer(1., myinput, [w, pipeinput])
            # tm.start()
        else:
            old_stdin = sys.stdin
            sys.stdin = StringIO()

        etxt = None
        try:
            datasetIngest.main()
        except Exception as e:
            etxt = str(e)
        except SystemExit as e:
            etxt = str(e)
        sys.argv = old_argv

        sys.stdout = old_stdout
        sys.stderr = old_stderr
        sys.stdin = old_stdin
        sys.argv = old_argv
        vl = mystdout.getvalue()
        er = mystderr.getvalue()
        # print(vl)
        # print(er)
        if etxt:
            # print(etxt)
            pass
        # self.assertEqual(etxt, None)
        return vl, er

    def runtestexcept(self, argv, exception):
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        old_stdin = sys.stdin
        sys.stdin = StringIO()
        sys.stdout = mystdout = StringIO()
        sys.stderr = mystderr = StringIO()

        old_argv = sys.argv
        sys.argv = argv
        try:
            error = False
            datasetIngest.main()
        except exception as e:
            etxt = str(e)
            error = True
        self.assertEqual(error, True)

        sys.argv = old_argv

        sys.stdout = old_stdout
        sys.stderr = old_stderr
        sys.stdin = old_stdin
        sys.argv = old_argv
        vl = mystdout.getvalue()
        er = mystderr.getvalue()
        return vl, er, etxt

    def test_datasetfile_exist(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        btmeta = "bt-mt-99001234.jsn"
        dslist = "sc-ds-99001234.lst"
        idslist = "sc-ids-99001234.lst"
        wrongdslist = "sc-ds-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        wlsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                "config",
                                wrongdslist)
        # fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'beamtime_filename_prefix: "bt-mt-"\n' \
            'beamtime_filename_postfix: ".jsn"\n' \
            'datasets_filename_pattern: "sc-ds-{{beamtimeid}}.lst"\n' \
            'ingested_datasets_filename_pattern: ' \
            '"sc-ids-{{beamtimeid}}.lst"\n' \
            'inotify_timeout: 0.2\n' \
            'get_event_timeout: 0.02\n' \
            'ingestion_delay_time: 2\n' \
            'max_request_tries_number: 10\n' \
            'recheck_beamtime_file_interval: 1000\n' \
            'rrecheck_dataset_list_interval: 1000\n' \
            'scicat_url: "{url}"\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [("scicat_dataset_ingest  -c %s"
                     % cfgfname).split(),
                    ("scicat_dataset_ingest --config %s"
                     % cfgfname).split()]
        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                # print(vl)
                # print(er)
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                self.assertEqual(
                    'INFO : DatasetIngest: beamtime path: {basedir}\n'
                    'INFO : DatasetIngest: beamtime file: '
                    'bt-mt-99001234.jsn\n'
                    'INFO : DatasetIngest: dataset list: {dslist}\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc1}\n'
                    'INFO : DatasetIngestor: Generating metadata: '
                    '{sc1} {subdir2}/{sc1}.scan.json\n'
                    'INFO : DatasetIngestor: '
                    'Generating origdatablock metadata:'
                    ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Check if dataset exists: '
                    '/99001234/{sc1}\n'
                    'INFO : DatasetIngestor: Post the dataset: '
                    '/99001234/{sc1}\n'
                    'INFO : DatasetIngestor: Ingest dataset: '
                    '{subdir2}/{sc1}.scan.json\n'
                    'INFO : DatasetIngestor: Ingest origdatablock: '
                    '{subdir2}/{sc1}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc2}\n'
                    'INFO : DatasetIngestor: Generating metadata: '
                    '{sc2} {subdir2}/{sc2}.scan.json\n'
                    'INFO : DatasetIngestor: '
                    'Generating origdatablock metadata:'
                    ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Check if dataset exists: '
                    '/99001234/{sc2}\n'
                    'INFO : DatasetIngestor: Post the dataset: '
                    '/99001234/{sc2}\n'
                    'INFO : DatasetIngestor: Ingest dataset: '
                    '{subdir2}/{sc2}.scan.json\n'
                    'INFO : DatasetIngestor: Ingest origdatablock: '
                    '{subdir2}/{sc2}.origdatablock.json\n'
                    .format(basedir=fdirname,
                            subdir2=fsubdirname2,
                            dslist=fdslist,
                            sc1='myscan_00001', sc2='myscan_00002'),
                    "\n".join(seri))
                self.assertEqual(
                    "Login: ingestor\n"
                    "RawDatasets: 99001234/myscan_00001\n"
                    "OrigDatablocks: /99001234/myscan_00001\n"
                    "RawDatasets: 99001234/myscan_00002\n"
                    "OrigDatablocks: /99001234/myscan_00002\n", vl)
                self.assertEqual(len(self.__server.userslogin), 1)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 2)
                self.myAssertDict(
                    json.loads(self.__server.datasets[0]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'instrumentId': '/petra3/p00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerGroup': '99001234-dmgt',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00001',
                     'datasetName': 'myscan_00001',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[1]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerGroup': '99001234-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00002',
                     'datasetName': 'myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.assertEqual(len(self.__server.origdatablocks), 2)
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[0]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'ownerGroup': '99001234-dmgt',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'datasetId': '/99001234/myscan_00001',
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[1]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001234-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_repeat(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        btmeta = "beamtime-metadata-99001234.json"
        dslist = "scicat-datasets-99001234.lst"
        idslist = "scicat-ingested-datasets-99001234.lst"
        wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        wlsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                "config",
                                wrongdslist)
        # fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingest -c %s'
                     % cfgfname).split(),
                    ('scicat_dataset_ingest --config %s'
                     % cfgfname).split()]
        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)
                vl, er = self.runtest(cmd)
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                # print(er)
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                self.assertEqual(
                    'INFO : DatasetIngest: beamtime path: {basedir}\n'
                    'INFO : DatasetIngest: beamtime file: '
                    'beamtime-metadata-99001234.json\n'
                    'INFO : DatasetIngest: dataset list: {dslist}\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc1}\n'
                    'INFO : DatasetIngestor: Checking origdatablock metadata:'
                    ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc2}\n'
                    'INFO : DatasetIngestor: Checking origdatablock metadata:'
                    ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                    .format(basedir=fdirname,
                            subdir2=fsubdirname2,
                            dslist=fdslist,
                            sc1='myscan_00001', sc2='myscan_00002'),
                    "\n".join(seri))
                self.assertEqual("Login: ingestor\n", vl)
                self.assertEqual(len(self.__server.userslogin), 2)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 2)
                self.myAssertDict(
                    json.loads(self.__server.datasets[0]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerGroup': '99001234-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00001',
                     'datasetName': 'myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[1]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'ownerGroup': '99001234-dmgt',
                     'pid': '99001234/myscan_00002',
                     'datasetName': 'myscan_00002',
                     'principalInvestigator': 'appuser@fake.com',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.assertEqual(len(self.__server.origdatablocks), 2)
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[0]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001234-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[1]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001234-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_touch(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        btmeta = "beamtime-metadata-99001234.json"
        dslist = "scicat-datasets-99001234.lst"
        idslist = "scicat-ingested-datasets-99001234.lst"
        wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        wlsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                "config",
                                wrongdslist)
        # fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'metadata_keywords_without_checks:\n' \
            '  - "techniques"\n' \
            '  - "classification"\n' \
            '  - "createdBy"\n' \
            '  - "updatedBy"\n' \
            '  - "datasetlifecycle"\n' \
            '  - "numberOfFiles"\n' \
            '  - "size"\n' \
            '  - "createdAt"\n' \
            '  - "updatedAt"\n' \
            '  - "history"\n' \
            '  - "creationTime"\n' \
            '  - "version"\n' \
            '  - "scientificMetadata"\n' \
            '  - "endTime"\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingest -c %s'
                     % cfgfname).split(),
                    ('scicat_dataset_ingest --config %s'
                     % cfgfname).split()]
        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)
                # print(vl)
                # print(er)

                dsfname1 = "%s/%s.scan.json" % \
                           (fsubdirname2, 'myscan_00001')
                dbfname2 = "%s/%s.origdatablock.json" % \
                           (fsubdirname2, 'myscan_00002')
                # import time
                # mtmds = os.path.getmtime(dsfname1)
                # mtmdb = os.path.getmtime(dbfname2)
                # print("BEFORE", mtmds, mtmdb)

                # on cenos6 touch modify only timestamps
                # when last modification > 1s
                time.sleep(1.1)
                os.utime(dbfname2)
                os.utime(dsfname1)

                # mtmds = os.path.getmtime(dsfname1)
                # mtmdb = os.path.getmtime(dbfname2)
                # print("AFTER", mtmds, mtmdb)

                vl, er = self.runtest(cmd)
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                # print(vl)
                # print(er)
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                self.assertEqual(
                    'INFO : DatasetIngest: beamtime path: {basedir}\n'
                    'INFO : DatasetIngest: beamtime file: '
                    'beamtime-metadata-99001234.json\n'
                    'INFO : DatasetIngest: dataset list: {dslist}\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc1}\n'
                    'INFO : DatasetIngestor: Checking origdatablock metadata:'
                    ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Check if dataset exists: '
                    '/99001234/{sc1}\n'
                    'INFO : DatasetIngestor: Find the dataset by id: '
                    '/99001234/{sc1}\n'
                    'INFO : DatasetIngestor: Ingest dataset: '
                    '{subdir2}/{sc1}.scan.json\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc2}\n'
                    'INFO : DatasetIngestor: Checking origdatablock metadata:'
                    ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                    'INFO : DatasetIngestor: '
                    'Generating origdatablock metadata:'
                    ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Ingest origdatablock:'
                    ' {subdir2}/{sc2}.origdatablock.json\n'
                    .format(basedir=fdirname,
                            subdir2=fsubdirname2,
                            dslist=fdslist,
                            sc1='myscan_00001', sc2='myscan_00002'),
                    "\n".join(seri))
                self.assertEqual(
                    "Login: ingestor\n"
                    "OrigDatablocks: delete /99001234/myscan_00002\n"
                    "OrigDatablocks: /99001234/myscan_00002\n",
                    vl)
                self.assertEqual(len(self.__server.userslogin), 2)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "ingestor", "password": "12342345"}')
                # self.assertEqual(
                #     self.__server.userslogin[2],
                #     b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 2)
                self.myAssertDict(
                    json.loads(self.__server.datasets[0]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'ownerGroup': '99001234-dmgt',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00001',
                     'datasetName': 'myscan_00001',
                     'principalInvestigator': 'appuser@fake.com',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[1]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'instrumentId': '/petra3/p00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerGroup': '99001234-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00002',
                     'datasetName': 'myscan_00002',
                     'principalInvestigator': 'appuser@fake.com',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.assertEqual(len(self.__server.origdatablocks), 3)
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[0]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001234-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[1]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001234-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[2]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001234-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_jsonchange(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        btmeta = "beamtime-metadata-99001234.json"
        dslist = "scicat-datasets-99001234.lst"
        idslist = "scicat-ingested-datasets-99001234.lst"
        wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        wlsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                "config",
                                wrongdslist)
        # fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingest -c %s'
                     % cfgfname).split(),
                    ('scicat_dataset_ingest --config %s'
                     % cfgfname).split()]
        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)

                vl, er = self.runtest(cmd)

                scfname = "%s/%s.scan.json" % (fsubdirname2, 'myscan_00001')
                odbfname = "%s/%s.origdatablock.json" \
                    % (fsubdirname2, 'myscan_00002')

                scdict = {}
                with open(scfname, "r") as fl:
                    scn = fl.read()
                    scdict = json.loads(scn)
                scdict["owner"] = "NewOwner"
                scdict["contactEmail"] = "new.owner@ggg.gg"
                scdict["techniques"] = [
                   {
                       'name': 'small angle x-ray scattering',
                       'pid':
                       'http://purl.org/pan-science/PaNET/PaNET01188'
                   }
                ]
                with open(scfname, "w") as fl:
                    fl.write(json.dumps(scdict))

                scdict = {}
                with open(odbfname, "r") as fl:
                    scn = fl.read()
                    scdict = json.loads(scn)
                scdict["size"] = 123123
                with open(odbfname, "w") as fl:
                    fl.write(json.dumps(scdict))

                vl, er = self.runtest(cmd)

                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                # print(er)
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                self.assertEqual(
                    'INFO : DatasetIngest: beamtime path: {basedir}\n'
                    'INFO : DatasetIngest: beamtime file: '
                    'beamtime-metadata-99001234.json\n'
                    'INFO : DatasetIngest: dataset list: {dslist}\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc1}\n'
                    'INFO : DatasetIngestor: Checking origdatablock metadata:'
                    ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Check if dataset exists: '
                    '/99001234/{sc1}\n'
                    'INFO : DatasetIngestor: Find the dataset by id: '
                    '/99001234/{sc1}\n'
                    'INFO : DatasetIngestor: '
                    'Patch scientificMetadata of dataset: '
                    '/99001234/{sc1}\n'
                    'INFO : DatasetIngestor: Ingest dataset: '
                    '{subdir2}/{sc1}.scan.json\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc2}\n'
                    'INFO : DatasetIngestor: Checking origdatablock metadata:'
                    ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                    'INFO : DatasetIngestor: '
                    'Generating origdatablock metadata:'
                    ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Ingest origdatablock:'
                    ' {subdir2}/{sc2}.origdatablock.json\n'
                    .format(basedir=fdirname,
                            subdir2=fsubdirname2,
                            dslist=fdslist,
                            sc1='myscan_00001', sc2='myscan_00002'),
                    "\n".join(seri))
                self.assertEqual(
                    "Login: ingestor\n"
                    "RawDatasets: /99001234/myscan_00001\n"
                    "OrigDatablocks: delete /99001234/myscan_00002\n"
                    "OrigDatablocks: /99001234/myscan_00002\n",
                    vl)
                self.assertEqual(len(self.__server.userslogin), 2)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "ingestor", "password": "12342345"}')
                # self.assertEqual(
                #     self.__server.userslogin[2],
                #     b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 3)
                self.myAssertDict(
                    json.loads(self.__server.datasets[0]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerGroup': '99001234-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00001',
                     'datasetName': 'myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[1]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'instrumentId': '/petra3/p00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'owner': 'Smithson',
                     'techniques': [],
                     'ownerEmail': 'peter.smithson@fake.de',
                     'ownerGroup': '99001234-dmgt',
                     'pid': '99001234/myscan_00002',
                     'datasetName': 'myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[2]),
                    {'contactEmail': 'new.owner@ggg.gg',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [{
                         'name': 'small angle x-ray scattering',
                         'pid':
                         'http://purl.org/pan-science/PaNET/PaNET01188'}],
                     'owner': 'NewOwner',
                     'ownerGroup': '99001234-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '/99001234/myscan_00001',
                     'datasetName': 'myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.assertEqual(len(self.__server.origdatablocks), 3)
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[0]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001234-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[1]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001234-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[2]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001234-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_jsonchange_corepath(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirnames = os.path.abspath(os.path.join(dirname, "scratch"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        coredir = "/tmp/scingestor_core_%s" % uuid.uuid4().hex
        cfsubdirname = os.path.abspath(os.path.join(coredir, "raw"))
        cfsubdirnames = os.path.abspath(os.path.join(coredir, "scratch"))
        cfsubdirname2 = os.path.abspath(os.path.join(cfsubdirname, "special"))
        btmeta = "beamtime-metadata-99001284.json"
        fullbtmeta = os.path.join(fdirname, btmeta)
        dslist = "scicat-datasets-99001284.lst"
        idslist = "scicat-ingested-datasets-99001284.lst"
        wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        with open(source) as blf:
            jblm = blf.read()
            blm = json.loads(jblm)
            blm["corePath"] = coredir
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        wlsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                "config",
                                wrongdslist)
        # fullbtmeta = os.path.join(fdirname, btmeta)
        fidslist = os.path.join(fsubdirname2, idslist)
        cfullbtmeta = os.path.join(coredir, btmeta)
        cfdslist = os.path.join(cfsubdirname2, dslist)
        cfidslist = os.path.join(cfsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        os.mkdir(fdirname)
        os.makedirs(coredir, exist_ok=True)
        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        commands = [('scicat_dataset_ingest -c %s'
                     % cfgfname).split(),
                    ('scicat_dataset_ingest --config %s'
                     % cfgfname).split()]
        # commands.pop()
        try:
            for kk, cmd in enumerate(commands):
                with open(credfile, "w") as cf:
                    cf.write(cred)
                if kk % 2:
                    scratchdir = cfsubdirnames
                else:
                    scratchdir = fsubdirnames

                cfg = 'beamtime_dirs:\n' \
                    '  - "{basedir}"\n' \
                    'scicat_url: "{url}"\n' \
                    'scandir_blacklist:\n' \
                    '  - "{scratchdir}"\n' \
                    'use_corepath_as_scandir: true\n' \
                    'ingestor_var_dir: "{vardir}"\n' \
                    'ingestor_credential_file: "{credfile}"\n'.format(
                        scratchdir=scratchdir,
                        basedir=fdirname, url=url,
                        vardir=vardir, credfile=credfile)
                with open(cfgfname, "w+") as cf:
                    cf.write(cfg)

                os.mkdir(fsubdirnames)
                os.mkdir(cfsubdirnames)
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                os.mkdir(cfsubdirname)
                os.mkdir(cfsubdirname2)
                with open(cfullbtmeta, "w") as blf:
                    blf.write(json.dumps(blm))
                with open(fullbtmeta, "w") as blf:
                    blf.write(json.dumps(blm))
                # shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                shutil.copy(lsource, cfsubdirname2)
                shutil.copy(wlsource, cfsubdirname)
                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                if os.path.exists(cfidslist):
                    os.remove(cfidslist)

                vl, er = self.runtest(cmd)

                scfname = "%s/%s.scan.json" % (cfsubdirname2, 'myscan_00001')
                odbfname = "%s/%s.origdatablock.json" \
                    % (cfsubdirname2, 'myscan_00002')

                scdict = {}
                with open(scfname, "r") as fl:
                    scn = fl.read()
                    scdict = json.loads(scn)
                scdict["owner"] = "NewOwner"
                scdict["contactEmail"] = "new.owner@ggg.gg"
                scdict["techniques"] = [
                   {
                       'name': 'small angle x-ray scattering',
                       'pid':
                       'http://purl.org/pan-science/PaNET/PaNET01188'
                   }
                ]
                with open(scfname, "w") as fl:
                    fl.write(json.dumps(scdict))

                scdict = {}
                with open(odbfname, "r") as fl:
                    scn = fl.read()
                    scdict = json.loads(scn)
                scdict["size"] = 123123
                with open(odbfname, "w") as fl:
                    fl.write(json.dumps(scdict))

                vl, er = self.runtest(cmd)

                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                # print(er)
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                self.assertEqual(
                    'INFO : DatasetIngest: beamtime path: {basedir}\n'
                    'INFO : DatasetIngest: beamtime file: '
                    'beamtime-metadata-99001284.json\n'
                    'INFO : DatasetIngest: dataset list: {dslist}\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc1}\n'
                    'INFO : DatasetIngestor: Checking origdatablock metadata:'
                    ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Check if dataset exists: '
                    '/99001284/{sc1}\n'
                    'INFO : DatasetIngestor: Find the dataset by id: '
                    '/99001284/{sc1}\n'
                    'INFO : DatasetIngestor: '
                    'Patch scientificMetadata of dataset: '
                    '/99001284/{sc1}\n'
                    'INFO : DatasetIngestor: Ingest dataset: '
                    '{subdir2}/{sc1}.scan.json\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc2}\n'
                    'INFO : DatasetIngestor: Checking origdatablock metadata:'
                    ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                    'INFO : DatasetIngestor: '
                    'Generating origdatablock metadata:'
                    ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Ingest origdatablock:'
                    ' {subdir2}/{sc2}.origdatablock.json\n'
                    .format(basedir=fdirname,
                            subdir2=cfsubdirname2,
                            dslist=cfdslist,
                            sc1='myscan_00001', sc2='myscan_00002'),
                    "\n".join(seri))
                self.assertEqual(
                    "Login: ingestor\n"
                    "RawDatasets: /99001284/myscan_00001\n"
                    "OrigDatablocks: delete /99001284/myscan_00002\n"
                    "OrigDatablocks: /99001284/myscan_00002\n",
                    vl)
                self.assertEqual(len(self.__server.userslogin), 2)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "ingestor", "password": "12342345"}')
                # self.assertEqual(
                #     self.__server.userslogin[2],
                #     b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 3)
                self.myAssertDict(
                    json.loads(self.__server.datasets[0]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerGroup': '99001284-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001284/myscan_00001',
                     'datasetName': 'myscan_00001',
                     'accessGroups': [
                         '99001284-dmgt', '99001284-clbt', '99001284-part',
                         'p00dmgt', 'p00staff'],
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001284',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001284'},
                     'sourceFolder':
                     '%s/raw/special' % coredir,
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[1]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'owner': 'Smithson',
                     'techniques': [],
                     'ownerEmail': 'peter.smithson@fake.de',
                     'ownerGroup': '99001284-dmgt',
                     'pid': '99001284/myscan_00002',
                     'datasetName': 'myscan_00002',
                     'accessGroups': [
                         '99001284-dmgt', '99001284-clbt', '99001284-part',
                         'p00dmgt', 'p00staff'],
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001284',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001284'},
                     'sourceFolder':
                     '%s/raw/special' % coredir,
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[2]),
                    {'contactEmail': 'new.owner@ggg.gg',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [{
                         'name': 'small angle x-ray scattering',
                         'pid':
                         'http://purl.org/pan-science/PaNET/PaNET01188'}],
                     'owner': 'NewOwner',
                     'ownerGroup': '99001284-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '/99001284/myscan_00001',
                     'datasetName': 'myscan_00001',
                     'accessGroups': [
                         '99001284-dmgt', '99001284-clbt', '99001284-part',
                         'p00dmgt', 'p00staff'],
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001284',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001284'},
                     'sourceFolder':
                     '%s/raw/special' % coredir,
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.assertEqual(len(self.__server.origdatablocks), 3)
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[0]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001284/myscan_00001',
                     'accessGroups': [
                         '99001284-dmgt', '99001284-clbt', '99001284-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001284-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[1]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001284/myscan_00002',
                     'accessGroups': [
                         '99001284-dmgt', '99001284-clbt', '99001284-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001284-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[2]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001284/myscan_00002',
                     'accessGroups': [
                         '99001284-dmgt', '99001284-clbt', '99001284-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001284-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
                if os.path.isdir(cfsubdirname):
                    shutil.rmtree(cfsubdirname)
                if os.path.isdir(fsubdirnames):
                    shutil.rmtree(fsubdirnames)
                if os.path.isdir(cfsubdirnames):
                    shutil.rmtree(cfsubdirnames)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)
            if os.path.isdir(coredir):
                shutil.rmtree(coredir)

    def test_datasetfile_jsonchange_nods(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        btmeta = "beamtime-metadata-99001234.json"
        dslist = "scicat-datasets-99001234.lst"
        idslist = "scicat-ingested-datasets-99001234.lst"
        wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        wlsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                "config",
                                wrongdslist)
        # fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'dataset_update_strategy: "no"\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingest -c %s'
                     % cfgfname).split(),
                    ('scicat_dataset_ingest --config %s'
                     % cfgfname).split()]
        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)

                vl, er = self.runtest(cmd)

                scfname = "%s/%s.scan.json" % (fsubdirname2, 'myscan_00001')
                odbfname = "%s/%s.origdatablock.json" \
                    % (fsubdirname2, 'myscan_00002')

                scdict = {}
                with open(scfname, "r") as fl:
                    scn = fl.read()
                    scdict = json.loads(scn)
                scdict["owner"] = "NewOwner"
                scdict["contactEmail"] = "new.owner@ggg.gg"
                scdict["techniques"] = [
                   {
                       'name': 'small angle x-ray scattering',
                       'pid':
                       'http://purl.org/pan-science/PaNET/PaNET01188'
                   }
                ]
                with open(scfname, "w") as fl:
                    fl.write(json.dumps(scdict))

                scdict = {}
                with open(odbfname, "r") as fl:
                    scn = fl.read()
                    scdict = json.loads(scn)
                scdict["size"] = 123123
                with open(odbfname, "w") as fl:
                    fl.write(json.dumps(scdict))

                vl, er = self.runtest(cmd)

                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                # print(er)
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                self.assertEqual(
                    'INFO : DatasetIngest: beamtime path: {basedir}\n'
                    'INFO : DatasetIngest: beamtime file: '
                    'beamtime-metadata-99001234.json\n'
                    'INFO : DatasetIngest: dataset list: {dslist}\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc1}\n'
                    'INFO : DatasetIngestor: Checking origdatablock metadata:'
                    ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc2}\n'
                    'INFO : DatasetIngestor: Checking origdatablock metadata:'
                    ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                    'INFO : DatasetIngestor: '
                    'Generating origdatablock metadata:'
                    ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Ingest origdatablock:'
                    ' {subdir2}/{sc2}.origdatablock.json\n'
                    .format(basedir=fdirname,
                            subdir2=fsubdirname2,
                            dslist=fdslist,
                            sc1='myscan_00001', sc2='myscan_00002'),
                    "\n".join(seri))
                self.assertEqual(
                    "Login: ingestor\n"
                    "OrigDatablocks: delete /99001234/myscan_00002\n"
                    "OrigDatablocks: /99001234/myscan_00002\n",
                    vl)
                self.assertEqual(len(self.__server.userslogin), 2)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "ingestor", "password": "12342345"}')
                # self.assertEqual(
                #     self.__server.userslogin[2],
                #     b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 2)
                self.myAssertDict(
                    json.loads(self.__server.datasets[0]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerGroup': '99001234-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00001',
                     'datasetName': 'myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[1]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'owner': 'Smithson',
                     'techniques': [],
                     'ownerEmail': 'peter.smithson@fake.de',
                     'ownerGroup': '99001234-dmgt',
                     'pid': '99001234/myscan_00002',
                     'datasetName': 'myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.assertEqual(len(self.__server.origdatablocks), 3)
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[0]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001234-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[1]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001234-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[2]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001234-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_jsonchange_mixed(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        btmeta = "beamtime-metadata-99001234.json"
        dslist = "scicat-datasets-99001234.lst"
        idslist = "scicat-ingested-datasets-99001234.lst"
        wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        wlsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                "config",
                                wrongdslist)
        # fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'dataset_update_strategy: "mixed"\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingest -c %s'
                     % cfgfname).split(),
                    ('scicat_dataset_ingest --config %s'
                     % cfgfname).split()]
        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)

                vl, er = self.runtest(cmd)

                scfname = "%s/%s.scan.json" % (fsubdirname2, 'myscan_00001')
                odbfname = "%s/%s.origdatablock.json" \
                    % (fsubdirname2, 'myscan_00002')

                scdict = {}
                with open(scfname, "r") as fl:
                    scn = fl.read()
                    scdict = json.loads(scn)
                scdict["owner"] = "NewOwner"
                scdict["contactEmail"] = "new.owner@ggg.gg"
                scdict["techniques"] = [
                   {
                       'name': 'small angle x-ray scattering',
                       'pid':
                       'http://purl.org/pan-science/PaNET/PaNET01188'
                   }
                ]
                with open(scfname, "w") as fl:
                    fl.write(json.dumps(scdict))

                scdict = {}
                with open(odbfname, "r") as fl:
                    scn = fl.read()
                    scdict = json.loads(scn)
                scdict["size"] = 123123
                with open(odbfname, "w") as fl:
                    fl.write(json.dumps(scdict))

                vl, er = self.runtest(cmd)

                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                # print(er)
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                self.assertEqual(
                    'INFO : DatasetIngest: beamtime path: {basedir}\n'
                    'INFO : DatasetIngest: beamtime file: '
                    'beamtime-metadata-99001234.json\n'
                    'INFO : DatasetIngest: dataset list: {dslist}\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc1}\n'
                    'INFO : DatasetIngestor: Checking origdatablock metadata:'
                    ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Check if dataset exists: '
                    '/99001234/{sc1}\n'
                    'INFO : DatasetIngestor: Find the dataset by id: '
                    '/99001234/{sc1}\n'
                    'INFO : DatasetIngestor: Post the dataset with a new pid: '
                    '/99001234/{sc1}/2\n'
                    'INFO : DatasetIngestor: Ingest dataset: '
                    '{subdir2}/{sc1}.scan.json\n'
                    'INFO : DatasetIngestor: '
                    'Generating origdatablock metadata:'
                    ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Ingest origdatablock:'
                    ' {subdir2}/{sc1}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc2}\n'
                    'INFO : DatasetIngestor: Checking origdatablock metadata:'
                    ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                    'INFO : DatasetIngestor: '
                    'Generating origdatablock metadata:'
                    ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Ingest origdatablock:'
                    ' {subdir2}/{sc2}.origdatablock.json\n'
                    .format(basedir=fdirname,
                            subdir2=fsubdirname2,
                            dslist=fdslist,
                            sc1='myscan_00001', sc2='myscan_00002'),
                    "\n".join(seri))
                self.assertEqual(
                    "Login: ingestor\n"
                    "RawDatasets: 99001234/myscan_00001/2\n"
                    "OrigDatablocks: /99001234/myscan_00001/2\n"
                    "OrigDatablocks: delete /99001234/myscan_00002\n"
                    "OrigDatablocks: /99001234/myscan_00002\n",
                    vl)
                self.assertEqual(len(self.__server.userslogin), 2)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "ingestor", "password": "12342345"}')
                # self.assertEqual(
                #     self.__server.userslogin[2],
                #     b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 3)
                self.myAssertDict(
                    json.loads(self.__server.datasets[0]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'instrumentId': '/petra3/p00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerGroup': '99001234-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00001',
                     'datasetName': 'myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[1]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'ownerGroup': '99001234-dmgt',
                     'pid': '99001234/myscan_00002',
                     'datasetName': 'myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[2]),
                    {'contactEmail': 'new.owner@ggg.gg',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [{
                         'name': 'small angle x-ray scattering',
                         'pid':
                         'http://purl.org/pan-science/PaNET/PaNET01188'}],
                     'owner': 'NewOwner',
                     'ownerGroup': '99001234-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00001/2',
                     'datasetName': 'myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.assertEqual(len(self.__server.origdatablocks), 4)
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[0]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001234-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[1]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001234-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[2]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001234/myscan_00001/2',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001234-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[3]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001234-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_changefiles(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        btmeta = "beamtime-metadata-99001234.json"
        dslist = "scicat-datasets-99001234.lst"
        idslist = "scicat-ingested-datasets-99001234.lst"
        wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        wlsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                "config",
                                wrongdslist)
        # fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        dfname = "%s/%s.dat" % (fsubdirname2, 'myscan_00002')
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingest -c %s  --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingest --config %s -l debug'
                     % cfgfname).split()]
        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)

                with open(dfname, "w") as fl:
                    fl.write("sdfsdfs\n")

                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)

                # import time
                # time.sleep(0.1)
                scfname2 = "%s/%s.scan.json" % (fsubdirname2, 'myscan_00002')
                odbfname2 = "%s/%s.origdatablock.json" \
                    % (fsubdirname2, 'myscan_00002')

                scdict = {}
                with open(scfname2, "r") as fl:
                    scn = fl.read()
                    scdict = json.loads(scn)
                scdict["owner"] = "NewOwner"
                scdict["contactEmail"] = "new.owner@ggg.gg"

                with open(scfname2, "w") as fl:
                    fl.write(json.dumps(scdict))
                with open(dfname, "w") as fl:
                    fl.write("sdfsfsdfsdfs\n")

                scdict = {}
                with open(odbfname2, "r") as fl:
                    scn = fl.read()
                    scdict = json.loads(scn)

                #    print(scn)
                # scdict["size"] = 123123
                # with open(odbfname2, "w") as fl:
                #     fl.write(json.dumps(scdict))

                vl, er = self.runtest(cmd)

                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                nodebug = "\n".join([ee for ee in seri
                                     if "DEBUG :" not in ee])
                # print(vl)
                try:
                    # print(er)
                    # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                    self.assertEqual(
                        'INFO : DatasetIngest: beamtime path: {basedir}\n'
                        'INFO : DatasetIngest: beamtime file: '
                        'beamtime-metadata-99001234.json\n'
                        'INFO : DatasetIngest: dataset list: {dslist}\n'
                        'INFO : DatasetIngestor: Checking: {dslist} {sc1}\n'
                        'INFO : DatasetIngestor: '
                        'Checking origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        # 'INFO : DatasetIngestor: Ingest dataset: '
                        # '{subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: Checking: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: '
                        'Checking origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Find the dataset by id: '
                        '/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: '
                        'Patch scientificMetadata of dataset: '
                        '/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Ingest dataset: '
                        '{subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: Ingest origdatablock:'
                        ' {subdir2}/{sc2}.origdatablock.json\n'
                        .format(basedir=fdirname,
                                subdir2=fsubdirname2,
                                dslist=fdslist,
                                sc1='myscan_00001', sc2='myscan_00002'),
                        nodebug)
                    self.assertEqual(
                        "Login: ingestor\n"
                        "RawDatasets: /99001234/myscan_00002\n"
                        "OrigDatablocks: delete /99001234/myscan_00002\n"
                        "OrigDatablocks: /99001234/myscan_00002\n",
                        vl)
                    self.assertEqual(len(self.__server.userslogin), 2)
                    self.assertEqual(
                        self.__server.userslogin[0],
                        b'{"username": "ingestor", "password": "12342345"}')
                    self.assertEqual(
                        self.__server.userslogin[1],
                        b'{"username": "ingestor", "password": "12342345"}')
                    # self.assertEqual(
                    #     self.__server.userslogin[2],
                    #     b'{"username": "ingestor", "password": "12342345"}')
                    self.assertEqual(len(self.__server.datasets), 3)
                    self.myAssertDict(
                        json.loads(self.__server.datasets[0]),
                        {'contactEmail': 'appuser@fake.com',
                         'createdAt': '2022-05-14 11:54:29',
                         'instrumentId': '/petra3/p00',
                         'creationLocation': '/DESY/PETRA III/P00',
                         'description': 'H20 distribution',
                         'endTime': '2022-05-19 09:00:00',
                         'isPublished': False,
                         'techniques': [],
                         'owner': 'Smithson',
                         'ownerGroup': '99001234-dmgt',
                         'ownerEmail': 'peter.smithson@fake.de',
                         'pid': '99001234/myscan_00001',
                         'datasetName': 'myscan_00001',
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'principalInvestigator': 'appuser@fake.com',
                         'proposalId': '99001234',
                         'scientificMetadata': {
                             'DOOR_proposalId': '99991173',
                             'beamtimeId': '99001234'},
                         'sourceFolder':
                         '/asap3/petra3/gpfs/p00/2022/data/9901234/'
                         'raw/special',
                         'type': 'raw',
                         'updatedAt': '2022-05-14 11:54:29'})
                    self.myAssertDict(
                        json.loads(self.__server.datasets[1]),
                        {'contactEmail': 'appuser@fake.com',
                         'createdAt': '2022-05-14 11:54:29',
                         'instrumentId': '/petra3/p00',
                         'creationLocation': '/DESY/PETRA III/P00',
                         'description': 'H20 distribution',
                         'endTime': '2022-05-19 09:00:00',
                         'ownerGroup': '99001234-dmgt',
                         'isPublished': False,
                         'techniques': [],
                         'owner': 'Smithson',
                         'ownerEmail': 'peter.smithson@fake.de',
                         'pid': '99001234/myscan_00002',
                         'datasetName': 'myscan_00002',
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'principalInvestigator': 'appuser@fake.com',
                         'proposalId': '99001234',
                         'scientificMetadata': {
                             'DOOR_proposalId': '99991173',
                             'beamtimeId': '99001234'},
                         'sourceFolder':
                         '/asap3/petra3/gpfs/p00/2022/data/9901234/'
                         'raw/special',
                         'type': 'raw',
                         'updatedAt': '2022-05-14 11:54:29'})
                    self.myAssertDict(
                        json.loads(self.__server.datasets[2]),
                        {'contactEmail': 'new.owner@ggg.gg',
                         'createdAt': '2022-05-14 11:54:29',
                         'instrumentId': '/petra3/p00',
                         'creationLocation': '/DESY/PETRA III/P00',
                         'description': 'H20 distribution',
                         'endTime': '2022-05-19 09:00:00',
                         'ownerGroup': '99001234-dmgt',
                         'isPublished': False,
                         'techniques': [],
                         'owner': 'NewOwner',
                         'ownerEmail': 'peter.smithson@fake.de',
                         'pid': '/99001234/myscan_00002',
                         'datasetName': 'myscan_00002',
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'principalInvestigator': 'appuser@fake.com',
                         'proposalId': '99001234',
                         'scientificMetadata': {
                             'DOOR_proposalId': '99991173',
                             'beamtimeId': '99001234'},
                         'sourceFolder':
                         '/asap3/petra3/gpfs/p00/2022/data/9901234/'
                         'raw/special',
                         'type': 'raw',
                         'updatedAt': '2022-05-14 11:54:29'})
                    self.assertEqual(len(self.__server.origdatablocks), 3)
                    self.myAssertDict(
                        json.loads(self.__server.origdatablocks[0]),
                        {'dataFileList': [
                            {'gid': 'jkotan',
                             'path': 'myscan_00001.scan.json',
                             'perm': '-rw-r--r--',
                             'size': 629,
                             'time': '2022-07-05T19:07:16.683673+0200',
                             'uid': 'jkotan'}],
                         'datasetId': '/99001234/myscan_00001',
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'ownerGroup': '99001234-dmgt',
                         'size': 629}, skip=["dataFileList", "size"])
                    self.myAssertDict(
                        json.loads(self.__server.origdatablocks[1]),
                        {'dataFileList': [
                            {'gid': 'jkotan',
                             'path': 'myscan_00001.scan.json',
                             'perm': '-rw-r--r--',
                             'size': 629,
                             'time': '2022-07-05T19:07:16.683673+0200',
                             'uid': 'jkotan'}],
                         'datasetId': '/99001234/myscan_00002',
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'ownerGroup': '99001234-dmgt',
                         'size': 629}, skip=["dataFileList", "size"])
                    self.myAssertDict(
                        json.loads(self.__server.origdatablocks[2]),
                        {'dataFileList': [
                            {'gid': 'jkotan',
                             'path': 'myscan_00001.scan.json',
                             'perm': '-rw-r--r--',
                             'size': 629,
                             'time': '2022-07-05T19:07:16.683673+0200',
                             'uid': 'jkotan'}],
                         'datasetId': '/99001234/myscan_00002',
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'ownerGroup': '99001234-dmgt',
                         'size': 629}, skip=["dataFileList", "size"])
                except Exception:
                    print(er)
                    raise
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
                if os.path.exists(dfname):
                    os.remove(dfname)
        finally:
            pass
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_changefiles_nods(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        btmeta = "beamtime-metadata-99001234.json"
        dslist = "scicat-datasets-99001234.lst"
        idslist = "scicat-ingested-datasets-99001234.lst"
        wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        wlsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                "config",
                                wrongdslist)
        # fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        dfname = "%s/%s.dat" % (fsubdirname2, 'myscan_00002')
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'dataset_update_strategy: "no"\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingest -c %s  --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingest --config %s -l debug'
                     % cfgfname).split()]
        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)

                with open(dfname, "w") as fl:
                    fl.write("sdfsdfs\n")

                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)

                # import time
                # time.sleep(0.1)
                scfname2 = "%s/%s.scan.json" % (fsubdirname2, 'myscan_00002')
                odbfname2 = "%s/%s.origdatablock.json" \
                    % (fsubdirname2, 'myscan_00002')

                scdict = {}
                with open(scfname2, "r") as fl:
                    scn = fl.read()
                    scdict = json.loads(scn)
                scdict["owner"] = "NewOwner"
                scdict["contactEmail"] = "new.owner@ggg.gg"

                with open(scfname2, "w") as fl:
                    fl.write(json.dumps(scdict))
                with open(dfname, "w") as fl:
                    fl.write("sdfsfsdfsdfs\n")

                scdict = {}
                with open(odbfname2, "r") as fl:
                    scn = fl.read()
                    scdict = json.loads(scn)

                #    print(scn)
                # scdict["size"] = 123123
                # with open(odbfname2, "w") as fl:
                #     fl.write(json.dumps(scdict))

                vl, er = self.runtest(cmd)

                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                nodebug = "\n".join([ee for ee in seri
                                     if "DEBUG :" not in ee])
                # print(vl)
                try:
                    # print(er)
                    # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                    self.assertEqual(
                        'INFO : DatasetIngest: beamtime path: {basedir}\n'
                        'INFO : DatasetIngest: beamtime file: '
                        'beamtime-metadata-99001234.json\n'
                        'INFO : DatasetIngest: dataset list: {dslist}\n'
                        'INFO : DatasetIngestor: Checking: {dslist} {sc1}\n'
                        'INFO : DatasetIngestor: '
                        'Checking origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        # 'INFO : DatasetIngestor: Ingest dataset: '
                        # '{subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: Checking: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: '
                        'Checking origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Ingest origdatablock:'
                        ' {subdir2}/{sc2}.origdatablock.json\n'
                        .format(basedir=fdirname,
                                subdir2=fsubdirname2,
                                dslist=fdslist,
                                sc1='myscan_00001', sc2='myscan_00002'),
                        nodebug)
                    self.assertEqual(
                        "Login: ingestor\n"
                        "OrigDatablocks: delete /99001234/myscan_00002\n"
                        "OrigDatablocks: /99001234/myscan_00002\n",
                        vl)
                    self.assertEqual(len(self.__server.userslogin), 2)
                    self.assertEqual(
                        self.__server.userslogin[0],
                        b'{"username": "ingestor", "password": "12342345"}')
                    self.assertEqual(
                        self.__server.userslogin[1],
                        b'{"username": "ingestor", "password": "12342345"}')
                    # self.assertEqual(
                    #     self.__server.userslogin[2],
                    #     b'{"username": "ingestor", "password": "12342345"}')
                    self.assertEqual(len(self.__server.datasets), 2)
                    self.myAssertDict(
                        json.loads(self.__server.datasets[0]),
                        {'contactEmail': 'appuser@fake.com',
                         'createdAt': '2022-05-14 11:54:29',
                         'instrumentId': '/petra3/p00',
                         'creationLocation': '/DESY/PETRA III/P00',
                         'description': 'H20 distribution',
                         'endTime': '2022-05-19 09:00:00',
                         'isPublished': False,
                         'techniques': [],
                         'owner': 'Smithson',
                         'ownerGroup': '99001234-dmgt',
                         'ownerEmail': 'peter.smithson@fake.de',
                         'pid': '99001234/myscan_00001',
                         'datasetName': 'myscan_00001',
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'principalInvestigator': 'appuser@fake.com',
                         'proposalId': '99001234',
                         'scientificMetadata': {
                             'DOOR_proposalId': '99991173',
                             'beamtimeId': '99001234'},
                         'sourceFolder':
                         '/asap3/petra3/gpfs/p00/2022/data/9901234/'
                         'raw/special',
                         'type': 'raw',
                         'updatedAt': '2022-05-14 11:54:29'})
                    self.myAssertDict(
                        json.loads(self.__server.datasets[1]),
                        {'contactEmail': 'appuser@fake.com',
                         'createdAt': '2022-05-14 11:54:29',
                         'instrumentId': '/petra3/p00',
                         'creationLocation': '/DESY/PETRA III/P00',
                         'description': 'H20 distribution',
                         'endTime': '2022-05-19 09:00:00',
                         'ownerGroup': '99001234-dmgt',
                         'isPublished': False,
                         'techniques': [],
                         'owner': 'Smithson',
                         'ownerEmail': 'peter.smithson@fake.de',
                         'pid': '99001234/myscan_00002',
                         'datasetName': 'myscan_00002',
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'principalInvestigator': 'appuser@fake.com',
                         'proposalId': '99001234',
                         'scientificMetadata': {
                             'DOOR_proposalId': '99991173',
                             'beamtimeId': '99001234'},
                         'sourceFolder':
                         '/asap3/petra3/gpfs/p00/2022/data/9901234/'
                         'raw/special',
                         'type': 'raw',
                         'updatedAt': '2022-05-14 11:54:29'})
                    self.assertEqual(len(self.__server.origdatablocks), 3)
                    self.myAssertDict(
                        json.loads(self.__server.origdatablocks[0]),
                        {'dataFileList': [
                            {'gid': 'jkotan',
                             'path': 'myscan_00001.scan.json',
                             'perm': '-rw-r--r--',
                             'size': 629,
                             'time': '2022-07-05T19:07:16.683673+0200',
                             'uid': 'jkotan'}],
                         'datasetId': '/99001234/myscan_00001',
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'ownerGroup': '99001234-dmgt',
                         'size': 629}, skip=["dataFileList", "size"])
                    self.myAssertDict(
                        json.loads(self.__server.origdatablocks[1]),
                        {'dataFileList': [
                            {'gid': 'jkotan',
                             'path': 'myscan_00001.scan.json',
                             'perm': '-rw-r--r--',
                             'size': 629,
                             'time': '2022-07-05T19:07:16.683673+0200',
                             'uid': 'jkotan'}],
                         'datasetId': '/99001234/myscan_00002',
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'ownerGroup': '99001234-dmgt',
                         'size': 629}, skip=["dataFileList", "size"])
                    self.myAssertDict(
                        json.loads(self.__server.origdatablocks[2]),
                        {'dataFileList': [
                            {'gid': 'jkotan',
                             'path': 'myscan_00001.scan.json',
                             'perm': '-rw-r--r--',
                             'size': 629,
                             'time': '2022-07-05T19:07:16.683673+0200',
                             'uid': 'jkotan'}],
                         'datasetId': '/99001234/myscan_00002',
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'ownerGroup': '99001234-dmgt',
                         'size': 629}, skip=["dataFileList", "size"])
                except Exception:
                    print(er)
                    raise
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
                if os.path.exists(dfname):
                    os.remove(dfname)
        finally:
            pass
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_changefiles_mixed(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        btmeta = "beamtime-metadata-99001234.json"
        dslist = "scicat-datasets-99001234.lst"
        idslist = "scicat-ingested-datasets-99001234.lst"
        wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        wlsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                "config",
                                wrongdslist)
        # fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        dfname = "%s/%s.dat" % (fsubdirname2, 'myscan_00002')
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'dataset_update_strategy: "mixed"\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingest -c %s  --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingest --config %s -l debug'
                     % cfgfname).split()]
        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)

                with open(dfname, "w") as fl:
                    fl.write("sdfsdfs\n")

                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)

                # import time
                # time.sleep(0.1)
                scfname2 = "%s/%s.scan.json" % (fsubdirname2, 'myscan_00002')
                odbfname2 = "%s/%s.origdatablock.json" \
                    % (fsubdirname2, 'myscan_00002')

                scdict = {}
                with open(scfname2, "r") as fl:
                    scn = fl.read()
                    scdict = json.loads(scn)
                scdict["owner"] = "NewOwner"
                scdict["contactEmail"] = "new.owner@ggg.gg"

                with open(scfname2, "w") as fl:
                    fl.write(json.dumps(scdict))
                with open(dfname, "w") as fl:
                    fl.write("sdfsfsdfsdfs\n")

                scdict = {}
                with open(odbfname2, "r") as fl:
                    scn = fl.read()
                    scdict = json.loads(scn)

                #    print(scn)
                # scdict["size"] = 123123
                # with open(odbfname2, "w") as fl:
                #     fl.write(json.dumps(scdict))

                vl, er = self.runtest(cmd)

                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                nodebug = "\n".join([ee for ee in seri
                                     if "DEBUG :" not in ee])
                # print(vl)
                try:
                    # print(er)
                    # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                    self.assertEqual(
                        'INFO : DatasetIngest: beamtime path: {basedir}\n'
                        'INFO : DatasetIngest: beamtime file: '
                        'beamtime-metadata-99001234.json\n'
                        'INFO : DatasetIngest: dataset list: {dslist}\n'
                        'INFO : DatasetIngestor: Checking: {dslist} {sc1}\n'
                        'INFO : DatasetIngestor: '
                        'Checking origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        # 'INFO : DatasetIngestor: Ingest dataset: '
                        # '{subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: Checking: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: '
                        'Checking origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Find the dataset by id: '
                        '/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: '
                        'Post the dataset with a new pid: '
                        '/99001234/{sc2}/2\n'
                        'INFO : DatasetIngestor: Ingest dataset: '
                        '{subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Ingest origdatablock:'
                        ' {subdir2}/{sc2}.origdatablock.json\n'
                        .format(basedir=fdirname,
                                subdir2=fsubdirname2,
                                dslist=fdslist,
                                sc1='myscan_00001', sc2='myscan_00002'),
                        nodebug)
                    self.assertEqual(
                        "Login: ingestor\n"
                        "RawDatasets: 99001234/myscan_00002/2\n"
                        "OrigDatablocks: /99001234/myscan_00002/2\n",
                        vl)
                    self.assertEqual(len(self.__server.userslogin), 2)
                    self.assertEqual(
                        self.__server.userslogin[0],
                        b'{"username": "ingestor", "password": "12342345"}')
                    self.assertEqual(
                        self.__server.userslogin[1],
                        b'{"username": "ingestor", "password": "12342345"}')
                    # self.assertEqual(
                    #     self.__server.userslogin[2],
                    #     b'{"username": "ingestor", "password": "12342345"}')
                    self.assertEqual(len(self.__server.datasets), 3)
                    self.myAssertDict(
                        json.loads(self.__server.datasets[0]),
                        {'contactEmail': 'appuser@fake.com',
                         'createdAt': '2022-05-14 11:54:29',
                         'instrumentId': '/petra3/p00',
                         'creationLocation': '/DESY/PETRA III/P00',
                         'description': 'H20 distribution',
                         'endTime': '2022-05-19 09:00:00',
                         'isPublished': False,
                         'techniques': [],
                         'owner': 'Smithson',
                         'ownerGroup': '99001234-dmgt',
                         'ownerEmail': 'peter.smithson@fake.de',
                         'pid': '99001234/myscan_00001',
                         'datasetName': 'myscan_00001',
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'principalInvestigator': 'appuser@fake.com',
                         'proposalId': '99001234',
                         'scientificMetadata': {
                             'DOOR_proposalId': '99991173',
                             'beamtimeId': '99001234'},
                         'sourceFolder':
                         '/asap3/petra3/gpfs/p00/2022/data/9901234/'
                         'raw/special',
                         'type': 'raw',
                         'updatedAt': '2022-05-14 11:54:29'})
                    self.myAssertDict(
                        json.loads(self.__server.datasets[1]),
                        {'contactEmail': 'appuser@fake.com',
                         'createdAt': '2022-05-14 11:54:29',
                         'instrumentId': '/petra3/p00',
                         'creationLocation': '/DESY/PETRA III/P00',
                         'description': 'H20 distribution',
                         'endTime': '2022-05-19 09:00:00',
                         'ownerGroup': '99001234-dmgt',
                         'isPublished': False,
                         'techniques': [],
                         'owner': 'Smithson',
                         'ownerEmail': 'peter.smithson@fake.de',
                         'pid': '99001234/myscan_00002',
                         'datasetName': 'myscan_00002',
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'principalInvestigator': 'appuser@fake.com',
                         'proposalId': '99001234',
                         'scientificMetadata': {
                             'DOOR_proposalId': '99991173',
                             'beamtimeId': '99001234'},
                         'sourceFolder':
                         '/asap3/petra3/gpfs/p00/2022/data/9901234/'
                         'raw/special',
                         'type': 'raw',
                         'updatedAt': '2022-05-14 11:54:29'})
                    self.myAssertDict(
                        json.loads(self.__server.datasets[2]),
                        {'contactEmail': 'new.owner@ggg.gg',
                         'createdAt': '2022-05-14 11:54:29',
                         'instrumentId': '/petra3/p00',
                         'creationLocation': '/DESY/PETRA III/P00',
                         'description': 'H20 distribution',
                         'endTime': '2022-05-19 09:00:00',
                         'ownerGroup': '99001234-dmgt',
                         'isPublished': False,
                         'techniques': [],
                         'owner': 'NewOwner',
                         'ownerEmail': 'peter.smithson@fake.de',
                         'pid': '99001234/myscan_00002/2',
                         'datasetName': 'myscan_00002',
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'principalInvestigator': 'appuser@fake.com',
                         'proposalId': '99001234',
                         'scientificMetadata': {
                             'DOOR_proposalId': '99991173',
                             'beamtimeId': '99001234'},
                         'sourceFolder':
                         '/asap3/petra3/gpfs/p00/2022/data/9901234/'
                         'raw/special',
                         'type': 'raw',
                         'updatedAt': '2022-05-14 11:54:29'})
                    self.assertEqual(len(self.__server.origdatablocks), 3)
                    self.myAssertDict(
                        json.loads(self.__server.origdatablocks[0]),
                        {'dataFileList': [
                            {'gid': 'jkotan',
                             'path': 'myscan_00001.scan.json',
                             'perm': '-rw-r--r--',
                             'size': 629,
                             'time': '2022-07-05T19:07:16.683673+0200',
                             'uid': 'jkotan'}],
                         'datasetId': '/99001234/myscan_00001',
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'ownerGroup': '99001234-dmgt',
                         'size': 629}, skip=["dataFileList", "size"])
                    self.myAssertDict(
                        json.loads(self.__server.origdatablocks[1]),
                        {'dataFileList': [
                            {'gid': 'jkotan',
                             'path': 'myscan_00001.scan.json',
                             'perm': '-rw-r--r--',
                             'size': 629,
                             'time': '2022-07-05T19:07:16.683673+0200',
                             'uid': 'jkotan'}],
                         'datasetId': '/99001234/myscan_00002',
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'ownerGroup': '99001234-dmgt',
                         'size': 629}, skip=["dataFileList", "size"])
                    self.myAssertDict(
                        json.loads(self.__server.origdatablocks[2]),
                        {'dataFileList': [
                            {'gid': 'jkotan',
                             'path': 'myscan_00001.scan.json',
                             'perm': '-rw-r--r--',
                             'size': 629,
                             'time': '2022-07-05T19:07:16.683673+0200',
                             'uid': 'jkotan'}],
                         'datasetId': '/99001234/myscan_00002/2',
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'ownerGroup': '99001234-dmgt',
                         'size': 629}, skip=["dataFileList", "size"])
                except Exception:
                    print(er)
                    raise
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
                if os.path.exists(dfname):
                    os.remove(dfname)
        finally:
            pass
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_changefiles_create(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        btmeta = "beamtime-metadata-99001234.json"
        dslist = "scicat-datasets-99001234.lst"
        idslist = "scicat-ingested-datasets-99001234.lst"
        wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        wlsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                "config",
                                wrongdslist)
        # fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        dfname = "%s/%s.dat" % (fsubdirname2, 'myscan_00002')
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'dataset_update_strategy: "create"\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingest -c %s  --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingest --config %s -l debug'
                     % cfgfname).split()]
        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)

                with open(dfname, "w") as fl:
                    fl.write("sdfsdfs\n")

                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)

                # import time
                # time.sleep(0.1)
                scfname2 = "%s/%s.scan.json" % (fsubdirname2, 'myscan_00002')
                odbfname2 = "%s/%s.origdatablock.json" \
                    % (fsubdirname2, 'myscan_00002')

                scdict = {}
                with open(scfname2, "r") as fl:
                    scn = fl.read()
                    scdict = json.loads(scn)
                scdict["owner"] = "NewOwner"
                scdict["contactEmail"] = "new.owner@ggg.gg"

                with open(scfname2, "w") as fl:
                    fl.write(json.dumps(scdict))
                with open(dfname, "w") as fl:
                    fl.write("sdfsfsdfsdfs\n")

                scdict = {}
                with open(odbfname2, "r") as fl:
                    scn = fl.read()
                    scdict = json.loads(scn)

                #    print(scn)
                # scdict["size"] = 123123
                # with open(odbfname2, "w") as fl:
                #     fl.write(json.dumps(scdict))

                vl, er = self.runtest(cmd)

                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                nodebug = "\n".join([ee for ee in seri
                                     if "DEBUG :" not in ee])
                # print(vl)
                try:
                    # print(er)
                    # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                    self.assertEqual(
                        'INFO : DatasetIngest: beamtime path: {basedir}\n'
                        'INFO : DatasetIngest: beamtime file: '
                        'beamtime-metadata-99001234.json\n'
                        'INFO : DatasetIngest: dataset list: {dslist}\n'
                        'INFO : DatasetIngestor: Checking: {dslist} {sc1}\n'
                        'INFO : DatasetIngestor: '
                        'Checking origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        # 'INFO : DatasetIngestor: Ingest dataset: '
                        # '{subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: Checking: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: '
                        'Checking origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Find the dataset by id: '
                        '/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: '
                        'Post the dataset with a new pid: '
                        '/99001234/{sc2}/2\n'
                        'INFO : DatasetIngestor: Ingest dataset: '
                        '{subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Ingest origdatablock:'
                        ' {subdir2}/{sc2}.origdatablock.json\n'
                        .format(basedir=fdirname,
                                subdir2=fsubdirname2,
                                dslist=fdslist,
                                sc1='myscan_00001', sc2='myscan_00002'),
                        nodebug)
                    self.assertEqual(
                        "Login: ingestor\n"
                        "RawDatasets: 99001234/myscan_00002/2\n"
                        "OrigDatablocks: /99001234/myscan_00002/2\n",
                        vl)
                    self.assertEqual(len(self.__server.userslogin), 2)
                    self.assertEqual(
                        self.__server.userslogin[0],
                        b'{"username": "ingestor", "password": "12342345"}')
                    self.assertEqual(
                        self.__server.userslogin[1],
                        b'{"username": "ingestor", "password": "12342345"}')
                    # self.assertEqual(
                    #     self.__server.userslogin[2],
                    #     b'{"username": "ingestor", "password": "12342345"}')
                    self.assertEqual(len(self.__server.datasets), 3)
                    self.myAssertDict(
                        json.loads(self.__server.datasets[0]),
                        {'contactEmail': 'appuser@fake.com',
                         'createdAt': '2022-05-14 11:54:29',
                         'instrumentId': '/petra3/p00',
                         'creationLocation': '/DESY/PETRA III/P00',
                         'description': 'H20 distribution',
                         'endTime': '2022-05-19 09:00:00',
                         'isPublished': False,
                         'techniques': [],
                         'owner': 'Smithson',
                         'ownerGroup': '99001234-dmgt',
                         'ownerEmail': 'peter.smithson@fake.de',
                         'pid': '99001234/myscan_00001',
                         'datasetName': 'myscan_00001',
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'principalInvestigator': 'appuser@fake.com',
                         'proposalId': '99001234',
                         'scientificMetadata': {
                             'DOOR_proposalId': '99991173',
                             'beamtimeId': '99001234'},
                         'sourceFolder':
                         '/asap3/petra3/gpfs/p00/2022/data/9901234/'
                         'raw/special',
                         'type': 'raw',
                         'updatedAt': '2022-05-14 11:54:29'})
                    self.myAssertDict(
                        json.loads(self.__server.datasets[1]),
                        {'contactEmail': 'appuser@fake.com',
                         'createdAt': '2022-05-14 11:54:29',
                         'instrumentId': '/petra3/p00',
                         'creationLocation': '/DESY/PETRA III/P00',
                         'description': 'H20 distribution',
                         'endTime': '2022-05-19 09:00:00',
                         'ownerGroup': '99001234-dmgt',
                         'techniques': [],
                         'isPublished': False,
                         'owner': 'Smithson',
                         'ownerEmail': 'peter.smithson@fake.de',
                         'pid': '99001234/myscan_00002',
                         'datasetName': 'myscan_00002',
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'principalInvestigator': 'appuser@fake.com',
                         'proposalId': '99001234',
                         'scientificMetadata': {
                             'DOOR_proposalId': '99991173',
                             'beamtimeId': '99001234'},
                         'sourceFolder':
                         '/asap3/petra3/gpfs/p00/2022/data/9901234/'
                         'raw/special',
                         'type': 'raw',
                         'updatedAt': '2022-05-14 11:54:29'})
                    self.myAssertDict(
                        json.loads(self.__server.datasets[2]),
                        {'contactEmail': 'new.owner@ggg.gg',
                         'createdAt': '2022-05-14 11:54:29',
                         'instrumentId': '/petra3/p00',
                         'creationLocation': '/DESY/PETRA III/P00',
                         'description': 'H20 distribution',
                         'endTime': '2022-05-19 09:00:00',
                         'ownerGroup': '99001234-dmgt',
                         'isPublished': False,
                         'techniques': [],
                         'owner': 'NewOwner',
                         'ownerEmail': 'peter.smithson@fake.de',
                         'pid': '99001234/myscan_00002/2',
                         'datasetName': 'myscan_00002',
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'principalInvestigator': 'appuser@fake.com',
                         'proposalId': '99001234',
                         'scientificMetadata': {
                             'DOOR_proposalId': '99991173',
                             'beamtimeId': '99001234'},
                         'sourceFolder':
                         '/asap3/petra3/gpfs/p00/2022/data/9901234/'
                         'raw/special',
                         'type': 'raw',
                         'updatedAt': '2022-05-14 11:54:29'})
                    self.assertEqual(len(self.__server.origdatablocks), 3)
                    self.myAssertDict(
                        json.loads(self.__server.origdatablocks[0]),
                        {'dataFileList': [
                            {'gid': 'jkotan',
                             'path': 'myscan_00001.scan.json',
                             'perm': '-rw-r--r--',
                             'size': 629,
                             'time': '2022-07-05T19:07:16.683673+0200',
                             'uid': 'jkotan'}],
                         'datasetId': '/99001234/myscan_00001',
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'ownerGroup': '99001234-dmgt',
                         'size': 629}, skip=["dataFileList", "size"])
                    self.myAssertDict(
                        json.loads(self.__server.origdatablocks[1]),
                        {'dataFileList': [
                            {'gid': 'jkotan',
                             'path': 'myscan_00001.scan.json',
                             'perm': '-rw-r--r--',
                             'size': 629,
                             'time': '2022-07-05T19:07:16.683673+0200',
                             'uid': 'jkotan'}],
                         'datasetId': '/99001234/myscan_00002',
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'ownerGroup': '99001234-dmgt',
                         'size': 629}, skip=["dataFileList", "size"])
                    self.myAssertDict(
                        json.loads(self.__server.origdatablocks[2]),
                        {'dataFileList': [
                            {'gid': 'jkotan',
                             'path': 'myscan_00001.scan.json',
                             'perm': '-rw-r--r--',
                             'size': 629,
                             'time': '2022-07-05T19:07:16.683673+0200',
                             'uid': 'jkotan'}],
                         'datasetId': '/99001234/myscan_00002/2',
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'ownerGroup': '99001234-dmgt',
                         'size': 629}, skip=["dataFileList", "size"])
                except Exception:
                    print(er)
                    raise
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
                if os.path.exists(dfname):
                    os.remove(dfname)
        finally:
            pass
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_change_scientific_metadata(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        btmeta = "beamtime-metadata-99001234.json"
        dslist = "scicat-datasets-99001234.lst"
        idslist = "scicat-ingested-datasets-99001234.lst"
        wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        wlsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                "config",
                                wrongdslist)
        # fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingest -c %s'
                     % cfgfname).split(),
                    ('scicat_dataset_ingest --config %s'
                     % cfgfname).split()]
        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)

                vl, er = self.runtest(cmd)

                scfname = "%s/%s.scan.json" % (fsubdirname2, 'myscan_00001')

                scdict = {}
                with open(scfname, "r") as fl:
                    scn = fl.read()
                    scdict = json.loads(scn)
                scdict["scientificMetadata"]["energy"] = 123123
                with open(scfname, "w") as fl:
                    fl.write(json.dumps(scdict))

                vl, er = self.runtest(cmd)

                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                # print(er)
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                self.assertEqual(
                    'INFO : DatasetIngest: beamtime path: {basedir}\n'
                    'INFO : DatasetIngest: beamtime file: '
                    'beamtime-metadata-99001234.json\n'
                    'INFO : DatasetIngest: dataset list: {dslist}\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc1}\n'
                    'INFO : DatasetIngestor: Checking origdatablock metadata:'
                    ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Check if dataset exists: '
                    '/99001234/{sc1}\n'
                    'INFO : DatasetIngestor: Find the dataset by id: '
                    '/99001234/{sc1}\n'
                    'INFO : DatasetIngestor: '
                    'Patch scientificMetadata of dataset: '
                    '/99001234/{sc1}\n'
                    'INFO : DatasetIngestor: Ingest dataset: '
                    '{subdir2}/{sc1}.scan.json\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc2}\n'
                    'INFO : DatasetIngestor: Checking origdatablock metadata:'
                    ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                    .format(basedir=fdirname,
                            subdir2=fsubdirname2,
                            dslist=fdslist,
                            sc1='myscan_00001', sc2='myscan_00002'),
                    "\n".join(seri))
                self.assertEqual(
                    "Login: ingestor\n"
                    "RawDatasets: /99001234/myscan_00001\n",
                    vl)
                self.assertEqual(len(self.__server.userslogin), 2)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "ingestor", "password": "12342345"}')
                # self.assertEqual(
                #     self.__server.userslogin[2],
                #     b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 3)
                self.myAssertDict(
                    json.loads(self.__server.datasets[0]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerGroup': '99001234-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00001',
                     'datasetName': 'myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[1]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'ownerGroup': '99001234-dmgt',
                     'pid': '99001234/myscan_00002',
                     'datasetName': 'myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[2]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerGroup': '99001234-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '/99001234/myscan_00001',
                     'datasetName': 'myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'energy': 123123,
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.assertEqual(len(self.__server.origdatablocks), 2)
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[0]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001234-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[1]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001234-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_change_scientific_metadata_nods(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        btmeta = "beamtime-metadata-99001234.json"
        dslist = "scicat-datasets-99001234.lst"
        idslist = "scicat-ingested-datasets-99001234.lst"
        wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        wlsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                "config",
                                wrongdslist)
        # fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'dataset_update_strategy: "no"\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingest -c %s'
                     % cfgfname).split(),
                    ('scicat_dataset_ingest --config %s'
                     % cfgfname).split()]
        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)

                vl, er = self.runtest(cmd)

                scfname = "%s/%s.scan.json" % (fsubdirname2, 'myscan_00001')

                scdict = {}
                with open(scfname, "r") as fl:
                    scn = fl.read()
                    scdict = json.loads(scn)
                scdict["scientificMetadata"]["energy"] = 123123
                with open(scfname, "w") as fl:
                    fl.write(json.dumps(scdict))

                vl, er = self.runtest(cmd)

                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                # print(er)
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                self.assertEqual(
                    'INFO : DatasetIngest: beamtime path: {basedir}\n'
                    'INFO : DatasetIngest: beamtime file: '
                    'beamtime-metadata-99001234.json\n'
                    'INFO : DatasetIngest: dataset list: {dslist}\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc1}\n'
                    'INFO : DatasetIngestor: Checking origdatablock metadata:'
                    ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc2}\n'
                    'INFO : DatasetIngestor: Checking origdatablock metadata:'
                    ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                    .format(basedir=fdirname,
                            subdir2=fsubdirname2,
                            dslist=fdslist,
                            sc1='myscan_00001', sc2='myscan_00002'),
                    "\n".join(seri))
                self.assertEqual(
                    "Login: ingestor\n",
                    vl)
                self.assertEqual(len(self.__server.userslogin), 2)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "ingestor", "password": "12342345"}')
                # self.assertEqual(
                #     self.__server.userslogin[2],
                #     b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 2)
                self.myAssertDict(
                    json.loads(self.__server.datasets[0]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerGroup': '99001234-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00001',
                     'datasetName': 'myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[1]),
                    {'contactEmail': 'appuser@fake.com',
                     'instrumentId': '/petra3/p00',
                     'createdAt': '2022-05-14 11:54:29',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'ownerGroup': '99001234-dmgt',
                     'pid': '99001234/myscan_00002',
                     'datasetName': 'myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.assertEqual(len(self.__server.origdatablocks), 2)
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[0]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001234-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[1]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001234-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_change_scientific_metadata_mixed(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        btmeta = "beamtime-metadata-99001234.json"
        dslist = "scicat-datasets-99001234.lst"
        idslist = "scicat-ingested-datasets-99001234.lst"
        wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        wlsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                "config",
                                wrongdslist)
        # fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'dataset_update_strategy: "mixed"\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingest -c %s'
                     % cfgfname).split(),
                    ('scicat_dataset_ingest --config %s'
                     % cfgfname).split()]
        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)

                vl, er = self.runtest(cmd)

                scfname = "%s/%s.scan.json" % (fsubdirname2, 'myscan_00001')

                scdict = {}
                with open(scfname, "r") as fl:
                    scn = fl.read()
                    scdict = json.loads(scn)
                scdict["scientificMetadata"]["energy"] = 123123
                with open(scfname, "w") as fl:
                    fl.write(json.dumps(scdict))

                vl, er = self.runtest(cmd)

                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                # print(er)
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                self.assertEqual(
                    'INFO : DatasetIngest: beamtime path: {basedir}\n'
                    'INFO : DatasetIngest: beamtime file: '
                    'beamtime-metadata-99001234.json\n'
                    'INFO : DatasetIngest: dataset list: {dslist}\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc1}\n'
                    'INFO : DatasetIngestor: Checking origdatablock metadata:'
                    ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Check if dataset exists: '
                    '/99001234/{sc1}\n'
                    'INFO : DatasetIngestor: Find the dataset by id: '
                    '/99001234/{sc1}\n'
                    'INFO : DatasetIngestor: '
                    'Patch scientificMetadata of dataset: '
                    '/99001234/{sc1}\n'
                    'INFO : DatasetIngestor: Ingest dataset: '
                    '{subdir2}/{sc1}.scan.json\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc2}\n'
                    'INFO : DatasetIngestor: Checking origdatablock metadata:'
                    ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                    .format(basedir=fdirname,
                            subdir2=fsubdirname2,
                            dslist=fdslist,
                            sc1='myscan_00001', sc2='myscan_00002'),
                    "\n".join(seri))
                self.assertEqual(
                    "Login: ingestor\n"
                    "RawDatasets: /99001234/myscan_00001\n",
                    vl)
                self.assertEqual(len(self.__server.userslogin), 2)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "ingestor", "password": "12342345"}')
                # self.assertEqual(
                #     self.__server.userslogin[2],
                #     b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 3)
                self.myAssertDict(
                    json.loads(self.__server.datasets[0]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerGroup': '99001234-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00001',
                     'datasetName': 'myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[1]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'ownerGroup': '99001234-dmgt',
                     'pid': '99001234/myscan_00002',
                     'datasetName': 'myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[2]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerGroup': '99001234-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '/99001234/myscan_00001',
                     'datasetName': 'myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'energy': 123123,
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.assertEqual(len(self.__server.origdatablocks), 2)
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[0]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001234-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[1]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001234-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_change_scientific_metadata_create(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        btmeta = "beamtime-metadata-99001234.json"
        dslist = "scicat-datasets-99001234.lst"
        idslist = "scicat-ingested-datasets-99001234.lst"
        wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        wlsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                "config",
                                wrongdslist)
        # fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'dataset_update_strategy: "create"\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingest -c %s'
                     % cfgfname).split(),
                    ('scicat_dataset_ingest --config %s'
                     % cfgfname).split()]
        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)

                vl, er = self.runtest(cmd)

                scfname = "%s/%s.scan.json" % (fsubdirname2, 'myscan_00001')

                scdict = {}
                with open(scfname, "r") as fl:
                    scn = fl.read()
                    scdict = json.loads(scn)
                scdict["scientificMetadata"]["energy"] = 123123
                with open(scfname, "w") as fl:
                    fl.write(json.dumps(scdict))

                vl, er = self.runtest(cmd)

                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                # print(er)
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                self.assertEqual(
                    'INFO : DatasetIngest: beamtime path: {basedir}\n'
                    'INFO : DatasetIngest: beamtime file: '
                    'beamtime-metadata-99001234.json\n'
                    'INFO : DatasetIngest: dataset list: {dslist}\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc1}\n'
                    'INFO : DatasetIngestor: Checking origdatablock metadata:'
                    ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Check if dataset exists: '
                    '/99001234/{sc1}\n'
                    'INFO : DatasetIngestor: Find the dataset by id: '
                    '/99001234/{sc1}\n'
                    'INFO : DatasetIngestor: '
                    'Post the dataset with a new pid: '
                    '/99001234/{sc1}/2\n'
                    'INFO : DatasetIngestor: Ingest dataset: '
                    '{subdir2}/{sc1}.scan.json\n'
                    'INFO : DatasetIngestor: '
                    'Generating origdatablock metadata:'
                    ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Ingest origdatablock: '
                    '{subdir2}/{sc1}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc2}\n'
                    'INFO : DatasetIngestor: Checking origdatablock metadata:'
                    ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                    .format(basedir=fdirname,
                            subdir2=fsubdirname2,
                            dslist=fdslist,
                            sc1='myscan_00001', sc2='myscan_00002'),
                    "\n".join(seri))
                self.assertEqual(
                    "Login: ingestor\n"
                    "RawDatasets: 99001234/myscan_00001/2\n"
                    "OrigDatablocks: /99001234/myscan_00001/2\n",
                    vl)
                self.assertEqual(len(self.__server.userslogin), 2)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "ingestor", "password": "12342345"}')
                # self.assertEqual(
                #     self.__server.userslogin[2],
                #     b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 3)
                self.myAssertDict(
                    json.loads(self.__server.datasets[0]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerGroup': '99001234-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00001',
                     'datasetName': 'myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[1]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'ownerGroup': '99001234-dmgt',
                     'pid': '99001234/myscan_00002',
                     'datasetName': 'myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[2]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerGroup': '99001234-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00001/2',
                     'datasetName': 'myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'energy': 123123,
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.assertEqual(len(self.__server.origdatablocks), 3)
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[0]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001234-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[1]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001234-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[2]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001234/myscan_00001/2',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001234-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_exist_log(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        btmeta = "beamtime-metadata-99001234.json"
        dslist = "scicat-datasets-99001234.lst"
        idslist = "scicat-ingested-datasets-99001234.lst"
        wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        wlsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                "config",
                                wrongdslist)
        # fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/tmp/scingestor_log_%s/{beamtimeid}" % uuid.uuid4().hex
        lvardir = vardir.format(beamtimeid="99001234")
        fidslist = "%s%s" % (vardir, fidslist)
        cred = "12342345"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [("scicat_dataset_ingest  -c %s"
                     % cfgfname).split(),
                    ("scicat_dataset_ingest --config %s"
                     % cfgfname).split()]
        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                # print(vl)
                # print(er)
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                self.assertEqual(
                    'INFO : DatasetIngest: beamtime path: {basedir}\n'
                    'INFO : DatasetIngest: beamtime file: '
                    'beamtime-metadata-99001234.json\n'
                    'INFO : DatasetIngest: dataset list: {dslist}\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc1}\n'
                    'INFO : DatasetIngestor: Generating metadata: '
                    '{sc1} {subdir2}/{sc1}.scan.json\n'
                    'INFO : DatasetIngestor: '
                    'Generating origdatablock metadata:'
                    ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Check if dataset exists: '
                    '/99001234/{sc1}\n'
                    'INFO : DatasetIngestor: Post the dataset: '
                    '/99001234/{sc1}\n'
                    'INFO : DatasetIngestor: Ingest dataset: '
                    '{subdir2}/{sc1}.scan.json\n'
                    'INFO : DatasetIngestor: Ingest origdatablock: '
                    '{subdir2}/{sc1}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc2}\n'
                    'INFO : DatasetIngestor: Generating metadata: '
                    '{sc2} {subdir2}/{sc2}.scan.json\n'
                    'INFO : DatasetIngestor: '
                    'Generating origdatablock metadata:'
                    ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Check if dataset exists: '
                    '/99001234/{sc2}\n'
                    'INFO : DatasetIngestor: Post the dataset: '
                    '/99001234/{sc2}\n'
                    'INFO : DatasetIngestor: Ingest dataset: '
                    '{subdir2}/{sc2}.scan.json\n'
                    'INFO : DatasetIngestor: Ingest origdatablock: '
                    '{subdir2}/{sc2}.origdatablock.json\n'
                    .format(basedir=fdirname,
                            subdir2=fsubdirname2,
                            dslist=fdslist,
                            sc1='myscan_00001', sc2='myscan_00002'),
                    "\n".join(seri))
                self.assertEqual(
                    "Login: ingestor\n"
                    "RawDatasets: 99001234/myscan_00001\n"
                    "OrigDatablocks: /99001234/myscan_00001\n"
                    "RawDatasets: 99001234/myscan_00002\n"
                    "OrigDatablocks: /99001234/myscan_00002\n", vl)
                self.assertEqual(len(self.__server.userslogin), 1)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 2)
                self.myAssertDict(
                    json.loads(self.__server.datasets[0]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerGroup': '99001234-dmgt',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00001',
                     'datasetName': 'myscan_00001',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[1]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerGroup': '99001234-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00002',
                     'datasetName': 'myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.assertEqual(len(self.__server.origdatablocks), 2)
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[0]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'ownerGroup': '99001234-dmgt',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'datasetId': '/99001234/myscan_00001',
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[1]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001234-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)
            if os.path.isdir(lvardir):
                shutil.rmtree(lvardir)

    def test_datasetfile_repeat_log(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        btmeta = "beamtime-metadata-99001234.json"
        dslist = "scicat-datasets-99001234.lst"
        idslist = "scicat-ingested-datasets-99001234.lst"
        wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        wlsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                "config",
                                wrongdslist)
        # fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/tmp/scingestor_log_%s" % uuid.uuid4().hex
        fidslist = "%s%s" % (vardir, fidslist)
        cred = "12342345"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingest -c %s'
                     % cfgfname).split(),
                    ('scicat_dataset_ingest --config %s'
                     % cfgfname).split()]
        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)
                vl, er = self.runtest(cmd)
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                # print(er)
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                self.assertEqual(
                    'INFO : DatasetIngest: beamtime path: {basedir}\n'
                    'INFO : DatasetIngest: beamtime file: '
                    'beamtime-metadata-99001234.json\n'
                    'INFO : DatasetIngest: dataset list: {dslist}\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc1}\n'
                    'INFO : DatasetIngestor: Checking origdatablock metadata:'
                    ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc2}\n'
                    'INFO : DatasetIngestor: Checking origdatablock metadata:'
                    ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                    .format(basedir=fdirname,
                            subdir2=fsubdirname2,
                            dslist=fdslist,
                            sc1='myscan_00001', sc2='myscan_00002'),
                    "\n".join(seri))
                self.assertEqual("Login: ingestor\n", vl)
                self.assertEqual(len(self.__server.userslogin), 2)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 2)
                self.myAssertDict(
                    json.loads(self.__server.datasets[0]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerGroup': '99001234-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00001',
                     'datasetName': 'myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[1]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'ownerGroup': '99001234-dmgt',
                     'pid': '99001234/myscan_00002',
                     'datasetName': 'myscan_00002',
                     'principalInvestigator': 'appuser@fake.com',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.assertEqual(len(self.__server.origdatablocks), 2)
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[0]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001234-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[1]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001234-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)
            if os.path.isdir(vardir):
                shutil.rmtree(vardir)

    def test_datasetfile_touch_log(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        btmeta = "beamtime-metadata-99001234.json"
        dslist = "scicat-datasets-99001234.lst"
        idslist = "scicat-ingested-datasets-99001234.lst"
        wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        wlsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                "config",
                                wrongdslist)
        # fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'

        vardir = "/tmp/scingestor_log_%s" % uuid.uuid4().hex
        fidslist = "%s%s" % (vardir, fidslist)

        cred = "12342345"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingest -c %s'
                     % cfgfname).split(),
                    ('scicat_dataset_ingest --config %s'
                     % cfgfname).split()]
        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)
                # print(vl)
                # print(er)

                dsfname1 = "%s/%s.scan.json" % \
                           (fsubdirname2, 'myscan_00001')
                dbfname2 = "%s/%s.origdatablock.json" % \
                           (fsubdirname2, 'myscan_00002')
                # print(dbfname2)
                # import time
                # mtmds = os.path.getmtime(dsfname1)
                # mtmdb = os.path.getmtime(dbfname2)
                # print("BEFORE", mtmds, mtmdb)

                # on cenos6 touch modify only timestamps
                # when last modification > 1s
                time.sleep(1.1)
                os.utime(dbfname2)
                os.utime(dsfname1)

                # mtmds = os.path.getmtime(dsfname1)
                # mtmdb = os.path.getmtime(dbfname2)
                # print("AFTER", mtmds, mtmdb)

                vl, er = self.runtest(cmd)
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                # print(vl)
                # print(er)
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                self.assertEqual(
                    'INFO : DatasetIngest: beamtime path: {basedir}\n'
                    'INFO : DatasetIngest: beamtime file: '
                    'beamtime-metadata-99001234.json\n'
                    'INFO : DatasetIngest: dataset list: {dslist}\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc1}\n'
                    'INFO : DatasetIngestor: Checking origdatablock metadata:'
                    ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Check if dataset exists: '
                    '/99001234/{sc1}\n'
                    'INFO : DatasetIngestor: Find the dataset by id: '
                    '/99001234/{sc1}\n'
                    'INFO : DatasetIngestor: Ingest dataset: '
                    '{subdir2}/{sc1}.scan.json\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc2}\n'
                    'INFO : DatasetIngestor: Checking origdatablock metadata:'
                    ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                    'INFO : DatasetIngestor: '
                    'Generating origdatablock metadata:'
                    ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Ingest origdatablock:'
                    ' {subdir2}/{sc2}.origdatablock.json\n'
                    .format(basedir=fdirname,
                            subdir2=fsubdirname2,
                            dslist=fdslist,
                            sc1='myscan_00001', sc2='myscan_00002'),
                    "\n".join(seri))
                self.assertEqual(
                    "Login: ingestor\n"
                    "OrigDatablocks: delete /99001234/myscan_00002\n"
                    "OrigDatablocks: /99001234/myscan_00002\n",
                    vl)
                self.assertEqual(len(self.__server.userslogin), 2)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "ingestor", "password": "12342345"}')
                # self.assertEqual(
                #     self.__server.userslogin[2],
                #     b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 2)
                self.myAssertDict(
                    json.loads(self.__server.datasets[0]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'ownerGroup': '99001234-dmgt',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00001',
                     'datasetName': 'myscan_00001',
                     'principalInvestigator': 'appuser@fake.com',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[1]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerGroup': '99001234-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00002',
                     'datasetName': 'myscan_00002',
                     'principalInvestigator': 'appuser@fake.com',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.assertEqual(len(self.__server.origdatablocks), 3)
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[0]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001234-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[1]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001234-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[2]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '/99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'ownerGroup': '99001234-dmgt',
                     'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)
            if os.path.isdir(vardir):
                shutil.rmtree(vardir)

    def test_datasetfile_exist_log_meta(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        btmeta = "beamtime-metadata-99001234.json"
        dslist = "scicat-datasets-99001234.lst"
        idslist = "scicat-ingested-datasets-99001234.lst"
        wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        wlsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                "config",
                                wrongdslist)
        # fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/tmp/scingestor_log_%s/{beamtimeid}" % uuid.uuid4().hex
        lvardir = vardir.format(beamtimeid="99001234")
        fidslist = "%s%s" % (lvardir, fidslist)
        cred = "12342345"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)
        prop = {
            "ownerGroup": "mygroup",
            "accessGroups": ["group1", "group2"],
        }

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'dataset_pid_prefix: "10.3204"\n' \
            'metadata_in_var_dir: true\n' \
            'owner_access_groups_from_proposal: true\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [("scicat_dataset_ingest  -c %s"
                     % cfgfname).split(),
                    ("scicat_dataset_ingest --config %s"
                     % cfgfname).split()]
        # commands.pop()
        try:
            oldpidprefix = self.__server.pidprefix
            self.__server.pidprefix = "10.3204/"
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.__server.reset()
                self.__server.pid_proposal["99001234"] = json.dumps(prop)
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                # print(vl)
                # print(er)
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                self.assertEqual(
                    'INFO : DatasetIngest: beamtime path: {basedir}\n'
                    'INFO : DatasetIngest: beamtime file: '
                    'beamtime-metadata-99001234.json\n'
                    'INFO : DatasetIngest: dataset list: {dslist}\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc1}\n'
                    'INFO : DatasetIngestor: Generating metadata: '
                    '{sc1} {vardir}{subdir2}/{sc1}.scan.json\n'
                    'INFO : DatasetIngestor: '
                    'Generating origdatablock metadata:'
                    ' {sc1} {vardir}{subdir2}/{sc1}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Check if dataset exists: '
                    '10.3204/99001234/{sc1}\n'
                    'INFO : DatasetIngestor: Post the dataset: '
                    '10.3204/99001234/{sc1}\n'
                    'INFO : DatasetIngestor: Ingest dataset: '
                    '{vardir}{subdir2}/{sc1}.scan.json\n'
                    'INFO : DatasetIngestor: Ingest origdatablock: '
                    '{vardir}{subdir2}/{sc1}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc2}\n'
                    'INFO : DatasetIngestor: Generating metadata: '
                    '{sc2} {vardir}{subdir2}/{sc2}.scan.json\n'
                    'INFO : DatasetIngestor: '
                    'Generating origdatablock metadata:'
                    ' {sc2} {vardir}{subdir2}/{sc2}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Check if dataset exists: '
                    '10.3204/99001234/{sc2}\n'
                    'INFO : DatasetIngestor: Post the dataset: '
                    '10.3204/99001234/{sc2}\n'
                    'INFO : DatasetIngestor: Ingest dataset: '
                    '{vardir}{subdir2}/{sc2}.scan.json\n'
                    'INFO : DatasetIngestor: Ingest origdatablock: '
                    '{vardir}{subdir2}/{sc2}.origdatablock.json\n'
                    .format(basedir=fdirname,
                            subdir2=fsubdirname2,
                            dslist=fdslist,
                            vardir=lvardir,
                            sc1='myscan_00001', sc2='myscan_00002'),
                    "\n".join(seri))
                self.assertEqual(
                    "Login: ingestor\n"
                    "Login: ingestor\n"
                    "RawDatasets: 99001234/myscan_00001\n"
                    "OrigDatablocks: 10.3204/99001234/myscan_00001\n"
                    "RawDatasets: 99001234/myscan_00002\n"
                    "OrigDatablocks: 10.3204/99001234/myscan_00002\n", vl)
                self.assertEqual(len(self.__server.userslogin), 2)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 2)
                self.myAssertDict(
                    json.loads(self.__server.datasets[0]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerGroup': 'mygroup',
                     'accessGroups': ['group1', 'group2'],
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00001',
                     'datasetName': 'myscan_00001',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[1]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'accessGroups': ['group1', 'group2'],
                     'ownerGroup': 'mygroup',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00002',
                     'datasetName': 'myscan_00002',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.assertEqual(len(self.__server.origdatablocks), 2)
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[0]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'ownerGroup': 'mygroup',
                     'accessGroups': ['group1', 'group2'],
                     'datasetId': '10.3204/99001234/myscan_00001',
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[1]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '10.3204/99001234/myscan_00002',
                     'accessGroups': ['group1', 'group2'],
                     'ownerGroup': 'mygroup',
                     'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
                if os.path.isdir("%s%s" % (lvardir, fsubdirname)):
                    shutil.rmtree("%s%s" % (lvardir, fsubdirname))
        finally:
            self.__server.pidprefix = oldpidprefix
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)
            if os.path.isdir(lvardir):
                shutil.rmtree(lvardir)

    def test_datasetfile_repeat_log_meta(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        btmeta = "beamtime-metadata-99001234.json"
        dslist = "scicat-datasets-99001234.lst"
        idslist = "scicat-ingested-datasets-99001234.lst"
        wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        wlsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                "config",
                                wrongdslist)
        # fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/tmp/scingestor_log_%s" % uuid.uuid4().hex
        fidslist = "%s%s" % (vardir, fidslist)
        cred = "12342345"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)
        prop = {
            "ownerGroup": "mygroup",
            "accessGroups": ["group1", "group2"],
        }

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'metadata_in_var_dir: true\n' \
            'owner_access_groups_from_proposal: true\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'dataset_pid_prefix: "10.3204"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingest -c %s'
                     % cfgfname).split(),
                    ('scicat_dataset_ingest --config %s'
                     % cfgfname).split()]
        # commands.pop()
        try:
            oldpidprefix = self.__server.pidprefix
            self.__server.pidprefix = "10.3204/"
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.__server.reset()
                self.__server.pid_proposal["99001234"] = json.dumps(prop)
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)
                vl, er = self.runtest(cmd)
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                # print(er)
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                self.assertEqual(
                    'INFO : DatasetIngest: beamtime path: {basedir}\n'
                    'INFO : DatasetIngest: beamtime file: '
                    'beamtime-metadata-99001234.json\n'
                    'INFO : DatasetIngest: dataset list: {dslist}\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc1}\n'
                    'INFO : DatasetIngestor: Checking origdatablock metadata:'
                    ' {sc1} {vardir}{subdir2}/{sc1}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc2}\n'
                    'INFO : DatasetIngestor: Checking origdatablock metadata:'
                    ' {sc2} {vardir}{subdir2}/{sc2}.origdatablock.json\n'
                    .format(basedir=fdirname,
                            subdir2=fsubdirname2,
                            vardir=vardir,
                            dslist=fdslist,
                            sc1='myscan_00001', sc2='myscan_00002'),
                    "\n".join(seri))
                self.assertEqual("Login: ingestor\nLogin: ingestor\n", vl)
                self.assertEqual(len(self.__server.userslogin), 4)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[2],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[3],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 2)
                self.myAssertDict(
                    json.loads(self.__server.datasets[0]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'ownerGroup': 'mygroup',
                     'owner': 'Smithson',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00001',
                     'datasetName': 'myscan_00001',
                     'accessGroups': ['group1', 'group2'],
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[1]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'ownerGroup': 'mygroup',
                     'pid': '99001234/myscan_00002',
                     'datasetName': 'myscan_00002',
                     'principalInvestigator': 'appuser@fake.com',
                     'accessGroups': ['group1', 'group2'],
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.assertEqual(len(self.__server.origdatablocks), 2)
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[0]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '10.3204/99001234/myscan_00001',
                     'accessGroups': ['group1', 'group2'],
                     'ownerGroup': 'mygroup',
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[1]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '10.3204/99001234/myscan_00002',
                     'accessGroups': ['group1', 'group2'],
                     'ownerGroup': 'mygroup',
                     'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
                if os.path.isdir("%s%s" % (vardir, fsubdirname)):
                    shutil.rmtree("%s%s" % (vardir, fsubdirname))
        finally:
            self.__server.pidprefix = oldpidprefix
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)
            if os.path.isdir(vardir):
                shutil.rmtree(vardir)

    def test_datasetfile_touch_log_meta(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        btmeta = "beamtime-metadata-99001234.json"
        dslist = "scicat-datasets-99001234.lst"
        idslist = "scicat-ingested-datasets-99001234.lst"
        wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        wlsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                "config",
                                wrongdslist)
        # fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'

        vardir = "/tmp/scingestor_log_%s" % uuid.uuid4().hex
        fidslist = "%s%s" % (vardir, fidslist)

        cred = "12342345"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'metadata_in_var_dir: true\n' \
            'dataset_pid_prefix: "10.3204"\n' \
            'owner_access_groups_from_proposal: true\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir, credfile=credfile)

        prop = {
            "ownerGroup": "mygroup",
            "accessGroups": ["group1", "group2"],
        }

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingest -c %s'
                     % cfgfname).split(),
                    ('scicat_dataset_ingest --config %s'
                     % cfgfname).split()]
        # commands.pop()
        try:
            oldpidprefix = self.__server.pidprefix
            self.__server.pidprefix = "10.3204/"
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.__server.reset()
                self.__server.pid_proposal["99001234"] = json.dumps(prop)
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)
                # print(vl)
                # print(er)

                dsfname1 = "%s%s/%s.scan.json" % \
                           (vardir, fsubdirname2, 'myscan_00001')
                dbfname2 = "%s%s/%s.origdatablock.json" % \
                           (vardir, fsubdirname2, 'myscan_00002')
                # print(dbfname2)
                # import time
                # mtmds = os.path.getmtime(dsfname1)
                # mtmdb = os.path.getmtime(dbfname2)
                # print("BEFORE", mtmds, mtmdb)

                # on cenos6 touch modify only timestamps
                # when last modification > 1s
                time.sleep(1.1)
                os.utime(dbfname2)
                os.utime(dsfname1)

                # mtmds = os.path.getmtime(dsfname1)
                # mtmdb = os.path.getmtime(dbfname2)
                # print("AFTER", mtmds, mtmdb)

                vl, er = self.runtest(cmd)
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                # print(vl)
                # print(er)
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                self.assertEqual(
                    'INFO : DatasetIngest: beamtime path: {basedir}\n'
                    'INFO : DatasetIngest: beamtime file: '
                    'beamtime-metadata-99001234.json\n'
                    'INFO : DatasetIngest: dataset list: {dslist}\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc1}\n'
                    'INFO : DatasetIngestor: Checking origdatablock metadata:'
                    ' {sc1} {vardir}{subdir2}/{sc1}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Check if dataset exists: '
                    '10.3204/99001234/{sc1}\n'
                    'INFO : DatasetIngestor: Find the dataset by id: '
                    '10.3204/99001234/{sc1}\n'
                    'INFO : DatasetIngestor: Ingest dataset: '
                    '{vardir}{subdir2}/{sc1}.scan.json\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc2}\n'
                    'INFO : DatasetIngestor: Checking origdatablock metadata:'
                    ' {sc2} {vardir}{subdir2}/{sc2}.origdatablock.json\n'
                    'INFO : DatasetIngestor: '
                    'Generating origdatablock metadata:'
                    ' {sc2} {vardir}{subdir2}/{sc2}.origdatablock.json\n'
                    'INFO : DatasetIngestor: Ingest origdatablock:'
                    ' {vardir}{subdir2}/{sc2}.origdatablock.json\n'
                    .format(basedir=fdirname,
                            subdir2=fsubdirname2,
                            dslist=fdslist,
                            vardir=vardir,
                            sc1='myscan_00001', sc2='myscan_00002'),
                    "\n".join(seri))
                self.assertEqual(
                    "Login: ingestor\n"
                    "Login: ingestor\n"
                    "OrigDatablocks: delete 10.3204/99001234/myscan_00002\n"
                    "OrigDatablocks: 10.3204/99001234/myscan_00002\n",
                    vl)
                self.assertEqual(len(self.__server.userslogin), 4)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[2],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[3],
                    b'{"username": "ingestor", "password": "12342345"}')
                # self.assertEqual(
                #     self.__server.userslogin[2],
                #     b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 2)
                self.myAssertDict(
                    json.loads(self.__server.datasets[0]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'ownerGroup': 'mygroup',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00001',
                     'datasetName': 'myscan_00001',
                     'principalInvestigator': 'appuser@fake.com',
                     'accessGroups': ['group1', 'group2'],
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[1]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerGroup': 'mygroup',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00002',
                     'datasetName': 'myscan_00002',
                     'principalInvestigator': 'appuser@fake.com',
                     'accessGroups': [
                         'group1', 'group2'],
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.assertEqual(len(self.__server.origdatablocks), 3)
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[0]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '10.3204/99001234/myscan_00001',
                     'accessGroups': [
                         'group1', 'group2'],
                     'ownerGroup': 'mygroup',
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[1]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '10.3204/99001234/myscan_00002',
                     'accessGroups': [
                         'group1', 'group2'],
                     'ownerGroup': 'mygroup',
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[2]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'datasetId': '10.3204/99001234/myscan_00002',
                     'accessGroups': [
                         'group1', 'group2'],
                     'ownerGroup': 'mygroup',
                     'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
                if os.path.isdir("%s%s" % (vardir, fsubdirname)):
                    shutil.rmtree("%s%s" % (vardir, fsubdirname))
        finally:
            self.__server.pidprefix = oldpidprefix
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)
            if os.path.isdir(vardir):
                shutil.rmtree(vardir)


if __name__ == '__main__':
    unittest.main()
