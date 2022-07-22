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
        logdir = "/"
        cred = "12342345"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'ingestor_log_dir: "{logdir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, logdir=logdir, credfile=credfile)

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
                    "OrigDatablocks: 10.3204/99001234/myscan_00001\n"
                    "RawDatasets: 99001234/myscan_00002\n"
                    "OrigDatablocks: 10.3204/99001234/myscan_00002\n", vl)
                self.assertEqual(len(self.__server.userslogin), 1)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 2)
                self.myAssertDict(
                    json.loads(self.__server.datasets[0]),
                    {'contactEmail': 'BSName',
                     'createdAt': '2022-05-14 11:54:29',
                     'creationLocation': '/DESY/PETRA III/p00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'owner': 'Ouruser',
                     'ownerGroup': '99001234-part',
                     'accessGroups': [
                         '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
                     'ownerEmail': 'appuser@fake.com',
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
                    {'contactEmail': 'BSName',
                     'createdAt': '2022-05-14 11:54:29',
                     'creationLocation': '/DESY/PETRA III/p00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'owner': 'Ouruser',
                     'ownerGroup': '99001234-part',
                     'ownerEmail': 'appuser@fake.com',
                     'pid': '99001234/myscan_00002',
                     'datasetName': 'myscan_00002',
                     'accessGroups': [
                         '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
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
                     'ownerGroup': '99001234-part',
                     'accessGroups': [
                         '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
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
                     'accessGroups': [
                         '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
                     'ownerGroup': '99001234-part',
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
        logdir = "/"
        cred = "12342345"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'ingestor_log_dir: "{logdir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, logdir=logdir, credfile=credfile)

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
                    {'contactEmail': 'BSName',
                     'createdAt': '2022-05-14 11:54:29',
                     'creationLocation': '/DESY/PETRA III/p00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'owner': 'Ouruser',
                     'ownerGroup': '99001234-part',
                     'ownerEmail': 'appuser@fake.com',
                     'pid': '99001234/myscan_00001',
                     'datasetName': 'myscan_00001',
                     'accessGroups': [
                         '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
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
                    {'contactEmail': 'BSName',
                     'createdAt': '2022-05-14 11:54:29',
                     'creationLocation': '/DESY/PETRA III/p00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'owner': 'Ouruser',
                     'ownerEmail': 'appuser@fake.com',
                     'ownerGroup': '99001234-part',
                     'pid': '99001234/myscan_00002',
                     'datasetName': 'myscan_00002',
                     'principalInvestigator': 'appuser@fake.com',
                     'accessGroups': [
                         '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
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
                     'accessGroups': [
                         '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
                     'ownerGroup': '99001234-part',
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
                         '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
                     'ownerGroup': '99001234-part',
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
        logdir = "/"
        cred = "12342345"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'ingestor_log_dir: "{logdir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, logdir=logdir, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingest -c %s'
                     % cfgfname).split(),
                    ('scicat_dataset_ingest --config %s'
                     % cfgfname).split()]
        commands.pop()
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
                    'INFO : DatasetIngestor: Ingest dataset: '
                    '{subdir2}/{sc1}.scan.json\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc2}\n'
                    'INFO : DatasetIngestor: Checking origdatablock metadata:'
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
                    "RawDatasets: 99001234/myscan_00001\n"
                    "OrigDatablocks: 10.3204/99001234/myscan_00002\n",
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
                    {'contactEmail': 'BSName',
                     'createdAt': '2022-05-14 11:54:29',
                     'creationLocation': '/DESY/PETRA III/p00',
                     'ownerGroup': '99001234-part',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'owner': 'Ouruser',
                     'ownerEmail': 'appuser@fake.com',
                     'pid': '99001234/myscan_00001',
                     'datasetName': 'myscan_00001',
                     'principalInvestigator': 'appuser@fake.com',
                     'accessGroups': [
                         '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
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
                    {'contactEmail': 'BSName',
                     'createdAt': '2022-05-14 11:54:29',
                     'creationLocation': '/DESY/PETRA III/p00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'owner': 'Ouruser',
                     'ownerGroup': '99001234-part',
                     'ownerEmail': 'appuser@fake.com',
                     'pid': '99001234/myscan_00002',
                     'datasetName': 'myscan_00002',
                     'principalInvestigator': 'appuser@fake.com',
                     'accessGroups': [
                         '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
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
                    {'contactEmail': 'BSName',
                     'createdAt': '2022-05-14 11:54:29',
                     'creationLocation': '/DESY/PETRA III/p00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'owner': 'Ouruser',
                     'ownerEmail': 'appuser@fake.com',
                     'ownerGroup': '99001234-part',
                     'pid': '99001234/myscan_00001',
                     'datasetName': 'myscan_00001',
                     'accessGroups': [
                         '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
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
                     'datasetId': '10.3204/99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
                     'ownerGroup': '99001234-part',
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
                         '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
                     'ownerGroup': '99001234-part',
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
                         '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
                     'ownerGroup': '99001234-part',
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
        logdir = "/"
        cred = "12342345"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'ingestor_log_dir: "{logdir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, logdir=logdir, credfile=credfile)

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
                    'INFO : DatasetIngestor: Ingest dataset: '
                    '{subdir2}/{sc1}.scan.json\n'
                    'INFO : DatasetIngestor: Checking: {dslist} {sc2}\n'
                    'INFO : DatasetIngestor: Checking origdatablock metadata:'
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
                    "RawDatasets: 99001234/myscan_00001\n"
                    "OrigDatablocks: 10.3204/99001234/myscan_00002\n",
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
                    {'contactEmail': 'BSName',
                     'createdAt': '2022-05-14 11:54:29',
                     'creationLocation': '/DESY/PETRA III/p00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'owner': 'Ouruser',
                     'ownerGroup': '99001234-part',
                     'ownerEmail': 'appuser@fake.com',
                     'pid': '99001234/myscan_00001',
                     'datasetName': 'myscan_00001',
                     'accessGroups': [
                         '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
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
                    {'contactEmail': 'BSName',
                     'createdAt': '2022-05-14 11:54:29',
                     'creationLocation': '/DESY/PETRA III/p00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'owner': 'Ouruser',
                     'ownerEmail': 'appuser@fake.com',
                     'ownerGroup': '99001234-part',
                     'pid': '99001234/myscan_00002',
                     'datasetName': 'myscan_00002',
                     'accessGroups': [
                         '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
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
                     'creationLocation': '/DESY/PETRA III/p00',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'owner': 'NewOwner',
                     'ownerGroup': '99001234-part',
                     'ownerEmail': 'appuser@fake.com',
                     'pid': '99001234/myscan_00001',
                     'datasetName': 'myscan_00001',
                     'accessGroups': [
                         '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
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
                     'datasetId': '10.3204/99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
                     'ownerGroup': '99001234-part',
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
                         '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
                     'ownerGroup': '99001234-part',
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
                         '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
                     'ownerGroup': '99001234-part',
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
        logdir = "/"
        cred = "12342345"
        dfname = "%s/%s.dat" % (fsubdirname2, 'myscan_00002')
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'ingestor_log_dir: "{logdir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, logdir=logdir, credfile=credfile)

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
                try:
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
                        # 'INFO : DatasetIngestor: Ingest dataset: '
                        # '{subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: Checking: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Checking origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
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
                        "RawDatasets: 99001234/myscan_00002\n"
                        "OrigDatablocks: 10.3204/99001234/myscan_00002\n",
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
                        {'contactEmail': 'BSName',
                         'createdAt': '2022-05-14 11:54:29',
                         'creationLocation': '/DESY/PETRA III/p00',
                         'description': 'H20 distribution',
                         'endTime': '2022-05-19 09:00:00',
                         'isPublished': False,
                         'owner': 'Ouruser',
                         'ownerGroup': '99001234-part',
                         'ownerEmail': 'appuser@fake.com',
                         'pid': '99001234/myscan_00001',
                         'datasetName': 'myscan_00001',
                         'accessGroups': [
                             '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
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
                        {'contactEmail': 'BSName',
                         'createdAt': '2022-05-14 11:54:29',
                         'creationLocation': '/DESY/PETRA III/p00',
                         'description': 'H20 distribution',
                         'endTime': '2022-05-19 09:00:00',
                         'ownerGroup': '99001234-part',
                         'isPublished': False,
                         'owner': 'Ouruser',
                         'ownerEmail': 'appuser@fake.com',
                         'pid': '99001234/myscan_00002',
                         'datasetName': 'myscan_00002',
                         'accessGroups': [
                             '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
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
                         'creationLocation': '/DESY/PETRA III/p00',
                         'description': 'H20 distribution',
                         'endTime': '2022-05-19 09:00:00',
                         'ownerGroup': '99001234-part',
                         'isPublished': False,
                         'owner': 'NewOwner',
                         'ownerEmail': 'appuser@fake.com',
                         'pid': '99001234/myscan_00002',
                         'datasetName': 'myscan_00002',
                         'accessGroups': [
                             '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
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
                         'datasetId': '10.3204/99001234/myscan_00001',
                         'accessGroups': [
                             '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
                         'ownerGroup': '99001234-part',
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
                             '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
                         'ownerGroup': '99001234-part',
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
                             '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
                         'ownerGroup': '99001234-part',
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


if __name__ == '__main__':
    unittest.main()
