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
import time
import json
import uuid
import re

from scingestor import beamtimeWatcher
from scingestor import safeINotifier


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
class DatasetWatcherTest(unittest.TestCase):

    # constructor
    # \param methodName name of the test method
    def __init__(self, methodName):
        unittest.TestCase.__init__(self, methodName)

        self.maxDiff = None
        self.notifier = safeINotifier.SafeINotifier()

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
            tm = threading.Timer(1., myinput, [w, pipeinput])
            tm.start()
        else:
            old_stdin = sys.stdin
            sys.stdin = StringIO()

        etxt = None
        try:
            beamtimeWatcher.main()
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
            beamtimeWatcher.main()
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

    def sortmarkedlines(self, text, marks=None, replace=None):
        replace = replace or {}
        marks = marks or []
        if isinstance(text, list):
            ltext = text
        else:
            ltext = text.split("\n")
        for bg, ed in marks:
            for il in range(bg, ed):
                for it, ot in replace.items():
                    ltext[il] = re.sub(it, ot, ltext[il])
            ltext[bg:ed] = sorted(ltext[bg:ed])
        return "\n".join(ltext)

    def test_datasetfile_exist(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
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
        fullbtmeta = os.path.join(fdirname, btmeta)
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
            'scicat_users_login_path: "Users/login"\n' \
            'scicat_datasets_path: "RawDatasets"\n' \
            'scicat_datablocks_path: "OrigDatablocks"\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        logfname = "%s_%s.log" % (self.__class__.__name__, fun)
        logfname1 = "%s_%s.log.1" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingestor -c %s -f %s -r10 '
                     % (cfgfname, logfname)).split(),
                    ('scicat_dataset_ingestor --config %s --log-file %s '
                     ' -r10 '
                     % (cfgfname, logfname)).split()]
        # commands.pop()
        lastlog = None
        try:
            for cmd in commands:
                time.sleep(1)
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                os.mkdir(fsubdirname3)
                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)
                with open(logfname) as lfl:
                    er = lfl.read()

                if lastlog:
                    with open(logfname1) as lfl:
                        er2 = lfl.read()
                    self.assertEqual(er2, lastlog)
                lastlog = er

                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]
                # print(vl)
                # print(er)

                # nodebug = "\n".join([ee for ee in er.split("\n")
                #                      if (("DEBUG :" not in ee) and
                #                          (not ee.startswith("127.0.0.1")))])
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                try:
                    self.assertEqual(
                        'INFO : BeamtimeWatcher: Adding watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Creating DatasetWatcher '
                        '{dslist}\n'
                        'INFO : DatasetWatcher: Adding watch {cnt5}: '
                        '{dslist} {idslist}\n'
                        'INFO : DatasetWatcher: Waiting datasets: '
                        '[\'{sc1}\', \'{sc2}\']\n'
                        'INFO : DatasetWatcher: Ingested datasets: []\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc1}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc2}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Stopping DatasetWatcher '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{dslist}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                subdir=fsubdirname, subdir2=fsubdirname2,
                                dslist=fdslist, idslist=fidslist,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4),
                                sc1='myscan_00001', sc2='myscan_00002'),
                        '\n'.join(dseri))
                except Exception:
                    print(er)
                    raise
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
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
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
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
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
                     'ownerGroup': '99001234-dmgt',
                     'datasetId': '/99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
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
                     'ownerGroup': '99001234-dmgt',
                     'datasetId': '/99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.exists(logfname):
                os.remove(logfname)
            if os.path.exists(logfname1):
                os.remove(logfname1)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_add(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
        os.mkdir(fdirname)
        btmeta = "beamtime-metadata-99001234.json"
        dslist = "sc-ds-99001234.lst"
        idslist = "sc-ids-99001234.lst"
        # wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        shutil.copy(source, fdirname)
        # shutil.copy(lsource, fsubdirname2)
        # shutil.copy(wlsource, fsubdirname)
        fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        username = "myingestor"
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'inotify_timeout: 0.2\n' \
            'get_event_timeout: 0.02\n' \
            'ingestion_delay_time: 2\n' \
            'max_request_tries_number: 10\n' \
            'recheck_beamtime_file_interval: 1000\n' \
            'rrecheck_dataset_list_interval: 1000\n' \
            'request_headers:\n' \
            '  "Content-Type": "application/json"\n' \
            '  "Accept": "application/json"\n' \
            'datasets_filename_pattern: "sc-ds-{{beamtimeid}}.lst"\n' \
            'ingested_datasets_filename_pattern: ' \
            '"sc-ids-{{beamtimeid}}.lst"\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_username: "{username}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir,
                username=username, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)

        commands = [('scicat_dataset_ingestor -c %s -r36 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r36 -l debug'
                     % cfgfname).split()]

        def tst_thread():
            """ test thread which adds and removes beamtime metadata file """
            time.sleep(3)
            shutil.copy(lsource, fsubdirname2)
            time.sleep(5)
            os.mkdir(fsubdirname3)
            time.sleep(12)
            with open(fdslist, "a+") as fds:
                fds.write("myscan_00003\n")
                fds.write("myscan_00004\n")

        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                # print(cmd)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                shutil.copy(lsource, fsubdirname2)
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                th = threading.Thread(target=tst_thread)
                th.start()
                vl, er = self.runtest(cmd)
                th.join()
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                # print(er)
                try:
                    self.assertEqual(
                        'INFO : BeamtimeWatcher: Adding watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Creating DatasetWatcher '
                        '{dslist}\n'
                        'INFO : DatasetWatcher: Adding watch {cnt5}: '
                        '{dslist} {idslist}\n'
                        'INFO : DatasetWatcher: Waiting datasets: '
                        '[\'{sc1}\', \'{sc2}\']\n'
                        'INFO : DatasetWatcher: Ingested datasets: []\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc1}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc3}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc3} {subdir2}/{sc3}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc3} {subdir2}/{sc3}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc4}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc4} {subdir2}/{sc4}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc4} {subdir2}/{sc4}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc4}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc4}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Stopping DatasetWatcher '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{dslist}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                subdir=fsubdirname, subdir2=fsubdirname2,
                                dslist=fdslist, idslist=fidslist,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4),
                                sc1='myscan_00001', sc2='myscan_00002',
                                sc3='myscan_00003', sc4='myscan_00004'),
                        "\n".join(dseri))
                except Exception:
                    print(er)
                    raise
                self.assertEqual(
                    'Login: myingestor\n'
                    "RawDatasets: 99001234/myscan_00001\n"
                    "OrigDatablocks: /99001234/myscan_00001\n"
                    "RawDatasets: 99001234/myscan_00002\n"
                    "OrigDatablocks: /99001234/myscan_00002\n"
                    'Login: myingestor\n'
                    "RawDatasets: 99001234/myscan_00003\n"
                    "OrigDatablocks: /99001234/myscan_00003\n"
                    "RawDatasets: 99001234/myscan_00004\n"
                    "OrigDatablocks: /99001234/myscan_00004\n", vl)
                self.assertEqual(len(self.__server.userslogin), 2)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 4)
                for i in range(4):
                    self.myAssertDict(
                        json.loads(self.__server.datasets[i]),
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
                         'pid': '99001234/myscan_%05i' % (i + 1),
                         'datasetName': 'myscan_%05i' % (i + 1),
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'principalInvestigator': 'appuser@fake.com',
                         'ownerGroup': '99001234-dmgt',
                         'proposalId': '99001234',
                         'scientificMetadata': {
                             'DOOR_proposalId': '99991173',
                             'beamtimeId': '99001234'},
                         'sourceFolder':
                         '/asap3/petra3/gpfs/p00/2022/data/9901234/'
                         'raw/special',
                         'type': 'raw',
                         'updatedAt': '2022-05-14 11:54:29'})

                self.assertEqual(len(self.__server.origdatablocks), 4)
                for i in range(4):
                    self.myAssertDict(
                        json.loads(self.__server.origdatablocks[i]),
                        {'dataFileList': [
                            {'gid': 'jkotan',
                             'path': 'myscan_00001.scan.json',
                             'perm': '-rw-r--r--',
                             'size': 629,
                             'time': '2022-07-05T19:07:16.683673+0200',
                             'uid': 'jkotan'}],
                         'ownerGroup': '99001234-dmgt',
                         'datasetId':
                         '/99001234/myscan_%05i' % (i + 1),
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_exist_attachment_no(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
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
        fullbtmeta = os.path.join(fdirname, btmeta)
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
            'scicat_users_login_path: "Users/login"\n' \
            'scicat_datasets_path: "RawDatasets"\n' \
            'scicat_datablocks_path: "OrigDatablocks"\n' \
            'ingest_dataset_attachment: true\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        logfname = "%s_%s.log" % (self.__class__.__name__, fun)
        logfname1 = "%s_%s.log.1" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingestor -c %s -f %s -r10 '
                     % (cfgfname, logfname)).split(),
                    ('scicat_dataset_ingestor --config %s --log-file %s '
                     ' -r10 '
                     % (cfgfname, logfname)).split()]
        # commands.pop()
        lastlog = None
        try:
            for cmd in commands:
                time.sleep(1)
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                os.mkdir(fsubdirname3)
                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)
                with open(logfname) as lfl:
                    er = lfl.read()

                if lastlog:
                    with open(logfname1) as lfl:
                        er2 = lfl.read()
                    self.assertEqual(er2, lastlog)
                lastlog = er

                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]
                # print(vl)
                # print(er)

                # nodebug = "\n".join([ee for ee in er.split("\n")
                #                      if (("DEBUG :" not in ee) and
                #                          (not ee.startswith("127.0.0.1")))])
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                try:
                    self.assertEqual(
                        'INFO : BeamtimeWatcher: Adding watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Creating DatasetWatcher '
                        '{dslist}\n'
                        'INFO : DatasetWatcher: Adding watch {cnt5}: '
                        '{dslist} {idslist}\n'
                        'INFO : DatasetWatcher: Waiting datasets: '
                        '[\'{sc1}\', \'{sc2}\']\n'
                        'INFO : DatasetWatcher: Ingested datasets: []\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc1}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc2}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Stopping DatasetWatcher '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{dslist}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                subdir=fsubdirname, subdir2=fsubdirname2,
                                dslist=fdslist, idslist=fidslist,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4),
                                sc1='myscan_00001', sc2='myscan_00002'),
                        '\n'.join(dseri))
                except Exception:
                    print(er)
                    raise
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
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
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
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
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
                     'ownerGroup': '99001234-dmgt',
                     'datasetId': '/99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
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
                     'ownerGroup': '99001234-dmgt',
                     'datasetId': '/99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.exists(logfname):
                os.remove(logfname)
            if os.path.exists(logfname1):
                os.remove(logfname1)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_add_attachment_png(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
        os.mkdir(fdirname)
        btmeta = "beamtime-metadata-99001234.json"
        dslist = "sc-ds-99001234.lst"
        idslist = "sc-ids-99001234.lst"
        # wrongdslist = "scicat-datasets-99001235.lst"
        cpng = "myscan_00003.png"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        psource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               cpng)
        shutil.copy(source, fdirname)
        # shutil.copy(lsource, fsubdirname2)
        # shutil.copy(wlsource, fsubdirname)
        fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        username = "myingestor"
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'inotify_timeout: 0.2\n' \
            'get_event_timeout: 0.02\n' \
            'ingestion_delay_time: 2\n' \
            'max_request_tries_number: 10\n' \
            'recheck_beamtime_file_interval: 1000\n' \
            'rrecheck_dataset_list_interval: 1000\n' \
            'request_headers:\n' \
            '  "Content-Type": "application/json"\n' \
            '  "Accept": "application/json"\n' \
            'datasets_filename_pattern: "sc-ds-{{beamtimeid}}.lst"\n' \
            'ingested_datasets_filename_pattern: ' \
            '"sc-ids-{{beamtimeid}}.lst"\n' \
            'ingest_dataset_attachment: true\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_username: "{username}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir,
                username=username, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)

        commands = [('scicat_dataset_ingestor -c %s -r36 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r36 -l debug'
                     % cfgfname).split()]

        def tst_thread():
            """ test thread which adds and removes beamtime metadata file """
            time.sleep(3)
            shutil.copy(lsource, fsubdirname2)
            shutil.copy(psource, fsubdirname2)
            time.sleep(5)
            os.mkdir(fsubdirname3)
            time.sleep(12)
            with open(fdslist, "a+") as fds:
                fds.write("myscan_00003\n")
                fds.write("myscan_00004\n")

        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                # print(cmd)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                shutil.copy(lsource, fsubdirname2)
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                th = threading.Thread(target=tst_thread)
                th.start()
                vl, er = self.runtest(cmd)
                th.join()
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                # print(er)
                try:
                    self.assertEqual(
                        'INFO : BeamtimeWatcher: Adding watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Creating DatasetWatcher '
                        '{dslist}\n'
                        'INFO : DatasetWatcher: Adding watch {cnt5}: '
                        '{dslist} {idslist}\n'
                        'INFO : DatasetWatcher: Waiting datasets: '
                        '[\'{sc1}\', \'{sc2}\']\n'
                        'INFO : DatasetWatcher: Ingested datasets: []\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc1}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc3}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc3} {subdir2}/{sc3}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc3} {subdir2}/{sc3}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata: {sc3}'
                        ' {subdir2}/{sc3}.attachment.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc4}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc4} {subdir2}/{sc4}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc4} {subdir2}/{sc4}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc4}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc4}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Stopping DatasetWatcher '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{dslist}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                subdir=fsubdirname, subdir2=fsubdirname2,
                                dslist=fdslist, idslist=fidslist,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4),
                                sc1='myscan_00001', sc2='myscan_00002',
                                sc3='myscan_00003', sc4='myscan_00004'),
                        "\n".join(dseri))
                except Exception:
                    print(er)
                    raise
                self.assertEqual(
                    'Login: myingestor\n'
                    "RawDatasets: 99001234/myscan_00001\n"
                    "OrigDatablocks: /99001234/myscan_00001\n"
                    "RawDatasets: 99001234/myscan_00002\n"
                    "OrigDatablocks: /99001234/myscan_00002\n"
                    'Login: myingestor\n'
                    "RawDatasets: 99001234/myscan_00003\n"
                    "OrigDatablocks: /99001234/myscan_00003\n"
                    "Datasets Attachments: /99001234/myscan_00003\n"
                    "RawDatasets: 99001234/myscan_00004\n"
                    "OrigDatablocks: /99001234/myscan_00004\n", vl)
                self.assertEqual(len(self.__server.userslogin), 2)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 4)
                for i in range(4):
                    self.myAssertDict(
                        json.loads(self.__server.datasets[i]),
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
                         'pid': '99001234/myscan_%05i' % (i + 1),
                         'datasetName': 'myscan_%05i' % (i + 1),
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'principalInvestigator': 'appuser@fake.com',
                         'ownerGroup': '99001234-dmgt',
                         'proposalId': '99001234',
                         'scientificMetadata': {
                             'DOOR_proposalId': '99991173',
                             'beamtimeId': '99001234'},
                         'sourceFolder':
                         '/asap3/petra3/gpfs/p00/2022/data/9901234/'
                         'raw/special',
                         'type': 'raw',
                         'updatedAt': '2022-05-14 11:54:29'})

                self.assertEqual(len(self.__server.origdatablocks), 4)
                for i in range(4):
                    self.myAssertDict(
                        json.loads(self.__server.origdatablocks[i]),
                        {'dataFileList': [
                            {'gid': 'jkotan',
                             'path': 'myscan_00001.scan.json',
                             'perm': '-rw-r--r--',
                             'size': 629,
                             'time': '2022-07-05T19:07:16.683673+0200',
                             'uid': 'jkotan'}],
                         'ownerGroup': '99001234-dmgt',
                         'datasetId':
                         '/99001234/myscan_%05i' % (i + 1),
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'size': 629}, skip=["dataFileList", "size"])
                self.assertEqual(len(self.__server.attachments), 1)
                self.assertEqual(len(self.__server.attachments[0]), 2)
                self.assertEqual(self.__server.attachments[0][0],
                                 '/99001234/myscan_00003')
                self.myAssertDict(
                    json.loads(self.__server.attachments[0][1]),
                    {
                        'ownerGroup': '99001234-dmgt',
                        'thumbnail':
                        "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAoAAA"
                        "AKCAIAAAACUFjqAAAACXBIWXMAAC4jAAAuIwF4pT92AAAAB3RJTU"
                        "UH5wEbCAAYYJKxWgAAABl0RVh0Q29tbWVudABDcmVhdGVkIHdpdG"
                        "ggR0lNUFeBDhcAAAEQSURBVBjTBcFNTgIxFADg9vVNO1MGCPiDxu"
                        "gK4wIXmpi40GN4Pg/gQbyAKxcaIxojKCoFZtq+vvp98uZ4cjDQ1+"
                        "ejnX5n+uzvH+cXl/XV2fj27iHIBLvdemjt/tbAlqA0Y1qP93qHA4"
                        "PtT78gjKuV1KYSWYpY1WitBiEpppiETAAhZ86CvNdKaY2F7bsATS"
                        "4Je9NFA2SqNeWmjUZXwYfYbLRk9u3aLREQCJJL8LdJbrm0XVtv17"
                        "oDqCnB5vTkCBAYjUlZSQDPHImYBQvgonx5n4EWXIA0pTFlhyKj0q"
                        "iMc7EJ+DSdw6iuikyc+eNzPpv9fi/c69uXW+U2Qm84BKtAZW6D90"
                        "SqqDyDz+CTQFSllv+/oo3kf3+TDAAAAABJRU5ErkJggg==",
                        'accessGroups': [
                            '99001234-dmgt', '99001234-clbt', '99001234-part',
                            'p00dmgt', 'p00staff'],
                    })
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_exist_json(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
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
        fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fds1 = os.path.join(fsubdirname2, "myscan_00001.scan.json")
        fds2 = os.path.join(fsubdirname2, "myscan_00002.scan.json")
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        ds1 = {'contactEmail': 'appuser@fake.com',
               'createdAt': '2022-05-14 11:54:29',
               'creationLocation': '/DESY/PETRA III/P00',
               'instrumentId': '/petra3/p00',
               'description': 'H20 distribution',
               'endTime': '2022-05-19 09:00:00',
               'isPublished': False,
               'techniques': [
                   {
                       'name': 'small angle x-ray scattering',
                       'pid':
                       'http://purl.org/pan-science/PaNET/PaNET01188'
                   }
               ],
               'owner': 'Smithson',
               'ownerGroup': '99001234-dmgt',
               'ownerEmail': 'peter.smithson@fake.de',
               'pid': '99001234/myscan_00001',
               'accessGroups': [
                   '99001234-dmgt', '99001234-clbt', '99001234-part',
                   'p00dmgt', 'p00staff'],
               'datasetName': 'myscan_00001',
               'principalInvestigator': 'appuser@fake.com',
               'proposalId': '99001234',
               'scientificMetadata': {
                   'DOOR_proposalId': '99991173',
                   'beamtimeId': '99001234'},
               'sourceFolder':
               '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
               'type': 'raw',
               'updatedAt': '2022-05-14 11:54:29'}
        ds2 = {'contactEmail': 'appuser@fake.com',
               'createdAt': '2022-05-14 11:54:29',
               'creationLocation': '/DESY/PETRA III/P00',
               'instrumentId': '/petra3/p00',
               'description': 'H20 distribution',
               'endTime': '2022-05-19 09:00:00',
               'isPublished': False,
               'techniques': [
                {
                    'name': 'wide angle x-ray scattering',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01191'
                },
                {
                    'name': 'grazing incidence diffraction',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01098'
                }
               ],
               'owner': 'Smithson',
               'ownerGroup': '99001234-dmgt',
               'ownerEmail': 'peter.smithson@fake.de',
               'pid': '99001234/myscan_00002',
               'accessGroups': [
                   '99001234-dmgt', '99001234-clbt', '99001234-part',
                   'p00dmgt', 'p00staff'],
               'datasetName': 'myscan_00002',
               'principalInvestigator': 'appuser@fake.com',
               'proposalId': '99001234',
               'scientificMetadata': {
                   'DOOR_proposalId': '99991173',
                   'beamtimeId': '99001234'},
               'sourceFolder':
               '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
               'type': 'raw',
               'updatedAt': '2022-05-14 11:54:29'}

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'max_scandir_depth: 2\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingestor -c %s -r10 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r10 -l debug'
                     % cfgfname).split()]
        # commands.pop()
        try:
            for cmd in commands:
                time.sleep(1)
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                os.mkdir(fsubdirname3)
                with open(fds1, "w") as cf:
                    cf.write(json.dumps(ds1))
                with open(fds2, "w") as cf:
                    cf.write(json.dumps(ds2))
                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]
                # print(vl)
                # print(er)

                # nodebug = "\n".join([ee for ee in er.split("\n")
                #                      if (("DEBUG :" not in ee) and
                #                          (not ee.startswith("127.0.0.1")))])
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                try:
                    self.assertEqual(
                        'INFO : BeamtimeWatcher: Adding watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Creating DatasetWatcher '
                        '{dslist}\n'
                        'INFO : DatasetWatcher: Adding watch {cnt5}: '
                        '{dslist} {idslist}\n'
                        'INFO : DatasetWatcher: Waiting datasets: '
                        '[\'{sc1}\', \'{sc2}\']\n'
                        'INFO : DatasetWatcher: Ingested datasets: []\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc1}\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc2}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Stopping DatasetWatcher '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{dslist}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                subdir=fsubdirname, subdir2=fsubdirname2,
                                dslist=fdslist, idslist=fidslist,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4),
                                sc1='myscan_00001', sc2='myscan_00002'),
                        '\n'.join(dseri))
                except Exception:
                    print(er)
                    raise
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
                    json.loads(self.__server.datasets[0]), ds1)
                self.myAssertDict(
                    json.loads(self.__server.datasets[1]), ds2)
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
                     'datasetId': '/99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
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
                     'ownerGroup': '99001234-dmgt',
                     'datasetId': '/99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_exist_json_attachment(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
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
        fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fds1 = os.path.join(fsubdirname2, "myscan_00001.scan.json")
        fds2 = os.path.join(fsubdirname2, "myscan_00002.scan.json")
        fat1 = os.path.join(fsubdirname2, "myscan_00001.attachment.json")
        fat2 = os.path.join(fsubdirname2, "myscan_00002.attachment.json")
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        ds1 = {'contactEmail': 'appuser@fake.com',
               'createdAt': '2022-05-14 11:54:29',
               'creationLocation': '/DESY/PETRA III/P00',
               'instrumentId': '/petra3/p00',
               'description': 'H20 distribution',
               'endTime': '2022-05-19 09:00:00',
               'isPublished': False,
               'techniques': [
                   {
                       'name': 'small angle x-ray scattering',
                       'pid':
                       'http://purl.org/pan-science/PaNET/PaNET01188'
                   }
               ],
               'owner': 'Smithson',
               'ownerGroup': '99001234-dmgt',
               'ownerEmail': 'peter.smithson@fake.de',
               'pid': '99001234/myscan_00001',
               'accessGroups': [
                   '99001234-dmgt', '99001234-clbt', '99001234-part',
                   'p00dmgt', 'p00staff'],
               'datasetName': 'myscan_00001',
               'principalInvestigator': 'appuser@fake.com',
               'proposalId': '99001234',
               'scientificMetadata': {
                   'DOOR_proposalId': '99991173',
                   'beamtimeId': '99001234'},
               'sourceFolder':
               '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
               'type': 'raw',
               'updatedAt': '2022-05-14 11:54:29'}
        ds2 = {'contactEmail': 'appuser@fake.com',
               'createdAt': '2022-05-14 11:54:29',
               'creationLocation': '/DESY/PETRA III/P00',
               'instrumentId': '/petra3/p00',
               'description': 'H20 distribution',
               'endTime': '2022-05-19 09:00:00',
               'isPublished': False,
               'techniques': [
                {
                    'name': 'wide angle x-ray scattering',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01191'
                },
                {
                    'name': 'grazing incidence diffraction',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01098'
                }
               ],
               'owner': 'Smithson',
               'ownerGroup': '99001234-dmgt',
               'ownerEmail': 'peter.smithson@fake.de',
               'pid': '99001234/myscan_00002',
               'accessGroups': [
                   '99001234-dmgt', '99001234-clbt', '99001234-part',
                   'p00dmgt', 'p00staff'],
               'datasetName': 'myscan_00002',
               'principalInvestigator': 'appuser@fake.com',
               'proposalId': '99001234',
               'scientificMetadata': {
                   'DOOR_proposalId': '99991173',
                   'beamtimeId': '99001234'},
               'sourceFolder':
               '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
               'type': 'raw',
               'updatedAt': '2022-05-14 11:54:29'}

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'max_scandir_depth: 2\n' \
            'ingest_dataset_attachment: true\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir, credfile=credfile)

        at1 = {
            'thumbnail': "data:iVBORw0KGgoAAAANSUhEAAAoAAA",
            'ownerGroup': '99001234-dmgt',
            'accessGroups': [
                '99001234-dmgt', '99001234-clbt', '99001234-part',
                'p00dmgt', 'p00staff'],
        }
        at2 = {
            'thumbnail': "data:sdfsAAA",
            'ownerGroup': '99001234-dmgt',
            'accessGroups': [
                '99001234-dmgt', '99001234-clbt', '99001234-part',
                'p00dmgt', 'p00staff'],
        }
        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingestor -c %s -r10 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r10 -l debug'
                     % cfgfname).split()]
        # commands.pop()
        try:
            for cmd in commands:
                time.sleep(1)
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                os.mkdir(fsubdirname3)
                with open(fds1, "w") as cf:
                    cf.write(json.dumps(ds1))
                with open(fds2, "w") as cf:
                    cf.write(json.dumps(ds2))
                with open(fat1, "w") as cf:
                    cf.write(json.dumps(at1))
                with open(fat2, "w") as cf:
                    cf.write(json.dumps(at2))
                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]
                # print(vl)
                # print(er)

                # nodebug = "\n".join([ee for ee in er.split("\n")
                #                      if (("DEBUG :" not in ee) and
                #                          (not ee.startswith("127.0.0.1")))])
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                try:
                    self.assertEqual(
                        'INFO : BeamtimeWatcher: Adding watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Creating DatasetWatcher '
                        '{dslist}\n'
                        'INFO : DatasetWatcher: Adding watch {cnt5}: '
                        '{dslist} {idslist}\n'
                        'INFO : DatasetWatcher: Waiting datasets: '
                        '[\'{sc1}\', \'{sc2}\']\n'
                        'INFO : DatasetWatcher: Ingested datasets: []\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc1}\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc2}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Stopping DatasetWatcher '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{dslist}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                subdir=fsubdirname, subdir2=fsubdirname2,
                                dslist=fdslist, idslist=fidslist,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4),
                                sc1='myscan_00001', sc2='myscan_00002'),
                        '\n'.join(dseri))
                except Exception:
                    print(er)
                    raise
                self.assertEqual(
                    "Login: ingestor\n"
                    "RawDatasets: 99001234/myscan_00001\n"
                    "OrigDatablocks: /99001234/myscan_00001\n"
                    "Datasets Attachments: /99001234/myscan_00001\n"
                    "RawDatasets: 99001234/myscan_00002\n"
                    "OrigDatablocks: /99001234/myscan_00002\n"
                    "Datasets Attachments: /99001234/myscan_00002\n", vl)
                self.assertEqual(len(self.__server.userslogin), 1)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 2)
                self.myAssertDict(
                    json.loads(self.__server.datasets[0]), ds1)
                self.myAssertDict(
                    json.loads(self.__server.datasets[1]), ds2)
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
                     'datasetId': '/99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
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
                     'ownerGroup': '99001234-dmgt',
                     'datasetId': '/99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'size': 629}, skip=["dataFileList", "size"])
                self.assertEqual(len(self.__server.attachments), 2)
                self.assertEqual(len(self.__server.attachments[0]), 2)
                self.assertEqual(self.__server.attachments[0][0],
                                 '/99001234/myscan_00001')
                self.myAssertDict(
                    json.loads(self.__server.attachments[0][1]), at1)
                self.assertEqual(len(self.__server.attachments[1]), 2)
                self.assertEqual(self.__server.attachments[1][0],
                                 '/99001234/myscan_00002')
                self.myAssertDict(
                    json.loads(self.__server.attachments[1][1]), at2)
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_exist_json_depth(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
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
        fullbtmeta = os.path.join(fdirname, btmeta)
        # fdslist = os.path.join(fsubdirname2, dslist)
        fds1 = os.path.join(fsubdirname2, "myscan_00001.scan.json")
        fds2 = os.path.join(fsubdirname2, "myscan_00002.scan.json")
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        ds1 = {'contactEmail': 'appuser@fake.com',
               'createdAt': '2022-05-14 11:54:29',
               'creationLocation': '/DESY/PETRA III/P00',
               'instrumentId': '/petra3/p00',
               'description': 'H20 distribution',
               'endTime': '2022-05-19 09:00:00',
               'isPublished': False,
               'techniques': [
                   {
                       'name': 'small angle x-ray scattering',
                       'pid':
                       'http://purl.org/pan-science/PaNET/PaNET01188'
                   }
               ],
               'owner': 'Smithson',
               'ownerGroup': '99001234-dmgt',
               'ownerEmail': 'peter.smithson@fake.de',
               'pid': '99001234/myscan_00001',
               'accessGroups': [
                   '99001234-dmgt', '99001234-clbt', '99001234-part',
                   'p00dmgt', 'p00staff'],
               'datasetName': 'myscan_00001',
               'principalInvestigator': 'appuser@fake.com',
               'proposalId': '99001234',
               'scientificMetadata': {
                   'DOOR_proposalId': '99991173',
                   'beamtimeId': '99001234'},
               'sourceFolder':
               '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
               'type': 'raw',
               'updatedAt': '2022-05-14 11:54:29'}
        ds2 = {'contactEmail': 'appuser@fake.com',
               'createdAt': '2022-05-14 11:54:29',
               'creationLocation': '/DESY/PETRA III/P00',
               'instrumentId': '/petra3/p00',
               'description': 'H20 distribution',
               'endTime': '2022-05-19 09:00:00',
               'isPublished': False,
               'techniques': [
                {
                    'name': 'wide angle x-ray scattering',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01191'
                },
                {
                    'name': 'grazing incidence diffraction',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01098'
                }
               ],
               'owner': 'Smithson',
               'ownerGroup': '99001234-dmgt',
               'ownerEmail': 'peter.smithson@fake.de',
               'pid': '99001234/myscan_00002',
               'accessGroups': [
                   '99001234-dmgt', '99001234-clbt', '99001234-part',
                   'p00dmgt', 'p00staff'],
               'datasetName': 'myscan_00002',
               'principalInvestigator': 'appuser@fake.com',
               'proposalId': '99001234',
               'scientificMetadata': {
                   'DOOR_proposalId': '99991173',
                   'beamtimeId': '99001234'},
               'sourceFolder':
               '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
               'type': 'raw',
               'updatedAt': '2022-05-14 11:54:29'}

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'max_scandir_depth: 1\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingestor -c %s -r10 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r10 -l debug'
                     % cfgfname).split()]
        # commands.pop()
        try:
            for cmd in commands:
                time.sleep(1)
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                os.mkdir(fsubdirname3)
                with open(fds1, "w") as cf:
                    cf.write(json.dumps(ds1))
                with open(fds2, "w") as cf:
                    cf.write(json.dumps(ds2))
                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]
                # print(vl)
                # print(er)

                # nodebug = "\n".join([ee for ee in er.split("\n")
                #                      if (("DEBUG :" not in ee) and
                #                          (not ee.startswith("127.0.0.1")))])
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                try:
                    self.assertEqual(
                        'INFO : BeamtimeWatcher: Adding watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                subdir=fsubdirname,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2)),
                        '\n'.join(dseri))
                except Exception:
                    print(er)
                    raise
                self.assertEqual("", vl)
                self.assertEqual(len(self.__server.userslogin), 0)
                self.assertEqual(len(self.__server.datasets), 0)
                self.assertEqual(len(self.__server.origdatablocks), 0)
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_add_json(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
        os.mkdir(fdirname)
        btmeta = "beamtime-metadata-99001234.json"
        dslist = "scicat-datasets-99001234.lst"
        idslist = "scicat-ingested-datasets-99001234.lst"
        # wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        shutil.copy(source, fdirname)
        # shutil.copy(lsource, fsubdirname2)
        # shutil.copy(wlsource, fsubdirname)
        fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        username = "myingestor"
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'max_scandir_depth: 2\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_username: "{username}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir,
                username=username, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)

        dss = []
        fdss = []
        for i in range(4):
            fdss.append(
                os.path.join(fsubdirname2, 'myscan_%05i.scan.json' % (i + 1)))
            dss.append(
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
                 'pid': '99001234/myscan_%05i' % (i + 1),
                 'datasetName': 'myscan_%05i' % (i + 1),
                 'accessGroups': [
                     '99001234-dmgt', '99001234-clbt', '99001234-part',
                     'p00dmgt', 'p00staff'],
                 'principalInvestigator': 'appuser@fake.com',
                 'ownerGroup': '99001234-dmgt',
                 'proposalId': '99001234',
                 'scientificMetadata': {
                     'DOOR_proposalId': '99991173',
                     'beamtimeId': '99001234'},
                 'sourceFolder':
                 '/asap3/petra3/gpfs/p00/2022/data/9901234/'
                 'raw/special',
                 'type': 'raw',
                 'updatedAt': '2022-05-14 11:54:29'})

        commands = [('scicat_dataset_ingestor -c %s -r36 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r36 -l debug'
                     % cfgfname).split()]

        def tst_thread():
            """ test thread which adds and removes beamtime metadata file """
            time.sleep(3)
            shutil.copy(lsource, fsubdirname2)
            time.sleep(5)
            os.mkdir(fsubdirname3)
            time.sleep(12)
            with open(fdslist, "a+") as fds:
                fds.write("myscan_00003\n")
                fds.write("myscan_00004\n")

        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                for i in range(4):
                    with open(fdss[i], "w") as cf:
                        cf.write(json.dumps(dss[i]))
                # print(cmd)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                shutil.copy(lsource, fsubdirname2)
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                th = threading.Thread(target=tst_thread)
                th.start()
                vl, er = self.runtest(cmd)
                th.join()
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                # print(er)
                try:
                    self.assertEqual(
                        'INFO : BeamtimeWatcher: Adding watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Creating DatasetWatcher '
                        '{dslist}\n'
                        'INFO : DatasetWatcher: Adding watch {cnt5}: '
                        '{dslist} {idslist}\n'
                        'INFO : DatasetWatcher: Waiting datasets: '
                        '[\'{sc1}\', \'{sc2}\']\n'
                        'INFO : DatasetWatcher: Ingested datasets: []\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc1}\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc3}\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc3} {subdir2}/{sc3}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc4}\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc4} {subdir2}/{sc4}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc4}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc4}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Stopping DatasetWatcher '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{dslist}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                subdir=fsubdirname, subdir2=fsubdirname2,
                                dslist=fdslist, idslist=fidslist,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4),
                                sc1='myscan_00001', sc2='myscan_00002',
                                sc3='myscan_00003', sc4='myscan_00004'),
                        "\n".join(dseri))
                except Exception:
                    print(er)
                    raise
                self.assertEqual(
                    'Login: myingestor\n'
                    "RawDatasets: 99001234/myscan_00001\n"
                    "OrigDatablocks: /99001234/myscan_00001\n"
                    "RawDatasets: 99001234/myscan_00002\n"
                    "OrigDatablocks: /99001234/myscan_00002\n"
                    'Login: myingestor\n'
                    "RawDatasets: 99001234/myscan_00003\n"
                    "OrigDatablocks: /99001234/myscan_00003\n"
                    "RawDatasets: 99001234/myscan_00004\n"
                    "OrigDatablocks: /99001234/myscan_00004\n", vl)
                self.assertEqual(len(self.__server.userslogin), 2)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 4)
                for i in range(4):
                    self.myAssertDict(
                        json.loads(self.__server.datasets[i]), dss[i])

                self.assertEqual(len(self.__server.origdatablocks), 4)
                for i in range(4):
                    self.myAssertDict(
                        json.loads(self.__server.origdatablocks[i]),
                        {'dataFileList': [
                            {'gid': 'jkotan',
                             'path': 'myscan_00001.scan.json',
                             'perm': '-rw-r--r--',
                             'size': 629,
                             'time': '2022-07-05T19:07:16.683673+0200',
                             'uid': 'jkotan'}],
                         'ownerGroup': '99001234-dmgt',
                         'datasetId':
                         '/99001234/myscan_%05i' % (i + 1),
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_add_json_attachment(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
        os.mkdir(fdirname)
        btmeta = "beamtime-metadata-99001234.json"
        dslist = "scicat-datasets-99001234.lst"
        idslist = "scicat-ingested-datasets-99001234.lst"
        # wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        shutil.copy(source, fdirname)
        # shutil.copy(lsource, fsubdirname2)
        # shutil.copy(wlsource, fsubdirname)
        fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        username = "myingestor"
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'max_scandir_depth: 2\n' \
            'ingest_dataset_attachment: true\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_username: "{username}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir,
                username=username, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)

        dss = []
        fdss = []
        ats = []
        fats = []
        for i in range(4):
            fdss.append(
                os.path.join(fsubdirname2, 'myscan_%05i.scan.json' % (i + 1)))
            dss.append(
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
                 'pid': '99001234/myscan_%05i' % (i + 1),
                 'datasetName': 'myscan_%05i' % (i + 1),
                 'accessGroups': [
                     '99001234-dmgt', '99001234-clbt', '99001234-part',
                     'p00dmgt', 'p00staff'],
                 'principalInvestigator': 'appuser@fake.com',
                 'ownerGroup': '99001234-dmgt',
                 'proposalId': '99001234',
                 'scientificMetadata': {
                     'DOOR_proposalId': '99991173',
                     'beamtimeId': '99001234'},
                 'sourceFolder':
                 '/asap3/petra3/gpfs/p00/2022/data/9901234/'
                 'raw/special',
                 'type': 'raw',
                 'updatedAt': '2022-05-14 11:54:29'})
            fats.append(
                os.path.join(
                    fsubdirname2, 'myscan_%05i.attachment.json' % (i + 1)))
            ats.append({
                'thumbnail': "data:sdfsAAA%s" % i,
                'ownerGroup': '99001234-dmgt',
                'accessGroups': [
                    '99001234-dmgt', '99001234-clbt', '99001234-part',
                    'p00dmgt', 'p00staff']})
        commands = [('scicat_dataset_ingestor -c %s -r36 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r36 -l debug'
                     % cfgfname).split()]

        def tst_thread():
            """ test thread which adds and removes beamtime metadata file """
            time.sleep(3)
            shutil.copy(lsource, fsubdirname2)
            time.sleep(5)
            os.mkdir(fsubdirname3)
            time.sleep(12)
            with open(fdslist, "a+") as fds:
                fds.write("myscan_00003\n")
                fds.write("myscan_00004\n")

        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                for i in range(4):
                    with open(fdss[i], "w") as cf:
                        cf.write(json.dumps(dss[i]))
                    with open(fats[i], "w") as cf:
                        cf.write(json.dumps(ats[i]))
                # print(cmd)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                shutil.copy(lsource, fsubdirname2)
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                th = threading.Thread(target=tst_thread)
                th.start()
                vl, er = self.runtest(cmd)
                th.join()
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                # print(er)
                try:
                    self.assertEqual(
                        'INFO : BeamtimeWatcher: Adding watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Creating DatasetWatcher '
                        '{dslist}\n'
                        'INFO : DatasetWatcher: Adding watch {cnt5}: '
                        '{dslist} {idslist}\n'
                        'INFO : DatasetWatcher: Waiting datasets: '
                        '[\'{sc1}\', \'{sc2}\']\n'
                        'INFO : DatasetWatcher: Ingested datasets: []\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc1}\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc3}\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc3} {subdir2}/{sc3}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc4}\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc4} {subdir2}/{sc4}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc4}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc4}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Stopping DatasetWatcher '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{dslist}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                subdir=fsubdirname, subdir2=fsubdirname2,
                                dslist=fdslist, idslist=fidslist,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4),
                                sc1='myscan_00001', sc2='myscan_00002',
                                sc3='myscan_00003', sc4='myscan_00004'),
                        "\n".join(dseri))
                except Exception:
                    print(er)
                    raise
                self.assertEqual(
                    'Login: myingestor\n'
                    "RawDatasets: 99001234/myscan_00001\n"
                    "OrigDatablocks: /99001234/myscan_00001\n"
                    "Datasets Attachments: /99001234/myscan_00001\n"
                    "RawDatasets: 99001234/myscan_00002\n"
                    "OrigDatablocks: /99001234/myscan_00002\n"
                    "Datasets Attachments: /99001234/myscan_00002\n"
                    'Login: myingestor\n'
                    "RawDatasets: 99001234/myscan_00003\n"
                    "OrigDatablocks: /99001234/myscan_00003\n"
                    "Datasets Attachments: /99001234/myscan_00003\n"
                    "RawDatasets: 99001234/myscan_00004\n"
                    "OrigDatablocks: /99001234/myscan_00004\n"
                    "Datasets Attachments: /99001234/myscan_00004\n", vl)
                self.assertEqual(len(self.__server.userslogin), 2)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 4)
                for i in range(4):
                    self.myAssertDict(
                        json.loads(self.__server.datasets[i]), dss[i])

                self.assertEqual(len(self.__server.origdatablocks), 4)
                for i in range(4):
                    self.myAssertDict(
                        json.loads(self.__server.origdatablocks[i]),
                        {'dataFileList': [
                            {'gid': 'jkotan',
                             'path': 'myscan_00001.scan.json',
                             'perm': '-rw-r--r--',
                             'size': 629,
                             'time': '2022-07-05T19:07:16.683673+0200',
                             'uid': 'jkotan'}],
                         'ownerGroup': '99001234-dmgt',
                         'datasetId':
                         '/99001234/myscan_%05i' % (i + 1),
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'size': 629}, skip=["dataFileList", "size"])

                self.assertEqual(len(self.__server.attachments), 4)
                for i in range(4):
                    self.assertEqual(len(self.__server.attachments[i]), 2)
                    self.assertEqual(self.__server.attachments[i][0],
                                     '/99001234/myscan_%05i' % (i + 1))
                    self.myAssertDict(
                        json.loads(self.__server.attachments[i][1]), ats[i])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_add_json_depth(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
        os.mkdir(fdirname)
        btmeta = "beamtime-metadata-99001234.json"
        dslist = "scicat-datasets-99001234.lst"
        idslist = "scicat-ingested-datasets-99001234.lst"
        # wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        shutil.copy(source, fdirname)
        # shutil.copy(lsource, fsubdirname2)
        # shutil.copy(wlsource, fsubdirname)
        fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        username = "myingestor"
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'max_scandir_depth: 1\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_username: "{username}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir,
                username=username, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)

        dss = []
        fdss = []
        for i in range(4):
            fdss.append(
                os.path.join(fsubdirname2, 'myscan_%05i.scan.json' % (i + 1)))
            dss.append(
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
                 'pid': '99001234/myscan_%05i' % (i + 1),
                 'datasetName': 'myscan_%05i' % (i + 1),
                 'accessGroups': [
                     '99001234-dmgt', '99001234-clbt', '99001234-part',
                     'p00dmgt', 'p00staff'],
                 'principalInvestigator': 'appuser@fake.com',
                 'ownerGroup': '99001234-dmgt',
                 'proposalId': '99001234',
                 'scientificMetadata': {
                     'DOOR_proposalId': '99991173',
                     'beamtimeId': '99001234'},
                 'sourceFolder':
                 '/asap3/petra3/gpfs/p00/2022/data/9901234/'
                 'raw/special',
                 'type': 'raw',
                 'updatedAt': '2022-05-14 11:54:29'})

        commands = [('scicat_dataset_ingestor -c %s -r36 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r36 -l debug'
                     % cfgfname).split()]

        def tst_thread():
            """ test thread which adds and removes beamtime metadata file """
            time.sleep(3)
            shutil.copy(lsource, fsubdirname2)
            time.sleep(5)
            os.mkdir(fsubdirname3)
            time.sleep(12)
            with open(fdslist, "a+") as fds:
                fds.write("myscan_00003\n")
                fds.write("myscan_00004\n")

        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                for i in range(4):
                    with open(fdss[i], "w") as cf:
                        cf.write(json.dumps(dss[i]))
                # print(cmd)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                shutil.copy(lsource, fsubdirname2)
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                th = threading.Thread(target=tst_thread)
                th.start()
                vl, er = self.runtest(cmd)
                th.join()
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                # print(er)
                try:
                    self.assertEqual(
                        'INFO : BeamtimeWatcher: Adding watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                subdir=fsubdirname,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2)),
                        "\n".join(dseri))
                except Exception:
                    print(er)
                    raise
                self.assertEqual("", vl)
                self.assertEqual(len(self.__server.userslogin), 0)
                self.assertEqual(len(self.__server.datasets), 0)
                self.assertEqual(len(self.__server.origdatablocks), 0)
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_exist_relpath(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
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
        fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        dspostfix = ".dataset.json"
        datablockpostfix = ".datablock.json"
        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'scan_metadata_postfix: "{dspostfix}"\n' \
            'datablock_metadata_postfix: "{datablockpostfix}"\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'relative_path_in_datablock: true\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir, credfile=credfile,
                dspostfix=dspostfix, datablockpostfix=datablockpostfix)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingestor -c %s -r10 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r10 -l debug'
                     % cfgfname).split()]
        # commands.pop()
        try:
            for cmd in commands:
                time.sleep(1)
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                os.mkdir(fsubdirname3)
                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]
                # print(vl)
                # print(er)
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                try:
                    self.assertEqual(
                        'INFO : BeamtimeWatcher: Adding watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Creating DatasetWatcher '
                        '{dslist}\n'
                        'INFO : DatasetWatcher: Adding watch {cnt5}: '
                        '{dslist} {idslist}\n'
                        'INFO : DatasetWatcher: Waiting datasets: '
                        '[\'{sc1}\', \'{sc2}\']\n'
                        'INFO : DatasetWatcher: Ingested datasets: []\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc1}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc1} {subdir2}/{sc1}.dataset.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.datablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc2} {subdir2}/{sc2}.dataset.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.datablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc2}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Stopping DatasetWatcher '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{dslist}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                subdir=fsubdirname, subdir2=fsubdirname2,
                                dslist=fdslist, idslist=fidslist,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4),
                                sc1='myscan_00001', sc2='myscan_00002'),
                        "\n".join(dseri))
                except Exception:
                    print(er)
                    raise
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
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'datasetName': 'myscan_00001',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234',
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
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'datasetName': 'myscan_00002',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.assertEqual(len(self.__server.origdatablocks), 2)
                # print(self.__server.origdatablocks)
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[0]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.dataset.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'ownerGroup': '99001234-dmgt',
                     'datasetId': '/99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[1]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.dataset.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'ownerGroup': '99001234-dmgt',
                     'datasetId': '/99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_add_relpath(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
        os.mkdir(fdirname)
        btmeta = "beamtime-metadata-99001234.json"
        dslist = "scicat-datasets-99001234.lst"
        idslist = "scicat-ingested-datasets-99001234.lst"
        # wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        shutil.copy(source, fdirname)
        # shutil.copy(lsource, fsubdirname2)
        # shutil.copy(wlsource, fsubdirname)
        fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        username = "myingestor"
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'relative_path_in_datablock: true\n' \
            'ingestor_username: "{username}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir,
                username=username, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)

        commands = [('scicat_dataset_ingestor -c %s -r38 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r38 -l debug'
                     % cfgfname).split()]

        def tst_thread():
            """ test thread which adds and removes beamtime metadata file """
            time.sleep(3)
            shutil.copy(lsource, fsubdirname2)
            time.sleep(5)
            os.mkdir(fsubdirname3)
            time.sleep(12)
            with open(fdslist, "a+") as fds:
                fds.write("myscan_00003\n")
                fds.write("myscan_00004\n")

        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                # print(cmd)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                shutil.copy(lsource, fsubdirname2)
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                th = threading.Thread(target=tst_thread)
                th.start()
                vl, er = self.runtest(cmd)
                th.join()
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                # print(er)
                try:
                    pattern = self.sortmarkedlines(
                        'INFO : BeamtimeWatcher: Adding watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Creating DatasetWatcher '
                        '{dslist}\n'
                        'INFO : DatasetWatcher: Adding watch {cnt5}: '
                        '{dslist} {idslist}\n'
                        'INFO : DatasetWatcher: Waiting datasets: '
                        '[\'{sc1}\', \'{sc2}\']\n'
                        'INFO : DatasetWatcher: Ingested datasets: []\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc1}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc3}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc3} {subdir2}/{sc3}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc3} {subdir2}/{sc3}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc4}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc4} {subdir2}/{sc4}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc4} {subdir2}/{sc4}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc4}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc4}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Stopping DatasetWatcher '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{dslist}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                subdir=fsubdirname, subdir2=fsubdirname2,
                                dslist=fdslist, idslist=fidslist,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4),
                                sc1='myscan_00001', sc2='myscan_00002',
                                sc3='myscan_00003', sc4='myscan_00004'),
                        [(30, 39)], {'watch [0-9]:': 'watch:'})
                    self.assertEqual(
                        pattern, self.sortmarkedlines(
                            dseri, [(30, 39)], {'watch [0-9]:': 'watch:'}))
                except Exception:
                    print(er)
                    raise
                self.assertEqual(
                    'Login: myingestor\n'
                    "RawDatasets: 99001234/myscan_00001\n"
                    "OrigDatablocks: /99001234/myscan_00001\n"
                    "RawDatasets: 99001234/myscan_00002\n"
                    "OrigDatablocks: /99001234/myscan_00002\n"
                    'Login: myingestor\n'
                    "RawDatasets: 99001234/myscan_00003\n"
                    "OrigDatablocks: /99001234/myscan_00003\n"
                    "RawDatasets: 99001234/myscan_00004\n"
                    "OrigDatablocks: /99001234/myscan_00004\n", vl)
                self.assertEqual(len(self.__server.userslogin), 2)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 4)
                for i in range(4):
                    self.myAssertDict(
                        json.loads(self.__server.datasets[i]),
                        {'contactEmail': 'appuser@fake.com',
                         'createdAt': '2022-05-14 11:54:29',
                         'creationLocation': '/DESY/PETRA III/P00',
                         'instrumentId': '/petra3/p00',
                         'description': 'H20 distribution',
                         'endTime': '2022-05-19 09:00:00',
                         'isPublished': False,
                         'techniques': [],
                         'owner': 'Smithson',
                         'ownerEmail': 'peter.smithson@fake.de',
                         'pid': '99001234/myscan_%05i' % (i + 1),
                         'datasetName': 'myscan_%05i' % (i + 1),
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'principalInvestigator': 'appuser@fake.com',
                         'ownerGroup': '99001234-dmgt',
                         'proposalId': '99001234',
                         'scientificMetadata': {
                             'DOOR_proposalId': '99991173',
                             'beamtimeId': '99001234'},
                         'sourceFolder':
                         '/asap3/petra3/gpfs/p00/2022/data/9901234',
                         'type': 'raw',
                         'updatedAt': '2022-05-14 11:54:29'})

                # print(self.__server.origdatablocks)
                self.assertEqual(len(self.__server.origdatablocks), 4)
                for i in range(4):
                    self.myAssertDict(
                        json.loads(self.__server.origdatablocks[i]),
                        {'dataFileList': [
                            {'gid': 'jkotan',
                             'path': 'myscan_00001.scan.json',
                             'perm': '-rw-r--r--',
                             'size': 629,
                             'time': '2022-07-05T19:07:16.683673+0200',
                             'uid': 'jkotan'}],
                         'ownerGroup': '99001234-dmgt',
                         'datasetId':
                         '/99001234/myscan_%05i' % (i + 1),
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_add_relpath_script(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
        os.mkdir(fdirname)
        btmeta = "beamtime-metadata-99001234.json"
        dslist = "scicat-datasets-99001234.lst"
        idslist = "scicat-ingested-datasets-99001234.lst"
        # wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        shutil.copy(source, fdirname)
        # shutil.copy(lsource, fsubdirname2)
        # shutil.copy(wlsource, fsubdirname)
        fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        username = "myingestor"
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'dataset_metadata_generator: "nxsfileinfo metadata ' \
            ' -o {{scanpath}}/{{scanname}}{{scanpostfix}} ' \
            ' -x 0o662 ' \
            ' -b {{beamtimefile}} -p {{beamtimeid}}/{{scanname}} "\n' \
            'datablock_metadata_generator: "nxsfileinfo origdatablock ' \
            ' -s *.pyc,*{{datablockpostfix}},*{{scanpostfix}},*~ ' \
            ' -p {{pidprefix}}/{{beamtimeid}}/{{scanname}} ' \
            ' -x 0o662 ' \
            ' -r {{relpath}} ' \
            ' -c {{beamtimeid}}-clbt,{{beamtimeid}}-dmgt,{{beamline}}dmgt ' \
            ' -o {{scanpath}}/{{scanname}}{{datablockpostfix}} "\n' \
            'datablock_metadata_stream_generator: ' \
            'nxsfileinfo origdatablock ' \
            ' -s *.pyc,*{{datablockpostfix}},*{{scanpostfix}},*~ ' \
            ' -c {{beamtimeid}}-clbt,{{beamtimeid}}-dmgt,{{beamline}}dmgt' \
            ' -r {{relpath}} ' \
            ' -x 0o662 ' \
            ' -p {{pidprefix}}/{{beamtimeid}}/{{scanname}} "\n' \
            'datablock_metadata_generator_scanpath_postfix: '\
            ' " {{scanpath}}/{{scanname}}"\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'relative_path_in_datablock: true\n' \
            'ingestor_username: "{username}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir,
                username=username, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)

        commands = [('scicat_dataset_ingestor -c %s -r36 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r36 -l debug'
                     % cfgfname).split()]

        def tst_thread():
            """ test thread which adds and removes beamtime metadata file """
            time.sleep(3)
            shutil.copy(lsource, fsubdirname2)
            time.sleep(5)
            os.mkdir(fsubdirname3)
            time.sleep(12)
            with open(fdslist, "a+") as fds:
                fds.write("myscan_00003\n")
                fds.write("myscan_00004\n")

        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                # print(cmd)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                shutil.copy(lsource, fsubdirname2)
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                th = threading.Thread(target=tst_thread)
                th.start()
                vl, er = self.runtest(cmd)
                th.join()
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                # print(er)
                try:
                    self.assertEqual(
                        'INFO : BeamtimeWatcher: Adding watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Creating DatasetWatcher '
                        '{dslist}\n'
                        'INFO : DatasetWatcher: Adding watch {cnt5}: '
                        '{dslist} {idslist}\n'
                        'INFO : DatasetWatcher: Waiting datasets: '
                        '[\'{sc1}\', \'{sc2}\']\n'
                        'INFO : DatasetWatcher: Ingested datasets: []\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc1}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc3}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc3} {subdir2}/{sc3}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc3} {subdir2}/{sc3}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc4}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc4} {subdir2}/{sc4}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc4} {subdir2}/{sc4}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001234/{sc4}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001234/{sc4}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Stopping DatasetWatcher '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{dslist}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                subdir=fsubdirname, subdir2=fsubdirname2,
                                dslist=fdslist, idslist=fidslist,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4),
                                sc1='myscan_00001', sc2='myscan_00002',
                                sc3='myscan_00003', sc4='myscan_00004'),
                        "\n".join(dseri))
                except Exception:
                    print(er)
                    raise
                self.assertEqual(
                    'Login: myingestor\n'
                    "RawDatasets: 99001234/myscan_00001\n"
                    "OrigDatablocks: /99001234/myscan_00001\n"
                    "RawDatasets: 99001234/myscan_00002\n"
                    "OrigDatablocks: /99001234/myscan_00002\n"
                    'Login: myingestor\n'
                    "RawDatasets: 99001234/myscan_00003\n"
                    "OrigDatablocks: /99001234/myscan_00003\n"
                    "RawDatasets: 99001234/myscan_00004\n"
                    "OrigDatablocks: /99001234/myscan_00004\n", vl)
                self.assertEqual(len(self.__server.userslogin), 2)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 4)
                for i in range(4):
                    self.myAssertDict(
                        json.loads(self.__server.datasets[i]),
                        {'contactEmail': 'appuser@fake.com',
                         'createdAt': '2022-05-14 11:54:29',
                         'creationLocation': '/DESY/PETRA III/P00',
                         'instrumentId': '/petra3/p00',
                         'description': 'H20 distribution',
                         'endTime': '2022-05-19 09:00:00',
                         'isPublished': False,
                         'techniques': [],
                         'owner': 'Smithson',
                         'ownerEmail': 'peter.smithson@fake.de',
                         'pid': '99001234/myscan_%05i' % (i + 1),
                         'datasetName': 'myscan_%05i' % (i + 1),
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt',
                             '99001234-part', 'p00dmgt', 'p00staff'],
                         'principalInvestigator': 'appuser@fake.com',
                         'ownerGroup': '99001234-dmgt',
                         'proposalId': '99001234',
                         'scientificMetadata': {
                             'DOOR_proposalId': '99991173',
                             'beamtimeId': '99001234'},
                         'sourceFolder':
                         '/asap3/petra3/gpfs/p00/2022/data/9901234',
                         'type': 'raw',
                         'updatedAt': '2022-05-14 11:54:29'})

                # print(self.__server.origdatablocks)
                self.assertEqual(len(self.__server.origdatablocks), 4)
                for i in range(4):
                    self.myAssertDict(
                        json.loads(self.__server.origdatablocks[i]),
                        {'dataFileList': [
                            {'gid': 'jkotan',
                             'path': 'myscan_00001.scan.json',
                             'perm': '-rw-r--r--',
                             'size': 629,
                             'time': '2022-07-05T19:07:16.683673+0200',
                             'uid': 'jkotan'}],
                         'ownerGroup': '99001234-dmgt',
                         'datasetId':
                         '/99001234/myscan_%05i' % (i + 1),
                         'accessGroups': [
                             '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
                         'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_exist_relpath_det(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirnamedet1 = os.path.abspath(os.path.join(fsubdirname, "lambda1"))
        fsubdirnamedet2 = os.path.abspath(os.path.join(fsubdirname, "lambda2"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
        btmeta = "beamtime-metadata-99001236.json"
        dslist = "scicat-datasets-99001236.lst"
        idslist = "scicat-ingested-datasets-99001236.lst"
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
        fullbtmeta = os.path.join(fdirname, btmeta)
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
            'relative_path_in_datablock: true\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingestor -c %s -r10 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r10 -l debug'
                     % cfgfname).split()]
        # commands.pop()
        try:
            for cmd in commands:
                time.sleep(1)
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                os.mkdir(fsubdirnamedet1)
                os.mkdir(fsubdirnamedet2)
                with open(os.path.join(
                        fsubdirnamedet1, "data1.dat"), "w+") as cf:
                    cf.write("12345")
                with open(os.path.join(
                        fsubdirnamedet2, "data2.dat"), "w+") as cf:
                    cf.write("12345")
                os.mkdir(fsubdirname3)
                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]
                # print(vl)
                # print(er)
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                try:
                    pattern = self.sortmarkedlines(
                        'INFO : BeamtimeWatcher: Adding watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{detdir1} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{detdir1}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{detdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt5}: '
                        '{detdir2}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt6}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Creating DatasetWatcher '
                        '{dslist}\n'
                        'INFO : DatasetWatcher: Adding watch {cnt7}: '
                        '{dslist} {idslist}\n'
                        'INFO : DatasetWatcher: Waiting datasets: '
                        '[\'{sc1} {det1}\', \'{sc2} {det2}\']\n'
                        'INFO : DatasetWatcher: Ingested datasets: []\n'
                        'INFO : DatasetIngestor: '
                        'Ingesting: {dslist} {sc1} {det1}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {det1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001236/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001236/{sc1}\n'
                        'INFO : DatasetIngestor: '
                        'Ingesting: {dslist} {sc2} {det2}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {det2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001236/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001236/{sc2}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{detdir1}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{detdir2}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt6}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Stopping DatasetWatcher '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt7}: '
                        '{dslist}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                subdir=fsubdirname, subdir2=fsubdirname2,
                                detdir1=fsubdirnamedet1,
                                detdir2=fsubdirnamedet2,
                                dslist=fdslist, idslist=fidslist,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4),
                                cnt6=(cnt + 5), cnt7=(cnt + 6),
                                det1="../lambda1/", det2="../lambda2/",
                                sc1='myscan_00001', sc2='myscan_00002'),
                        [(5, 39)], {'watch [0-9]:': 'watch:'})
                    self.assertEqual(
                        pattern, self.sortmarkedlines(
                            dseri, [(5, 39)], {'watch [0-9]:': 'watch:'}))
                except Exception:
                    print(er)
                    raise
                self.assertEqual(
                    "Login: ingestor\n"
                    "RawDatasets: 99001236/myscan_00001\n"
                    "OrigDatablocks: /99001236/myscan_00001\n"
                    "RawDatasets: 99001236/myscan_00002\n"
                    "OrigDatablocks: /99001236/myscan_00002\n", vl)
                self.assertEqual(len(self.__server.userslogin), 1)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 2)
                self.myAssertDict(
                    json.loads(self.__server.datasets[0]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'creationLocation': '/DESY/FS-SC/SDD01',
                     'instrumentId': '/fs-sc/sdd01',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerGroup': '99001236-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001236/myscan_00001',
                     'accessGroups': [
                         '99001236-dmgt', '99001236-clbt', '99001236-part',
                         'sdd01dmgt', 'sdd01staff'],
                     'datasetName': 'myscan_00001',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001236',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001236'},
                     'sourceFolder':
                     '/asap3/fs-sc/gpfs/p00/2022/data/9901236',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[1]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/fs-sc/sdd01',
                     'creationLocation': '/DESY/FS-SC/SDD01',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerGroup': '99001236-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001236/myscan_00002',
                     'accessGroups': [
                         '99001236-dmgt', '99001236-clbt', '99001236-part',
                         'sdd01dmgt', 'sdd01staff'],
                     'datasetName': 'myscan_00002',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001236',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001236'},
                     'sourceFolder':
                     '/asap3/fs-sc/gpfs/p00/2022/data/9901236',
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
                     'ownerGroup': '99001236-dmgt',
                     'datasetId': '/99001236/myscan_00001',
                     'accessGroups': [
                         '99001236-dmgt', '99001236-clbt', '99001236-part',
                         'sdd01dmgt', 'sdd01staff'],
                     'size': 629}, skip=["dataFileList", "size"])
                dfl = json.loads(
                    self.__server.origdatablocks[0])["dataFileList"]
                paths = sorted([df["path"] for df in dfl])
                self.assertEqual(paths, ['raw/lambda1/data1.dat'])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[1]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'ownerGroup': '99001236-dmgt',
                     'datasetId': '/99001236/myscan_00002',
                     'accessGroups': [
                         '99001236-dmgt', '99001236-clbt', '99001236-part',
                         'sdd01dmgt', 'sdd01staff'],
                     'size': 629}, skip=["dataFileList", "size"])
                dfl = json.loads(
                    self.__server.origdatablocks[1])["dataFileList"]
                paths = sorted([df["path"] for df in dfl])
                self.assertEqual(paths, ['raw/lambda2/data2.dat'])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
                if os.path.isdir(vardir):
                    shutil.rmtree(vardir)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_add_relpath_det(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
        fsubdirnamedet1 = os.path.abspath(os.path.join(fsubdirname, "lambda1"))
        fsubdirnamedet2 = os.path.abspath(os.path.join(fsubdirname, "lambda2"))
        fsubdirnamedet3 = os.path.abspath(os.path.join(fsubdirname, "lambda3"))
        fsubdirnamedet4 = os.path.abspath(os.path.join(fsubdirname, "lambda4"))
        os.mkdir(fdirname)
        btmeta = "beamtime-metadata-99001236.json"
        dslist = "scicat-datasets-99001236.lst"
        idslist = "scicat-ingested-datasets-99001236.lst"
        # wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        shutil.copy(source, fdirname)
        # shutil.copy(lsource, fsubdirname2)
        # shutil.copy(wlsource, fsubdirname)
        fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/tmp/scingestor_log_%s" % uuid.uuid4().hex
        fidslist = "%s%s" % (vardir, fidslist)
        cred = "12342345"
        username = "myingestor"
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'relative_path_in_datablock: true\n' \
            'ingestor_username: "{username}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir,
                username=username, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)

        commands = [('scicat_dataset_ingestor -c %s -r36 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r36 -l debug'
                     % cfgfname).split()]

        def tst_thread():
            """ test thread which adds and removes beamtime metadata file """
            time.sleep(3)
            shutil.copy(lsource, fsubdirname2)
            time.sleep(5)
            os.mkdir(fsubdirname3)
            time.sleep(12)
            os.mkdir(fsubdirnamedet3)
            with open(os.path.join(
                    fsubdirnamedet3, "data3.dat"), "w+") as cf:
                cf.write("12345")
            time.sleep(0.1)
            os.mkdir(fsubdirnamedet4)
            with open(os.path.join(
                    fsubdirnamedet4, "data4.dat"), "w+") as cf:
                cf.write("12345")
            time.sleep(0.1)
            with open(fdslist, "a+") as fds:
                fds.write("myscan_00003 ../lambda3\n")
                fds.write("myscan_00004 ../lambda4\n")

        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                os.mkdir(fsubdirnamedet1)
                os.mkdir(fsubdirnamedet2)
                with open(os.path.join(
                        fsubdirnamedet1, "data1.dat"), "w+") as cf:
                    cf.write("12345")
                with open(os.path.join(
                        fsubdirnamedet2, "data2.dat"), "w+") as cf:
                    cf.write("12345")
                # print(cmd)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                shutil.copy(lsource, fsubdirname2)
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                th = threading.Thread(target=tst_thread)
                th.start()
                vl, er = self.runtest(cmd)
                th.join()
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                # print(er)
                try:
                    self.assertEqual(
                        'INFO : BeamtimeWatcher: Adding watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{detdir1} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{detdir1}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{detdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt5}: '
                        '{detdir2}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt6}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Creating DatasetWatcher '
                        '{dslist}\n'
                        'INFO : DatasetWatcher: Adding watch {cnt7}: '
                        '{dslist} {idslist}\n'
                        'INFO : DatasetWatcher: Waiting datasets: '
                        '[\'{sc1} {det1}\', \'{sc2} {det2}\']\n'
                        'INFO : DatasetWatcher: Ingested datasets: []\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist}'
                        ' {sc1} {det1}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {det1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001236/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001236/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist}'
                        ' {sc2} {det2}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {det2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001236/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001236/{sc2}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{detdir3} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt8}: '
                        '{detdir3}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{detdir4} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt9}: '
                        '{detdir4}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} '
                        '{sc3} {det3}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc3} {subdir2}/{sc3}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc3} {det3} {subdir2}/{sc3}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001236/{sc3}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001236/{sc3}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} '
                        '{sc4} {det4}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc4} {subdir2}/{sc4}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc4} {det4} {subdir2}/{sc4}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001236/{sc4}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001236/{sc4}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{detdir1}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{detdir2}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt6}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Stopping DatasetWatcher '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt7}: '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt8}: '
                        '{detdir3}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt9}: '
                        '{detdir4}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                subdir=fsubdirname, subdir2=fsubdirname2,
                                detdir1=fsubdirnamedet1,
                                detdir2=fsubdirnamedet2,
                                detdir3=fsubdirnamedet3,
                                detdir4=fsubdirnamedet4,
                                dslist=fdslist, idslist=fidslist,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4),
                                cnt6=(cnt + 5), cnt7=(cnt + 6),
                                cnt8=(cnt + 7), cnt9=(cnt + 8),
                                det1="../lambda1/", det2="../lambda2/",
                                det3="../lambda3", det4="../lambda4",
                                sc1='myscan_00001', sc2='myscan_00002',
                                sc3='myscan_00003', sc4='myscan_00004'),
                        "\n".join(dseri))
                except Exception:
                    print(er)
                    raise
                self.assertEqual(
                    'Login: myingestor\n'
                    "RawDatasets: 99001236/myscan_00001\n"
                    "OrigDatablocks: /99001236/myscan_00001\n"
                    "RawDatasets: 99001236/myscan_00002\n"
                    "OrigDatablocks: /99001236/myscan_00002\n"
                    'Login: myingestor\n'
                    "RawDatasets: 99001236/myscan_00003\n"
                    "OrigDatablocks: /99001236/myscan_00003\n"
                    "RawDatasets: 99001236/myscan_00004\n"
                    "OrigDatablocks: /99001236/myscan_00004\n", vl)
                self.assertEqual(len(self.__server.userslogin), 2)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 4)
                for i in range(4):
                    self.myAssertDict(
                        json.loads(self.__server.datasets[i]),
                        {'contactEmail': 'appuser@fake.com',
                         'createdAt': '2022-05-14 11:54:29',
                         'creationLocation': '/DESY/FS-SC/SDD01',
                         'instrumentId': '/fs-sc/sdd01',
                         'description': 'H20 distribution',
                         'endTime': '2022-05-19 09:00:00',
                         'isPublished': False,
                         'techniques': [],
                         'owner': 'Smithson',
                         'ownerEmail': 'peter.smithson@fake.de',
                         'pid': '99001236/myscan_%05i' % (i + 1),
                         'datasetName': 'myscan_%05i' % (i + 1),
                         'accessGroups': [
                             '99001236-dmgt', '99001236-clbt', '99001236-part',
                             'sdd01dmgt', 'sdd01staff'],
                         'principalInvestigator': 'appuser@fake.com',
                         'ownerGroup': '99001236-dmgt',
                         'proposalId': '99001236',
                         'scientificMetadata': {
                             'DOOR_proposalId': '99991173',
                             'beamtimeId': '99001236'},
                         'sourceFolder':
                         '/asap3/fs-sc/gpfs/p00/2022/data/9901236',
                         'type': 'raw',
                         'updatedAt': '2022-05-14 11:54:29'})

                # print(self.__server.origdatablocks)
                self.assertEqual(len(self.__server.origdatablocks), 4)
                for i in range(4):
                    self.myAssertDict(
                        json.loads(self.__server.origdatablocks[i]),
                        {'dataFileList': [
                            {'gid': 'jkotan',
                             'path': 'myscan_00001.scan.json',
                             'perm': '-rw-r--r--',
                             'size': 629,
                             'time': '2022-07-05T19:07:16.683673+0200',
                             'uid': 'jkotan'}],
                         'ownerGroup': '99001236-dmgt',
                         'datasetId':
                         '/99001236/myscan_%05i' % (i + 1),
                         'accessGroups': [
                             '99001236-dmgt', '99001236-clbt', '99001236-part',
                             'sdd01dmgt', 'sdd01staff'],
                         'size': 629}, skip=["dataFileList", "size"])
                    dfl = json.loads(
                        self.__server.origdatablocks[i])["dataFileList"]
                    paths = sorted([df["path"] for df in dfl])
                    self.assertEqual(
                        paths,
                        ['raw/lambda{ct}/data{ct}.dat'.format(ct=(i + 1))])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
                if os.path.isdir(vardir):
                    shutil.rmtree(vardir)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_exist_relpath_det_meta(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirnamedet1 = os.path.abspath(os.path.join(fsubdirname, "lambda1"))
        fsubdirnamedet2 = os.path.abspath(os.path.join(fsubdirname, "lambda2"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
        btmeta = "beamtime-metadata-99001236.json"
        dslist = "scicat-datasets-99001236.lst"
        idslist = "scicat-ingested-datasets-99001236.lst"
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
        fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/tmp/scingestor_log_%s/{beamtimeid}" % uuid.uuid4().hex
        lvardir = vardir.format(beamtimeid="99001236")
        fidslist = "%s%s" % (lvardir, fidslist)
        cred = "12342345"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'relative_path_generator_switch: " -r {{relpath}} "\n' \
            'owner_access_groups_from_proposal: true\n' \
            'metadata_in_var_dir: true\n' \
            'relative_path_in_datablock: true\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir, credfile=credfile)

        prop = {
            "ownerGroup": "mygroup",
            "accessGroups": ["group1", "group2"],
        }
        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingestor -c %s -r10 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r10 -l debug'
                     % cfgfname).split()]
        # commands.pop()
        try:
            for cmd in commands:
                time.sleep(1)
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                os.mkdir(fsubdirnamedet1)
                os.mkdir(fsubdirnamedet2)
                with open(os.path.join(
                        fsubdirnamedet1, "data1.dat"), "w+") as cf:
                    cf.write("12345")
                with open(os.path.join(
                        fsubdirnamedet2, "data2.dat"), "w+") as cf:
                    cf.write("12345")
                os.mkdir(fsubdirname3)
                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                self.__server.pid_proposal["99001236"] = json.dumps(prop)
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]
                # print(vl)
                # print(er)
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                try:
                    pattern = self.sortmarkedlines(
                        'INFO : BeamtimeWatcher: Adding watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{detdir1} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{detdir1}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{detdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt5}: '
                        '{detdir2}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt6}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Creating DatasetWatcher '
                        '{dslist}\n'
                        'INFO : DatasetWatcher: Adding watch {cnt7}: '
                        '{dslist} {idslist}\n'
                        'INFO : DatasetWatcher: Waiting datasets: '
                        '[\'{sc1} {det1}\', \'{sc2} {det2}\']\n'
                        'INFO : DatasetWatcher: Ingested datasets: []\n'
                        'INFO : DatasetIngestor: '
                        'Ingesting: {dslist} {sc1} {det1}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc1} {vardir}{subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {det1} '
                        '{vardir}{subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001236/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001236/{sc1}\n'
                        'INFO : DatasetIngestor: '
                        'Ingesting: {dslist} {sc2} {det2}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc2} {vardir}{subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {det2} '
                        '{vardir}{subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001236/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001236/{sc2}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{detdir1}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{detdir2}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt6}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Stopping DatasetWatcher '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt7}: '
                        '{dslist}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                subdir=fsubdirname, subdir2=fsubdirname2,
                                detdir1=fsubdirnamedet1,
                                vardir=lvardir,
                                detdir2=fsubdirnamedet2,
                                dslist=fdslist, idslist=fidslist,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4),
                                cnt6=(cnt + 5), cnt7=(cnt + 6),
                                det1="../lambda1/", det2="../lambda2/",
                                sc1='myscan_00001', sc2='myscan_00002'),
                        [(5, 39)], {'watch [0-9]:': 'watch:'})
                    self.assertEqual(
                        pattern, self.sortmarkedlines(
                            dseri, [(5, 39)], {'watch [0-9]:': 'watch:'}))
                except Exception:
                    print(er)
                    raise
                self.assertEqual(
                    "Login: ingestor\n"
                    "Login: ingestor\n"
                    "RawDatasets: 99001236/myscan_00001\n"
                    "OrigDatablocks: /99001236/myscan_00001\n"
                    "RawDatasets: 99001236/myscan_00002\n"
                    "OrigDatablocks: /99001236/myscan_00002\n", vl)
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
                     'creationLocation': '/DESY/FS-SC/SDD01',
                     'instrumentId': '/fs-sc/sdd01',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerGroup': 'mygroup',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001236/myscan_00001',
                     'accessGroups': ['group1', 'group2'],
                     'datasetName': 'myscan_00001',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001236',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001236'},
                     'sourceFolder':
                     '/asap3/fs-sc/gpfs/p00/2022/data/9901236',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[1]),
                    {'contactEmail': 'appuser@fake.com',
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/fs-sc/sdd01',
                     'creationLocation': '/DESY/FS-SC/SDD01',
                     'description': 'H20 distribution',
                     'endTime': '2022-05-19 09:00:00',
                     'isPublished': False,
                     'techniques': [],
                     'owner': 'Smithson',
                     'ownerGroup': 'mygroup',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001236/myscan_00002',
                     'accessGroups': ['group1', 'group2'],
                     'datasetName': 'myscan_00002',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001236',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'beamtimeId': '99001236'},
                     'sourceFolder':
                     '/asap3/fs-sc/gpfs/p00/2022/data/9901236',
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
                     'datasetId': '/99001236/myscan_00001',
                     'accessGroups': ['group1', 'group2'],
                     'size': 629}, skip=["dataFileList", "size"])
                dfl = json.loads(
                    self.__server.origdatablocks[0])["dataFileList"]
                paths = sorted([df["path"] for df in dfl])
                self.assertEqual(paths, ['raw/lambda1/data1.dat'])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[1]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'ownerGroup': 'mygroup',
                     'datasetId': '/99001236/myscan_00002',
                     'accessGroups': ['group1', 'group2'],
                     'size': 629}, skip=["dataFileList", "size"])
                dfl = json.loads(
                    self.__server.origdatablocks[1])["dataFileList"]
                paths = sorted([df["path"] for df in dfl])
                self.assertEqual(paths, ['raw/lambda2/data2.dat'])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
                if os.path.isdir(lvardir):
                    shutil.rmtree(lvardir)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_add_relpath_det_meta(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
        fsubdirnamedet1 = os.path.abspath(os.path.join(fsubdirname, "lambda1"))
        fsubdirnamedet2 = os.path.abspath(os.path.join(fsubdirname, "lambda2"))
        fsubdirnamedet3 = os.path.abspath(os.path.join(fsubdirname, "lambda3"))
        fsubdirnamedet4 = os.path.abspath(os.path.join(fsubdirname, "lambda4"))
        os.mkdir(fdirname)
        btmeta = "beamtime-metadata-99001236.json"
        dslist = "scicat-datasets-99001236.lst"
        idslist = "scicat-ingested-datasets-99001236.lst"
        # wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        shutil.copy(source, fdirname)
        # shutil.copy(lsource, fsubdirname2)
        # shutil.copy(wlsource, fsubdirname)
        fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/tmp/scingestor_log_%s" % uuid.uuid4().hex
        fidslist = "%s%s" % (vardir, fidslist)
        cred = "12342345"
        username = "myingestor"
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
            'relative_path_generator_switch: " -r {{relpath}} "\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'relative_path_in_datablock: true\n' \
            'ingestor_username: "{username}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir,
                username=username, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)

        commands = [('scicat_dataset_ingestor -c %s -r36 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r36 -l debug'
                     % cfgfname).split()]

        def tst_thread():
            """ test thread which adds and removes beamtime metadata file """
            time.sleep(3)
            shutil.copy(lsource, fsubdirname2)
            time.sleep(5)
            os.mkdir(fsubdirname3)
            time.sleep(12)
            os.mkdir(fsubdirnamedet3)
            with open(os.path.join(
                    fsubdirnamedet3, "data3.dat"), "w+") as cf:
                cf.write("12345")
            time.sleep(0.1)
            os.mkdir(fsubdirnamedet4)
            with open(os.path.join(
                    fsubdirnamedet4, "data4.dat"), "w+") as cf:
                cf.write("12345")
            time.sleep(0.1)
            with open(fdslist, "a+") as fds:
                fds.write("myscan_00003 ../lambda3\n")
                fds.write("myscan_00004 ../lambda4\n")

        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                os.mkdir(fsubdirnamedet1)
                os.mkdir(fsubdirnamedet2)
                with open(os.path.join(
                        fsubdirnamedet1, "data1.dat"), "w+") as cf:
                    cf.write("12345")
                with open(os.path.join(
                        fsubdirnamedet2, "data2.dat"), "w+") as cf:
                    cf.write("12345")
                # print(cmd)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                self.__server.pid_proposal["99001236"] = json.dumps(prop)
                shutil.copy(lsource, fsubdirname2)
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                th = threading.Thread(target=tst_thread)
                th.start()
                vl, er = self.runtest(cmd)
                th.join()
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                # print(er)
                try:
                    pattern = self.sortmarkedlines(
                        'INFO : BeamtimeWatcher: Adding watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{detdir1} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{detdir1}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{detdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt5}: '
                        '{detdir2}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt6}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Creating DatasetWatcher '
                        '{dslist}\n'
                        'INFO : DatasetWatcher: Adding watch {cnt7}: '
                        '{dslist} {idslist}\n'
                        'INFO : DatasetWatcher: Waiting datasets: '
                        '[\'{sc1} {det1}\', \'{sc2} {det2}\']\n'
                        'INFO : DatasetWatcher: Ingested datasets: []\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist}'
                        ' {sc1} {det1}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc1} {vardir}{subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {det1} '
                        '{vardir}{subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001236/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001236/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist}'
                        ' {sc2} {det2}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc2} {vardir}{subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {det2} '
                        '{vardir}{subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001236/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001236/{sc2}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{detdir3} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt8}: '
                        '{detdir3}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{detdir4} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt9}: '
                        '{detdir4}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} '
                        '{sc3} {det3}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc3} {vardir}{subdir2}/{sc3}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc3} {det3} '
                        '{vardir}{subdir2}/{sc3}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001236/{sc3}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001236/{sc3}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} '
                        '{sc4} {det4}\n'
                        'INFO : DatasetIngestor: Generating metadata: '
                        '{sc4} {vardir}{subdir2}/{sc4}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc4} {det4} '
                        '{vardir}{subdir2}/{sc4}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '/99001236/{sc4}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '/99001236/{sc4}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{detdir1}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{detdir2}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt6}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Stopping DatasetWatcher '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt7}: '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt8}: '
                        '{detdir3}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt9}: '
                        '{detdir4}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                subdir=fsubdirname, subdir2=fsubdirname2,
                                vardir=vardir,
                                detdir1=fsubdirnamedet1,
                                detdir2=fsubdirnamedet2,
                                detdir3=fsubdirnamedet3,
                                detdir4=fsubdirnamedet4,
                                dslist=fdslist, idslist=fidslist,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4),
                                cnt6=(cnt + 5), cnt7=(cnt + 6),
                                cnt8=(cnt + 7), cnt9=(cnt + 8),
                                det1="../lambda1/", det2="../lambda2/",
                                det3="../lambda3", det4="../lambda4",
                                sc1='myscan_00001', sc2='myscan_00002',
                                sc3='myscan_00003', sc4='myscan_00004'),
                        [(5, 55)], {'watch [0-9]:': 'watch:'})
                    self.assertEqual(
                        pattern, self.sortmarkedlines(
                            dseri, [(5, 55)], {'watch [0-9]:': 'watch:'}))
                except Exception:
                    print(er)
                    raise
                self.assertEqual(
                    'Login: myingestor\n'
                    'Login: myingestor\n'
                    "RawDatasets: 99001236/myscan_00001\n"
                    "OrigDatablocks: /99001236/myscan_00001\n"
                    "RawDatasets: 99001236/myscan_00002\n"
                    "OrigDatablocks: /99001236/myscan_00002\n"
                    'Login: myingestor\n'
                    "RawDatasets: 99001236/myscan_00003\n"
                    "OrigDatablocks: /99001236/myscan_00003\n"
                    "RawDatasets: 99001236/myscan_00004\n"
                    "OrigDatablocks: /99001236/myscan_00004\n", vl)
                self.assertEqual(len(self.__server.userslogin), 3)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[2],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 4)
                for i in range(4):
                    self.myAssertDict(
                        json.loads(self.__server.datasets[i]),
                        {'contactEmail': 'appuser@fake.com',
                         'createdAt': '2022-05-14 11:54:29',
                         'instrumentId': '/fs-sc/sdd01',
                         'creationLocation': '/DESY/FS-SC/SDD01',
                         'description': 'H20 distribution',
                         'endTime': '2022-05-19 09:00:00',
                         'isPublished': False,
                         'techniques': [],
                         'owner': 'Smithson',
                         'ownerEmail': 'peter.smithson@fake.de',
                         'pid': '99001236/myscan_%05i' % (i + 1),
                         'datasetName': 'myscan_%05i' % (i + 1),
                         'accessGroups': ['group1', 'group2'],
                         'principalInvestigator': 'appuser@fake.com',
                         'ownerGroup': 'mygroup',
                         'proposalId': '99001236',
                         'scientificMetadata': {
                             'DOOR_proposalId': '99991173',
                             'beamtimeId': '99001236'},
                         'sourceFolder':
                         '/asap3/fs-sc/gpfs/p00/2022/data/9901236',
                         'type': 'raw',
                         'updatedAt': '2022-05-14 11:54:29'})

                # print(self.__server.origdatablocks)
                self.assertEqual(len(self.__server.origdatablocks), 4)
                for i in range(4):
                    self.myAssertDict(
                        json.loads(self.__server.origdatablocks[i]),
                        {'dataFileList': [
                            {'gid': 'jkotan',
                             'path': 'myscan_00001.scan.json',
                             'perm': '-rw-r--r--',
                             'size': 629,
                             'time': '2022-07-05T19:07:16.683673+0200',
                             'uid': 'jkotan'}],
                         'ownerGroup': 'mygroup',
                         'datasetId':
                         '/99001236/myscan_%05i' % (i + 1),
                         'accessGroups': ['group1', 'group2'],
                         'size': 629}, skip=["dataFileList", "size"])
                    dfl = json.loads(
                        self.__server.origdatablocks[i])["dataFileList"]
                    paths = sorted([df["path"] for df in dfl])
                    self.assertEqual(
                        paths,
                        ['raw/lambda{ct}/data{ct}.dat'.format(ct=(i + 1))])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
                if os.path.isdir(vardir):
                    shutil.rmtree(vardir)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)


if __name__ == '__main__':
    unittest.main()
