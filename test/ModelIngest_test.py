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

from scingestor import modelIngest


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
class ModelIngestTest(unittest.TestCase):

    # constructor
    # \param methodName name of the test method
    def __init__(self, methodName):
        unittest.TestCase.__init__(self, methodName)

        self.maxDiff = None

        self.helpshort = """usage: scicat_ingest [-h]""" \
            """[-m MODEL] [-c CONFIG] [-l LOG] [-f LOGFILE] [-t]
                     [-p TOKENFILE]
                     metadata_json_file [metadata_json_file ...]
scicat_ingest: error: unrecognized arguments: """

        self.helpshort2 = """usage: scicat_ingest [-h]""" \
            """[-m MODEL] [-c CONFIG] [-l LOG] [-f LOGFILE] [-t]
                     [-p TOKENFILE]
                     metadata_json_file [metadata_json_file ...]
scicat_ingest: error: the following arguments are required: metadata_json_file
"""
        self.helpshort3 = """Error: SciCat model not define. """ \
            """Use the -m or --model option
"""
        self.helpinfo = """usage: scicat_ingest [-h]""" \
            """[-m MODEL] [-c CONFIG] [-l LOG] [-f LOGFILE] [-t]
                     [-p TOKENFILE]
                     metadata_json_file [metadata_json_file ...]

Ingest script for SciCat Models.

positional arguments:
  metadata_json_file    metadata json file(s)

optional arguments:
  -h, --help            show this help message and exit
  -m MODEL, --model MODEL
                        SciCat model name in plural
  -c CONFIG, --configuration CONFIG
                        configuration file name
  -l LOG, --log LOG     logging level, i.e. debug, info, warning, error,
                        critical
  -f LOGFILE, --log-file LOGFILE
                        log file name
  -t, --timestamps      timestamps in logs
  -p TOKENFILE, --token-file TOKENFILE
                        file with a user token

 examples:
      scicat_ingest -m Samples -c ~/.scingestor.yaml
      scicat_ingest -m Attachments -c ~/.scingestor.yaml -p ~/.mytoken.cfg
"""

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
            modelIngest.main()
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
            modelIngest.main()
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

    def test_help(self):
        # fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))

        helps = ['-h', '--help']
        for hl in helps:
            vl, er, et = self.runtestexcept(
                ['scicat_ingest', hl], SystemExit)
            self.assertEqual(
                "".join(self.helpinfo.split()).replace(
                    "optionalarguments:", "options:"),
                "".join(vl.split()).replace("optionalarguments:", "options:"))
            self.assertEqual('', er)

    def test_wrong_args(self):
        # fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))

        helps = ['--wrong', '-asd']
        for hl in helps:
            vl, er, et = self.runtestexcept(
                ['scicat_ingest', 'meta.json', hl], SystemExit)
            self.assertEqual('', vl.strip())
            self.assertEqual(
                "".join(self.helpshort.split() + [hl]),
                "".join(er.split()))

    def test_wrong_args2(self):
        # fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))

        helps = ['--wrong', '-asd']
        for hl in helps:
            vl, er, et = self.runtestexcept(
                ['scicat_ingest', hl], SystemExit)
            self.assertEqual('', vl.strip())
            self.assertEqual(
                "".join(self.helpshort2.split()),
                "".join(er.split()))

    def test_wrong_args3(self):
        # fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))

        vl, er, et = self.runtestexcept(
            ['scicat_ingest', 'meta.json'], SystemExit)
        self.assertEqual('', vl.strip())
        self.assertEqual(
            "".join(self.helpshort3.split()),
            "".join(er.split()))

    def test_modelfile(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        # fullbtmeta = os.path.join(fdirname, btmeta)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        cred = '{"jwt":"12342345"}'
        uname = "lingestor"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'scicat_url: "{url}"\n' \
            'ingestor_username: "{username}"\n' \
            'max_request_tries_number: 10\n' \
            'request_headers:\n' \
            '  "Content-Type": "application/json"\n' \
            '  "Accept": "application/json"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                url=url, credfile=credfile, username=uname)

        jsns = [
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
             'updatedAt': '2022-05-14 11:54:29'},

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
             'updatedAt': '2022-05-14 11:54:29'}
        ]

        jsnfiles = [
            "%s_%s_00001.dataset.json" % (self.__class__.__name__, fun),
            "%s_%s_00002.dataset.json" % (self.__class__.__name__, fun)
        ]
        for k, jfile in enumerate(jsnfiles):
            with open(jfile, "w+") as jf:
                jf.write(json.dumps(jsns[k]))

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [("scicat_ingest -m Datasets -c %s  "
                     % (cfgfname)).split(),
                    ("scicat_ingest --model Datasets --config %s "
                     % (cfgfname)).split()]
        # commands.pop()
        try:
            for cmd in commands:
                self.__server.reset()
                cmd.extend(jsnfiles)
                # print(cmd)
                vl, er = self.runtest(cmd)
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                # print(vl)
                # print(er)
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                self.assertEqual(
                    "INFO : ModelIngestor: Post the Datasets from "
                    "ModelIngestTest_test_modelfile_00001.dataset.json\n"
                    "INFO : ModelIngestor: Post the Datasets from "
                    "ModelIngestTest_test_modelfile_00002.dataset.json\n",
                    "\n".join(seri))
                self.assertEqual(
                    # "Login: lingestor\n"
                    "Datasets: 99001234/myscan_00001\n"
                    "Datasets: 99001234/myscan_00002\n", vl)
                self.assertEqual(len(self.__server.userslogin), 0)
                # self.assertEqual(
                #     self.__server.userslogin[0],
                #     b'{"username": "lingestor", "password": "12342345"}')
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
                self.assertEqual(len(self.__server.origdatablocks), 0)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.exists(jsnfiles[0]):
                os.remove(jsnfiles[0])
            if os.path.exists(jsnfiles[1]):
                os.remove(jsnfiles[1])
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_modelfile_wrong_username(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        # fullbtmeta = os.path.join(fdirname, btmeta)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        cred = '{"password":"12342345"}'
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'scicat_url: "{url}"\n' \
            'ingestor_username: ""\n' \
            'max_request_tries_number: 10\n' \
            'request_headers:\n' \
            '  "Content-Type": "application/json"\n' \
            '  "Accept": "application/json"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                url=url, credfile=credfile)

        jsns = [
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
             'updatedAt': '2022-05-14 11:54:29'},

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
             'updatedAt': '2022-05-14 11:54:29'}
        ]

        jsnfiles = [
            "%s_%s_00001.dataset.json" % (self.__class__.__name__, fun),
            "%s_%s_00002.dataset.json" % (self.__class__.__name__, fun)
        ]
        for k, jfile in enumerate(jsnfiles):
            with open(jfile, "w+") as jf:
                jf.write(json.dumps(jsns[k]))

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [("scicat_ingest -m Datasets -c %s  "
                     % (cfgfname)).split(),
                    ("scicat_ingest --model Datasets --config %s "
                     % (cfgfname)).split()]
        # commands.pop()
        try:
            for cmd in commands:
                self.__server.reset()
                cmd.extend(jsnfiles)
                # print(cmd)
                vl, er = self.runtest(cmd)
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                # print(vl)
                # print(er)
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                self.assertEqual(
                    'ERROR : ModelIngestor: {"Error": "Empty username"}\n'
                    "INFO : ModelIngestor: Post the Datasets from "
                    "ModelIngestTest_test_modelfile_wrong_username_00001."
                    "dataset.json\n"
                    'ERROR : ModelIngestor: {"Error": "Empty access_token"}\n'
                    "INFO : ModelIngestor: Post the Datasets from "
                    "ModelIngestTest_test_modelfile_wrong_username_00002."
                    "dataset.json\n"
                    'ERROR : ModelIngestor: {"Error": "Empty access_token"}\n',
                    "\n".join(seri))
                self.assertEqual(
                    "Login: \n", vl)
                self.assertEqual(len(self.__server.userslogin), 1)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 0)
                self.assertEqual(len(self.__server.origdatablocks), 0)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.exists(jsnfiles[0]):
                os.remove(jsnfiles[0])
            if os.path.exists(jsnfiles[1]):
                os.remove(jsnfiles[1])
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_modelfile_wrong_format(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        # fullbtmeta = os.path.join(fdirname, btmeta)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        cred = "12342345"
        uname = "lingestor"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'scicat_url: "{url}"\n' \
            'ingestor_username: "{username}"\n' \
            'max_request_tries_number: 10\n' \
            'request_headers:\n' \
            '  "Content-Type": "application/json"\n' \
            '  "Accept": "application/json"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                url=url, credfile=credfile, username=uname)

        jsnfiles = [
            "%s_%s_00001.dataset.json" % (self.__class__.__name__, fun),
            "%s_%s_00002.dataset.json" % (self.__class__.__name__, fun)
        ]
        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [("scicat_ingest -m Datasets -c %s  "
                     % (cfgfname)).split(),
                    ("scicat_ingest --model Datasets --config %s "
                     % (cfgfname)).split()]
        # commands.pop()
        try:
            for cmd in commands:
                self.__server.reset()
                cmd.extend(jsnfiles)
                # print(cmd)
                vl, er = self.runtest(cmd)
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                # print(vl)
                # print(er)
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                self.assertEqual(
                    "ERROR : ModelIngestor: [Errno 2] "
                    "No such file or directory: "
                    "'ModelIngestTest_test_modelfile_wrong_format_"
                    "00001.dataset.json'\n"
                    "ERROR : ModelIngestor: [Errno 2] "
                    "No such file or directory: "
                    "'ModelIngestTest_test_modelfile_wrong_format_"
                    "00002.dataset.json'\n",
                    "\n".join(seri))
                self.assertEqual("Login: lingestor\n", vl)
                self.assertEqual(len(self.__server.userslogin), 1)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "lingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 0)
                self.assertEqual(len(self.__server.origdatablocks), 0)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.exists(jsnfiles[0]):
                os.remove(jsnfiles[0])
            if os.path.exists(jsnfiles[1]):
                os.remove(jsnfiles[1])
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_modelfile_wrong_type(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        # fullbtmeta = os.path.join(fdirname, btmeta)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        cred = "12342345"
        uname = "lingestor"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'scicat_url: "{url}"\n' \
            'ingestor_username: "{username}"\n' \
            'max_request_tries_number: ha\n' \
            'request_headers: true\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                url=url, credfile=credfile, username=uname)

        jsns = [
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
             'updatedAt': '2022-05-14 11:54:29'},

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
             'updatedAt': '2022-05-14 11:54:29'}
        ]

        jsnfiles = [
            "%s_%s_00001.dataset.json" % (self.__class__.__name__, fun),
            "%s_%s_00002.dataset.json" % (self.__class__.__name__, fun)
        ]
        for k, jfile in enumerate(jsnfiles):
            with open(jfile, "w+") as jf:
                jf.write(json.dumps(jsns[k]))

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [("scicat_ingest -m Datasets -c %s  "
                     % (cfgfname)).split(),
                    ("scicat_ingest --model Datasets --config %s "
                     % (cfgfname)).split()]
        # commands.pop()
        try:
            for cmd in commands:
                self.__server.reset()
                cmd.extend(jsnfiles)
                # print(cmd)
                vl, er = self.runtest(cmd)
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                # print(vl)
                # print(er)
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                self.assertEqual(
                    'WARNING : invalid literal for int() with base 10: '
                    '\'ha\'\n'
                    'WARNING : \'bool\' object is not iterable\n'
                    "INFO : ModelIngestor: Post the Datasets from "
                    "ModelIngestTest_test_modelfile_wrong_type_00001."
                    "dataset.json\n"
                    "INFO : ModelIngestor: Post the Datasets from "
                    "ModelIngestTest_test_modelfile_wrong_type_00002."
                    "dataset.json\n",
                    "\n".join(seri))
                self.assertEqual(
                    "Login: lingestor\n"
                    "Datasets: 99001234/myscan_00001\n"
                    "Datasets: 99001234/myscan_00002\n", vl)
                self.assertEqual(len(self.__server.userslogin), 1)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "lingestor", "password": "12342345"}')
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
                self.assertEqual(len(self.__server.origdatablocks), 0)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.exists(jsnfiles[0]):
                os.remove(jsnfiles[0])
            if os.path.exists(jsnfiles[1]):
                os.remove(jsnfiles[1])
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_modelfile_token(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        # fullbtmeta = os.path.join(fdirname, btmeta)
        tokenfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        token = "12342345"
        os.mkdir(fdirname)
        with open(tokenfile, "w") as cf:
            cf.write(token)

        cfg = 'scicat_url: "{url}"\n'.format(url=url)

        jsns = [
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
             'updatedAt': '2022-05-14 11:54:29'},

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
             'updatedAt': '2022-05-14 11:54:29'}
        ]

        jsnfiles = [
            "%s_%s_00001.dataset.json" % (self.__class__.__name__, fun),
            "%s_%s_00002.dataset.json" % (self.__class__.__name__, fun)
        ]
        for k, jfile in enumerate(jsnfiles):
            with open(jfile, "w+") as jf:
                jf.write(json.dumps(jsns[k]))

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [("scicat_ingest -m Datasets -c %s  -p %s "
                     % (cfgfname, tokenfile)).split(),
                    ("scicat_ingest --model Datasets "
                     "--config %s --token-file %s "
                     % (cfgfname, tokenfile)).split()]
        # commands.pop()
        try:
            for cmd in commands:
                self.__server.reset()
                cmd.extend(jsnfiles)
                # print(cmd)
                vl, er = self.runtest(cmd)
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                # print(vl)
                # print(er)
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                self.assertEqual(
                    "INFO : ModelIngestor: Post the Datasets from "
                    "ModelIngestTest_test_modelfile_token_00001"
                    ".dataset.json\n"
                    "INFO : ModelIngestor: Post the Datasets from "
                    "ModelIngestTest_test_modelfile_token_00002"
                    ".dataset.json\n",
                    "\n".join(seri))
                self.assertEqual(
                    "Datasets: 99001234/myscan_00001\n"
                    "Datasets: 99001234/myscan_00002\n", vl)
                self.assertEqual(len(self.__server.userslogin), 0)
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
                self.assertEqual(len(self.__server.origdatablocks), 0)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.exists(jsnfiles[0]):
                os.remove(jsnfiles[0])
            if os.path.exists(jsnfiles[1]):
                os.remove(jsnfiles[1])
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)


if __name__ == '__main__':
    unittest.main()
