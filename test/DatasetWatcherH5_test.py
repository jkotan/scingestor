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

from scingestor import beamtimeWatcher
from scingestor import safeINotifier

from nxstools import filewriter

try:
    from .SciCatTestServer import SciCatTestServer, SciCatMockHandler
except Exception:
    from SciCatTestServer import SciCatTestServer, SciCatMockHandler


try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO

WRITERS = {}
try:
    from nxstools import h5pywriter
    WRITERS["h5py"] = h5pywriter
except Exception:
    pass

try:
    from nxstools import h5cppwriter
    WRITERS["h5cpp"] = h5cppwriter
except Exception:
    pass


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
class DatasetWatcherH5Test(unittest.TestCase):

    # constructor
    # \param methodName name of the test method
    def __init__(self, methodName):
        unittest.TestCase.__init__(self, methodName)

        self.maxDiff = None
        self.notifier = safeINotifier.SafeINotifier()

        if "h5cpp" in WRITERS.keys():
            self.writer = "h5cpp"
        else:
            self.writer = "h5py"

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

    def test_datasetfile_exist_h5(self):
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
        logdir = "/"
        cred = "12342345"
        chmod = "0o662"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        wrmodule = WRITERS[self.writer]
        filewriter.writer = wrmodule

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'chmod_json_files: "{chmod}"\n' \
            'ingestor_log_dir: "{logdir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, logdir=logdir,
                credfile=credfile, chmod=chmod)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingestor -c %s -r10 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r10 -l debug'
                     % cfgfname).split()]
        # commands.pop()

        args = [
            [
                "myscan_00001.nxs",
                "Test experiment",
                "BL1234554",
                "PETRA III",
                "P3",
                "2014-02-12T15:19:21+00:00",
                "2014-02-15T15:17:21+00:00",
                "water",
                "H20",
                'technique: "saxs"',
            ],
            [
                "myscan_00002.nxs",
                "My experiment",
                "BT123_ADSAD",
                "Petra III",
                "PIII",
                "2019-02-14T15:19:21+00:00",
                "2019-02-15T15:27:21+00:00",
                "test sample",
                "LaB6",
                'techniques_pids:\n'
                '  - "PaNET01191"\n'
                '  - "PaNET01188"\n'
                '  - "PaNET01098"\n'
            ],
        ]
        ltechs = [
            [
                {
                    'name': 'small angle x-ray scattering',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01188'
                }
            ],
            [
                {
                    'name': 'wide angle x-ray scattering',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01191'
                },
                {
                    'name': 'small angle x-ray scattering',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01188'
                },
                {
                    'name': 'grazing incidence diffraction',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01098'
                },
            ],

        ]

        try:
            for cmd in commands:
                time.sleep(1)
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                os.mkdir(fsubdirname3)

                for k, arg in enumerate(args):
                    nxsfilename = os.path.join(fsubdirname2, arg[0])
                    dsfilename = nxsfilename[:-4] + ".scan.json"
                    dbfilename = nxsfilename[:-4] + ".origdatablock.json"
                    title = arg[1]
                    beamtime = arg[2]
                    insname = arg[3]
                    inssname = arg[4]
                    stime = arg[5]
                    etime = arg[6]
                    smpl = arg[7]
                    formula = arg[8]

                    nxsfile = filewriter.create_file(
                        nxsfilename, overwrite=True)
                    rt = nxsfile.root()
                    entry = rt.create_group("entry12345", "NXentry")
                    ins = entry.create_group("instrument", "NXinstrument")
                    det = ins.create_group("detector", "NXdetector")
                    entry.create_field(
                        "experiment_description", "string").write(arg[9])
                    entry.create_group("data", "NXdata")
                    sample = entry.create_group("sample", "NXsample")
                    det.create_field("intimage", "uint32", [0, 30], [1, 30])

                    entry.create_field("title", "string").write(title)
                    entry.create_field(
                        "experiment_identifier", "string").write(beamtime)
                    entry.create_field("start_time", "string").write(stime)
                    entry.create_field("end_time", "string").write(etime)
                    sname = ins.create_field("name", "string")
                    sname.write(insname)
                    sattr = sname.attributes.create("short_name", "string")
                    sattr.write(inssname)
                    sname = sample.create_field("name", "string")
                    sname.write(smpl)
                    sfml = sample.create_field("chemical_formula", "string")
                    sfml.write(formula)
                    nxsfile.close()

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

                status = os.stat(dsfilename)
                self.assertEqual(chmod, str(oct(status.st_mode & 0o777)))
                status = os.stat(dbfilename)
                self.assertEqual(chmod, str(oct(status.st_mode & 0o777)))

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
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '10.3204/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '10.3204/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '10.3204/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '10.3204/99001234/{sc2}\n'
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
                     'creationTime': args[0][6],
                     'createdAt': '2022-05-14 11:54:29',
                     'creationLocation': '/DESY/PETRA III/p00',
                     'description': args[0][1],
                     'endTime': args[0][6],
                     'isPublished': False,
                     'techniques': ltechs[0],
                     'owner': 'Ouruser',
                     'ownerGroup': '99001234-part',
                     'ownerEmail': 'appuser@fake.com',
                     'pid': '99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
                     'datasetName': 'myscan_00001',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata':
                     {'NX_class': 'NXentry',
                      'name': 'entry12345',
                      'experiment_description': {
                        'value': args[0][9]
                      },
                      'data': {'NX_class': 'NXdata'},
                      'end_time': {'value': '%s' % args[0][6]},
                      'experiment_identifier': {'value': '%s' % args[0][2]},
                      'instrument': {
                          'NX_class': 'NXinstrument',
                          'detector': {
                              'NX_class': 'NXdetector',
                              'intimage': {
                                  'shape': [0, 30]}},
                          'name': {
                            'short_name': '%s' % args[0][4],
                            'value': '%s' % args[0][3]}},
                      'sample': {
                        'NX_class': 'NXsample',
                          'chemical_formula': {'value': '%s' % args[0][8]},
                          'name': {'value': '%s' % args[0][7]}},
                      'start_time': {
                          'value': '%s' % args[0][5]},
                      'title': {'value': '%s' % args[0][1]},
                      'DOOR_proposalId': '99991173',
                      'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[1]),
                    {'contactEmail': 'BSName',
                     'creationTime': args[1][6],
                     'createdAt': '2022-05-14 11:54:29',
                     'creationLocation': '/DESY/PETRA III/p00',
                     'description': args[1][1],
                     'endTime': args[1][6],
                     'isPublished': False,
                     'techniques': ltechs[1],
                     'owner': 'Ouruser',
                     'ownerGroup': '99001234-part',
                     'ownerEmail': 'appuser@fake.com',
                     'pid': '99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
                     'datasetName': 'myscan_00002',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata':
                     {'NX_class': 'NXentry',
                      'name': 'entry12345',
                      'experiment_description': {
                        'value':  args[1][9]
                      },
                      'data': {'NX_class': 'NXdata'},
                      'end_time': {'value': '%s' % args[1][6]},
                      'experiment_identifier': {'value': '%s' % args[1][2]},
                      'instrument': {
                          'NX_class': 'NXinstrument',
                          'detector': {
                              'NX_class': 'NXdetector',
                              'intimage': {
                                  'shape': [0, 30]}},
                          'name': {
                              'short_name': '%s' % args[1][4],
                              'value': '%s' % args[1][3]}},
                      'sample': {
                        'NX_class': 'NXsample',
                          'chemical_formula': {'value': '%s' % args[1][8]},
                          'name': {'value': '%s' % args[1][7]}},
                      'start_time': {
                          'value': '%s' % args[1][5]},
                      'title': {'value': '%s' % args[1][1]},
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
                     'datasetId': '10.3204/99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
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
                     'ownerGroup': '99001234-part',
                     'datasetId': '10.3204/99001234/myscan_00002',
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

    def test_datasetfile_add_h5(self):
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
        logdir = "/"
        cred = "12342345"
        username = "myingestor"
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'oned_in_metadata: true\n' \
            'ingestor_log_dir: "{logdir}"\n' \
            'ingestor_username: "{username}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, logdir=logdir,
                username=username, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)

        wrmodule = WRITERS[self.writer]
        filewriter.writer = wrmodule

        commands = [('scicat_dataset_ingestor -c %s -r36 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r36 -l debug'
                     % cfgfname).split()]

        arg = [
            "myscan_%05i.nxs",
            "Test experiment",
            "BL1234554",
            "PETRA III",
            "P3",
            "2014-02-12T15:19:21+00:00",
            "2014-02-15T15:17:21+00:00",
            "water",
            "H20",
            'technique: "saxs"',
        ]
        ltech = [
            {
                'name': 'small angle x-ray scattering',
                'pid':
                'http://purl.org/pan-science/PaNET/PaNET01188'
            }
        ]

        def test_thread():
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

                for k in range(4):
                    nxsfilename = os.path.join(
                        fsubdirname2, arg[0] % (k + 1))
                    title = arg[1]
                    beamtime = arg[2]
                    insname = arg[3]
                    inssname = arg[4]
                    stime = arg[5]
                    etime = arg[6]
                    smpl = arg[7]
                    formula = arg[8]
                    spectrum = [243, 34, 34, 23, 334, 34, 34, 33, 32, 11]

                    nxsfile = filewriter.create_file(
                        nxsfilename, overwrite=True)
                    rt = nxsfile.root()
                    entry = rt.create_group("entry12345", "NXentry")
                    ins = entry.create_group("instrument", "NXinstrument")
                    det = ins.create_group("detector", "NXdetector")
                    entry.create_field(
                        "experiment_description", "string").write(arg[9])
                    entry.create_group("data", "NXdata")
                    sample = entry.create_group("sample", "NXsample")
                    det.create_field("intimage", "uint32", [0, 30], [1, 30])
                    sp = det.create_field("spectrum", "uint32", [10], [10])
                    sp.write(spectrum)

                    entry.create_field("title", "string").write(title)
                    entry.create_field(
                        "experiment_identifier", "string").write(beamtime)
                    entry.create_field("start_time", "string").write(stime)
                    entry.create_field("end_time", "string").write(etime)
                    sname = ins.create_field("name", "string")
                    sname.write(insname)
                    sattr = sname.attributes.create("short_name", "string")
                    sattr.write(inssname)
                    sname = sample.create_field("name", "string")
                    sname.write(smpl)
                    sfml = sample.create_field("chemical_formula", "string")
                    sfml.write(formula)
                    nxsfile.close()

                # print(cmd)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                shutil.copy(lsource, fsubdirname2)
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                th = threading.Thread(target=test_thread)
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
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '10.3204/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '10.3204/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '10.3204/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '10.3204/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc3}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc3} {subdir2}/{sc3}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc3} {subdir2}/{sc3}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '10.3204/99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '10.3204/99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc4}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc4} {subdir2}/{sc4}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc4} {subdir2}/{sc4}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '10.3204/99001234/{sc4}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '10.3204/99001234/{sc4}\n'
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
                    "OrigDatablocks: 10.3204/99001234/myscan_00001\n"
                    "RawDatasets: 99001234/myscan_00002\n"
                    "OrigDatablocks: 10.3204/99001234/myscan_00002\n"
                    'Login: myingestor\n'
                    "RawDatasets: 99001234/myscan_00003\n"
                    "OrigDatablocks: 10.3204/99001234/myscan_00003\n"
                    "RawDatasets: 99001234/myscan_00004\n"
                    "OrigDatablocks: 10.3204/99001234/myscan_00004\n", vl)
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
                        {'contactEmail': 'BSName',
                         'creationTime': arg[6],
                         'createdAt': '2022-05-14 11:54:29',
                         'creationLocation': '/DESY/PETRA III/p00',
                         'description': arg[1],
                         'endTime': arg[6],
                         'isPublished': False,
                         'techniques': ltech,
                         'owner': 'Ouruser',
                         'ownerEmail': 'appuser@fake.com',
                         'pid': '99001234/myscan_%05i' % (i + 1),
                         'datasetName': 'myscan_%05i' % (i + 1),
                         'accessGroups': [
                             '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
                         'principalInvestigator': 'appuser@fake.com',
                         'ownerGroup': '99001234-part',
                         'proposalId': '99001234',
                         'scientificMetadata':
                         {'NX_class': 'NXentry',
                          'name': 'entry12345',
                          'experiment_description': {
                              'value': arg[9]
                          },
                          'data': {'NX_class': 'NXdata'},
                          'end_time': {'value': '%s' % arg[6]},
                          'experiment_identifier': {'value': '%s' % arg[2]},
                          'instrument': {
                              'NX_class': 'NXinstrument',
                              'detector': {
                                  'NX_class': 'NXdetector',
                                  'intimage': {
                                      'shape': [0, 30]
                                  },
                                  'spectrum': {
                                      'value': spectrum,
                                      'shape': [10]
                                  }
                              },

                              'name': {
                                  'short_name': '%s' % arg[4],
                                  'value': '%s' % arg[3]}},
                          'sample': {
                              'NX_class': 'NXsample',
                              'chemical_formula': {'value': '%s' % arg[8]},
                              'name': {'value': '%s' % arg[7]}},
                          'start_time': {
                              'value': '%s' % arg[5]},
                          'title': {'value': '%s' % arg[1]},
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
                         'ownerGroup': '99001234-part',
                         'datasetId':
                         '10.3204/99001234/myscan_%05i' % (i + 1),
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

    def test_datasetfile_exist_h5_script(self):
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
        logdir = "/"
        cred = "12342345"
        chmod = "0o662"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        wrmodule = WRITERS[self.writer]
        filewriter.writer = wrmodule

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'ingestor_log_dir: "{logdir}"\n' \
            'nxs_dataset_metadata_generator: "nxsfileinfo metadata ' \
            ' -o {{scanpath}}/{{scanname}}{{scpostfix}} ' \
            ' -x 0o662 ' \
            ' -r {{relpath}} ' \
            ' -b {{beamtimefile}} -p {{beamtimeid}}/{{scanname}} ' \
            '{{scanpath}}/{{scanname}}.nxs"\n' \
            'datablock_metadata_generator: "nxsfileinfo origdatablock ' \
            ' -s *.pyc,*{{dbpostfix}},*{{scpostfix}},*~ ' \
            ' -x 0o662 ' \
            ' -p {{doiprefix}}/{{beamtimeid}}/{{scanname}} ' \
            ' -c {{beamtimeid}}-clbt,{{beamtimeid}}-dmgt,{{beamline}}dmgt ' \
            ' -o {{scanpath}}/{{scanname}}{{dbpostfix}} "\n' \
            'datablock_metadata_stream_generator: ' \
            'nxsfileinfo origdatablock ' \
            ' -s *.pyc,*{{dbpostfix}},*{{scpostfix}},*~ ' \
            ' -x 0o662 ' \
            ' -c {{beamtimeid}}-clbt,{{beamtimeid}}-dmgt,{{beamline}}dmgt' \
            ' -p {{doiprefix}}/{{beamtimeid}}/{{scanname}} "\n' \
            'datablock_metadata_generator_scanpath_postfix: '\
            '" {{scanpath}}/{{scanname}} "\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, logdir=logdir,
                credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingestor -c %s -r10 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r10 -l debug'
                     % cfgfname).split()]
        # commands.pop()

        args = [
            [
                "myscan_00001.nxs",
                "Test experiment",
                "BL1234554",
                "PETRA III",
                "P3",
                "2014-02-12T15:19:21+00:00",
                "2014-02-15T15:17:21+00:00",
                "water",
                "H20",
                'technique: "saxs"',
            ],
            [
                "myscan_00002.nxs",
                "My experiment",
                "BT123_ADSAD",
                "Petra III",
                "PIII",
                "2019-02-14T15:19:21+00:00",
                "2019-02-15T15:27:21+00:00",
                "test sample",
                "LaB6",
                'techniques_pids:\n'
                '  - "PaNET01191"\n'
                '  - "PaNET01188"\n'
                '  - "PaNET01098"\n'
            ],
        ]
        ltechs = [
            [
                {
                    'name': 'small angle x-ray scattering',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01188'
                }
            ],
            [
                {
                    'name': 'wide angle x-ray scattering',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01191'
                },
                {
                    'name': 'small angle x-ray scattering',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01188'
                },
                {
                    'name': 'grazing incidence diffraction',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01098'
                },
            ],

        ]

        try:
            for cmd in commands:
                time.sleep(1)
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                os.mkdir(fsubdirname3)

                for k, arg in enumerate(args):
                    nxsfilename = os.path.join(fsubdirname2, arg[0])
                    dsfilename = nxsfilename[:-4] + ".scan.json"
                    dbfilename = nxsfilename[:-4] + ".origdatablock.json"
                    title = arg[1]
                    beamtime = arg[2]
                    insname = arg[3]
                    inssname = arg[4]
                    stime = arg[5]
                    etime = arg[6]
                    smpl = arg[7]
                    formula = arg[8]

                    nxsfile = filewriter.create_file(
                        nxsfilename, overwrite=True)
                    rt = nxsfile.root()
                    entry = rt.create_group("entry12345", "NXentry")
                    ins = entry.create_group("instrument", "NXinstrument")
                    det = ins.create_group("detector", "NXdetector")
                    entry.create_field(
                        "experiment_description", "string").write(arg[9])
                    entry.create_group("data", "NXdata")
                    sample = entry.create_group("sample", "NXsample")
                    det.create_field("intimage", "uint32", [0, 30], [1, 30])

                    entry.create_field("title", "string").write(title)
                    entry.create_field(
                        "experiment_identifier", "string").write(beamtime)
                    entry.create_field("start_time", "string").write(stime)
                    entry.create_field("end_time", "string").write(etime)
                    sname = ins.create_field("name", "string")
                    sname.write(insname)
                    sattr = sname.attributes.create("short_name", "string")
                    sattr.write(inssname)
                    sname = sample.create_field("name", "string")
                    sname.write(smpl)
                    sfml = sample.create_field("chemical_formula", "string")
                    sfml.write(formula)
                    nxsfile.close()

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

                status = os.stat(dsfilename)
                self.assertEqual(chmod, str(oct(status.st_mode & 0o777)))
                status = os.stat(dbfilename)
                self.assertEqual(chmod, str(oct(status.st_mode & 0o777)))

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
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '10.3204/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '10.3204/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '10.3204/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '10.3204/99001234/{sc2}\n'
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
                     'creationTime': args[0][6],
                     'createdAt': '2022-05-14 11:54:29',
                     'creationLocation': '/DESY/PETRA III/p00',
                     'description': args[0][1],
                     'endTime': args[0][6],
                     'isPublished': False,
                     'techniques': ltechs[0],
                     'owner': 'Ouruser',
                     'ownerGroup': '99001234-part',
                     'ownerEmail': 'appuser@fake.com',
                     'pid': '99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
                     'datasetName': 'myscan_00001',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata':
                     {'NX_class': 'NXentry',
                      'name': 'entry12345',
                      'experiment_description': {
                        'value': args[0][9]
                      },
                      'data': {'NX_class': 'NXdata'},
                      'end_time': {'value': '%s' % args[0][6]},
                      'experiment_identifier': {'value': '%s' % args[0][2]},
                      'instrument': {
                          'NX_class': 'NXinstrument',
                          'detector': {
                              'NX_class': 'NXdetector',
                              'intimage': {
                                  'shape': [0, 30]}},
                          'name': {
                            'short_name': '%s' % args[0][4],
                            'value': '%s' % args[0][3]}},
                      'sample': {
                        'NX_class': 'NXsample',
                          'chemical_formula': {'value': '%s' % args[0][8]},
                          'name': {'value': '%s' % args[0][7]}},
                      'start_time': {
                          'value': '%s' % args[0][5]},
                      'title': {'value': '%s' % args[0][1]},
                      'DOOR_proposalId': '99991173',
                      'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[1]),
                    {'contactEmail': 'BSName',
                     'creationTime': args[1][6],
                     'createdAt': '2022-05-14 11:54:29',
                     'creationLocation': '/DESY/PETRA III/p00',
                     'description': args[1][1],
                     'endTime': args[1][6],
                     'isPublished': False,
                     'techniques': ltechs[1],
                     'owner': 'Ouruser',
                     'ownerGroup': '99001234-part',
                     'ownerEmail': 'appuser@fake.com',
                     'pid': '99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
                     'datasetName': 'myscan_00002',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata':
                     {'NX_class': 'NXentry',
                      'name': 'entry12345',
                      'experiment_description': {
                        'value':  args[1][9]
                      },
                      'data': {'NX_class': 'NXdata'},
                      'end_time': {'value': '%s' % args[1][6]},
                      'experiment_identifier': {'value': '%s' % args[1][2]},
                      'instrument': {
                          'NX_class': 'NXinstrument',
                          'detector': {
                              'NX_class': 'NXdetector',
                              'intimage': {
                                  'shape': [0, 30]}},
                          'name': {
                              'short_name': '%s' % args[1][4],
                              'value': '%s' % args[1][3]}},
                      'sample': {
                        'NX_class': 'NXsample',
                          'chemical_formula': {'value': '%s' % args[1][8]},
                          'name': {'value': '%s' % args[1][7]}},
                      'start_time': {
                          'value': '%s' % args[1][5]},
                      'title': {'value': '%s' % args[1][1]},
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
                     'datasetId': '10.3204/99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
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
                     'ownerGroup': '99001234-part',
                     'datasetId': '10.3204/99001234/myscan_00002',
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

    def test_datasetfile_add_h5_script(self):
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
        logdir = "/"
        cred = "12342345"
        username = "myingestor"
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'oned_in_metadata: true\n' \
            'nxs_dataset_metadata_generator: "nxsfileinfo metadata ' \
            ' -o {{scanpath}}/{{scanname}}{{scpostfix}} ' \
            ' -x 0o662 ' \
            ' --oned ' \
            ' -r {{relpath}} ' \
            ' -b {{beamtimefile}} -p {{beamtimeid}}/{{scanname}} ' \
            '{{scanpath}}/{{scanname}}.nxs"\n' \
            'datablock_metadata_generator: "nxsfileinfo origdatablock ' \
            ' -s *.pyc,*{{dbpostfix}},*{{scpostfix}},*~ ' \
            ' -p {{doiprefix}}/{{beamtimeid}}/{{scanname}} ' \
            ' -x 0o662 ' \
            ' -c {{beamtimeid}}-clbt,{{beamtimeid}}-dmgt,{{beamline}}dmgt ' \
            ' -o {{scanpath}}/{{scanname}}{{dbpostfix}} "\n' \
            'datablock_metadata_stream_generator: ' \
            'nxsfileinfo origdatablock ' \
            ' -s *.pyc,*{{dbpostfix}},*{{scpostfix}},*~ ' \
            ' -c {{beamtimeid}}-clbt,{{beamtimeid}}-dmgt,{{beamline}}dmgt' \
            ' -x 0o662 ' \
            ' -p {{doiprefix}}/{{beamtimeid}}/{{scanname}} "\n' \
            'datablock_metadata_generator_scanpath_postfix: '\
            ' " {{scanpath}}/{{scanname}}"\n' \
            'ingestor_log_dir: "{logdir}"\n' \
            'ingestor_username: "{username}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, logdir=logdir,
                username=username, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)

        wrmodule = WRITERS[self.writer]
        filewriter.writer = wrmodule

        commands = [('scicat_dataset_ingestor -c %s -r36 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r36 -l debug'
                     % cfgfname).split()]

        arg = [
            "myscan_%05i.nxs",
            "Test experiment",
            "BL1234554",
            "PETRA III",
            "P3",
            "2014-02-12T15:19:21+00:00",
            "2014-02-15T15:17:21+00:00",
            "water",
            "H20",
            'technique: "saxs"',
        ]
        ltech = [
            {
                'name': 'small angle x-ray scattering',
                'pid':
                'http://purl.org/pan-science/PaNET/PaNET01188'
            }
        ]

        def test_thread():
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

                for k in range(4):
                    nxsfilename = os.path.join(
                        fsubdirname2, arg[0] % (k + 1))
                    title = arg[1]
                    beamtime = arg[2]
                    insname = arg[3]
                    inssname = arg[4]
                    stime = arg[5]
                    etime = arg[6]
                    smpl = arg[7]
                    formula = arg[8]
                    spectrum = [243, 34, 34, 23, 334, 34, 34, 33, 32, 11]

                    nxsfile = filewriter.create_file(
                        nxsfilename, overwrite=True)
                    rt = nxsfile.root()
                    entry = rt.create_group("entry12345", "NXentry")
                    ins = entry.create_group("instrument", "NXinstrument")
                    det = ins.create_group("detector", "NXdetector")
                    entry.create_field(
                        "experiment_description", "string").write(arg[9])
                    entry.create_group("data", "NXdata")
                    sample = entry.create_group("sample", "NXsample")
                    det.create_field("intimage", "uint32", [0, 30], [1, 30])
                    sp = det.create_field("spectrum", "uint32", [10], [10])
                    sp.write(spectrum)

                    entry.create_field("title", "string").write(title)
                    entry.create_field(
                        "experiment_identifier", "string").write(beamtime)
                    entry.create_field("start_time", "string").write(stime)
                    entry.create_field("end_time", "string").write(etime)
                    sname = ins.create_field("name", "string")
                    sname.write(insname)
                    sattr = sname.attributes.create("short_name", "string")
                    sattr.write(inssname)
                    sname = sample.create_field("name", "string")
                    sname.write(smpl)
                    sfml = sample.create_field("chemical_formula", "string")
                    sfml.write(formula)
                    nxsfile.close()

                # print(cmd)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                shutil.copy(lsource, fsubdirname2)
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                th = threading.Thread(target=test_thread)
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
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '10.3204/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '10.3204/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '10.3204/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '10.3204/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc3}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc3} {subdir2}/{sc3}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc3} {subdir2}/{sc3}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '10.3204/99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '10.3204/99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc4}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc4} {subdir2}/{sc4}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc4} {subdir2}/{sc4}.origdatablock.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '10.3204/99001234/{sc4}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '10.3204/99001234/{sc4}\n'
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
                    "OrigDatablocks: 10.3204/99001234/myscan_00001\n"
                    "RawDatasets: 99001234/myscan_00002\n"
                    "OrigDatablocks: 10.3204/99001234/myscan_00002\n"
                    'Login: myingestor\n'
                    "RawDatasets: 99001234/myscan_00003\n"
                    "OrigDatablocks: 10.3204/99001234/myscan_00003\n"
                    "RawDatasets: 99001234/myscan_00004\n"
                    "OrigDatablocks: 10.3204/99001234/myscan_00004\n", vl)
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
                        {'contactEmail': 'BSName',
                         'creationTime': arg[6],
                         'createdAt': '2022-05-14 11:54:29',
                         'creationLocation': '/DESY/PETRA III/p00',
                         'description': arg[1],
                         'endTime': arg[6],
                         'isPublished': False,
                         'techniques': ltech,
                         'owner': 'Ouruser',
                         'ownerEmail': 'appuser@fake.com',
                         'pid': '99001234/myscan_%05i' % (i + 1),
                         'datasetName': 'myscan_%05i' % (i + 1),
                         'accessGroups': [
                             '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
                         'principalInvestigator': 'appuser@fake.com',
                         'ownerGroup': '99001234-part',
                         'proposalId': '99001234',
                         'scientificMetadata':
                         {'NX_class': 'NXentry',
                          'name': 'entry12345',
                          'experiment_description': {
                              'value': arg[9]
                          },
                          'data': {'NX_class': 'NXdata'},
                          'end_time': {'value': '%s' % arg[6]},
                          'experiment_identifier': {'value': '%s' % arg[2]},
                          'instrument': {
                              'NX_class': 'NXinstrument',
                              'detector': {
                                  'NX_class': 'NXdetector',
                                  'intimage': {
                                      'shape': [0, 30]
                                  },
                                  'spectrum': {
                                      'value': spectrum,
                                      'shape': [10]
                                  }
                              },

                              'name': {
                                  'short_name': '%s' % arg[4],
                                  'value': '%s' % arg[3]}},
                          'sample': {
                              'NX_class': 'NXsample',
                              'chemical_formula': {'value': '%s' % arg[8]},
                              'name': {'value': '%s' % arg[7]}},
                          'start_time': {
                              'value': '%s' % arg[5]},
                          'title': {'value': '%s' % arg[1]},
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
                         'ownerGroup': '99001234-part',
                         'datasetId':
                         '10.3204/99001234/myscan_%05i' % (i + 1),
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


if __name__ == '__main__':
    unittest.main()
