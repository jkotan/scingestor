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
class DatasetWatcherFIOTest(unittest.TestCase):

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
        if not isinstance(dct2, dict):
            print(dct)
            print(dct2)
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

    def test_datasetfile_exist_fio(self):
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
        fiofile = "mymeta2_00011.fio"
        fiosource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                 "config",
                                 fiofile)
        fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        chmod = "0o662"
        hattr = "nexdatas_source,nexdatas_strategy,units,NX_class,source," \
            "source_name,source_type,strategy,type"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        wrmodule = WRITERS[self.writer]
        filewriter.writer = wrmodule

        copymap = 'scientificMetadata.instrument_name ' \
            'scientificMetadata.instrument.name.value\n' \
            'scientificMetadata.sample_name ' \
            'scientificMetadata.sample.name.value\n' \
            'scientificMetadata.instrument.detector.intimage\n'
        cpmapname = "%s_%s.lst" % (self.__class__.__name__, fun)
        with open(cpmapname, "w+") as cf:
            cf.write(copymap)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'chmod_json_files: "{chmod}"\n' \
            'chmod_generator_switch: " -x {{chmod}} "\n' \
            'log_generator_commands: true\n' \
            'scicat_proposal_id_pattern: "{{beamtimeid}}"\n' \
            'add_empty_units: False\n' \
            'hidden_attributes: "{hattr}"\n' \
            'ingest_dataset_attachment: false\n' \
            'hidden_attributes_generator_switch: ' \
            '" -n {{hiddenattributes}} "\n' \
            'metadata_copy_map_file: "{cpmapfile}"\n' \
            'metadata_copy_map_file_generator_switch: ' \
            '" --copy-map-file {{copymapfile}} "\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir,
                credfile=credfile, chmod=chmod, hattr=hattr,
                cpmapfile=cpmapname)

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
                "myscan_00001.fio",
            ],
            [
                "myscan_00002.fio",
            ],
        ]

        try:
            for cmd in commands:
                time.sleep(1)
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                os.mkdir(fsubdirname3)

                for k, arg in enumerate(args):
                    fiofilename = os.path.join(fsubdirname2, arg[0])
                    dsfilename = fiofilename[:-4] + ".scan.json"
                    dbfilename = fiofilename[:-4] + ".origdatablock.json"

                    shutil.copy(fiosource, fiofilename)

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
                        'INFO : DatasetIngestor: Generating fio metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc1}.scan.json  '
                        '--id-format \'{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00001  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc1}.fio -r raw/special  -x 0o662  '
                        '-n nexdatas_source,nexdatas_strategy,units,NX_class,'
                        'source,source_name,source_type,strategy,type  '
                        '--copy-map-file {copymapfile}  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00001  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc1}.origdatablock.json  '
                        '-x 0o662  {subdir2}/{sc1} \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Generating fio metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc2}.scan.json  '
                        '--id-format \'{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00002  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc2}.fio -r raw/special  -x 0o662  '
                        '-n nexdatas_source,nexdatas_strategy,units,NX_class,'
                        'source,source_name,source_type,strategy,type  '
                        '--copy-map-file {copymapfile}  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00002  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc2}.origdatablock.json  '
                        '-x 0o662  {subdir2}/{sc2} \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc2}\n'
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
                                copymapfile=cpmapname,
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
                    "Datasets: 99001234/myscan_00001\n"
                    "OrigDatablocks: 99001234/myscan_00001\n"
                    "Datasets: 99001234/myscan_00002\n"
                    "OrigDatablocks: 99001234/myscan_00002\n", vl)
                self.assertEqual(len(self.__server.userslogin), 1)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 2)
                ds = json.loads(self.__server.datasets[0])
                self.myAssertDict(
                    ds,
                    {'accessGroups': [
                        '99001234-dmgt',
                        '99001234-clbt',
                        '99001234-part',
                        'p00dmgt',
                        'p00staff'],
                     'contactEmail': 'appuser@fake.com',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'datasetName': 'myscan_00001',
                     'description': 'H20 distribution',
                     'creationTime': 'Thu Dec  8 17:00:50 2022',
                     'endTime': 'Thu Dec  8 17:00:50 2022',
                     'isPublished': False,
                     'owner': 'Smithson',
                     'keywords': ['scan'],
                     'ownerEmail': 'peter.smithson@fake.de',
                     'ownerGroup': '99001234-dmgt',
                     'pid': '99001234/myscan_00001',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'ScanCommand': 'ascan exp_mot04 0.0 4.0 4 0.5',
                         'beamtimeId': '99001234',
                         'comments': {
                             'line_1': 'ascan exp_mot04 0.0 4.0 4 0.5',
                             'line_2':
                             'user jkotan Acquisition started at '
                             'Thu Dec  8 17:00:43 2022'},
                         'end_time': {
                             'value': 'Thu Dec  8 17:00:50 2022',
                             'unit': ''
                         },
                         'parameters': {
                             'abs': 1423,
                             'anav': 5.14532,
                             'atten': 99777400.0,
                             'bpm1': 7,
                             'hkl': [
                                 0.0031110747648095565,
                                 0.0024437328201669176,
                                 0.1910783136442638],
                             'rois_p100k': [
                                 228, 115, 238, 123, 227, 97, 252, 130,
                                 238, 115, 248, 123],
                             'sdd': None,
                             'signalcounter': 'p100k_roi1',
                             'ubmatrix':
                             '[[ 0.82633922 -0.80961862 -0.01117831]; '
                             '[ 0.02460193  0.0091408   1.15661358]; '
                             '[-0.80932194 -0.82636427  0.02374563]]'
                         },
                         'start_time': {
                             'value': 'Thu Dec  8 17:00:43 2022',
                             'unit': ''
                         }
                     },
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'techniques': [],
                     'type': 'raw'},
                    skip=["creationTime", "endTime",
                          "scientificMetadata.end_time",
                          "scientificMetadata.start_time"])
                self.assertTrue(ds["creationTime"].startswith(
                    "2022-12-08T17:00:50.000000"))
                self.assertTrue(ds["endTime"].startswith(
                    "2022-12-08T17:00:50.000000"))
                self.assertTrue(
                    ds["scientificMetadata"]["end_time"]["value"].
                    startswith("2022-12-08T17:00:50.000000"))
                self.assertTrue(
                    ds["scientificMetadata"]["start_time"]["value"].
                    startswith("2022-12-08T17:00:43.000000"))
                ds = json.loads(self.__server.datasets[1])
                self.myAssertDict(
                    ds,
                    {'accessGroups': [
                        '99001234-dmgt',
                        '99001234-clbt',
                        '99001234-part',
                        'p00dmgt',
                        'p00staff'],
                     'contactEmail': 'appuser@fake.com',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'datasetName': 'myscan_00002',
                     'description': 'H20 distribution',
                     'creationTime': 'Thu Dec  8 17:00:50 2022',
                     'endTime': 'Thu Dec  8 17:00:50 2022',
                     'isPublished': False,
                     'owner': 'Smithson',
                     'keywords': ['scan'],
                     'ownerEmail': 'peter.smithson@fake.de',
                     'ownerGroup': '99001234-dmgt',
                     'pid': '99001234/myscan_00002',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'ScanCommand': 'ascan exp_mot04 0.0 4.0 4 0.5',
                         'beamtimeId': '99001234',
                         'comments': {
                             'line_1': 'ascan exp_mot04 0.0 4.0 4 0.5',
                             'line_2':
                             'user jkotan Acquisition started at '
                             'Thu Dec  8 17:00:43 2022'},
                         'end_time': {
                             'value': 'Thu Dec  8 17:00:50 2022',
                             'unit': ''
                         },
                         'parameters': {
                             'abs': 1423,
                             'anav': 5.14532,
                             'atten': 99777400.0,
                             'bpm1': 7,
                             'hkl': [
                                 0.0031110747648095565,
                                 0.0024437328201669176,
                                 0.1910783136442638],
                             'rois_p100k': [
                                 228, 115, 238, 123, 227, 97, 252, 130,
                                 238, 115, 248, 123],
                             'sdd': None,
                             'signalcounter': 'p100k_roi1',
                             'ubmatrix':
                             '[[ 0.82633922 -0.80961862 -0.01117831]; '
                             '[ 0.02460193  0.0091408   1.15661358]; '
                             '[-0.80932194 -0.82636427  0.02374563]]'
                         },
                         'start_time': {
                             'value': 'Thu Dec  8 17:00:43 2022',
                             'unit': ''
                         }
                     },
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'techniques': [],
                     'type': 'raw'},
                    skip=["creationTime", "endTime",
                          "scientificMetadata.end_time",
                          "scientificMetadata.start_time"])
                self.assertTrue(ds["creationTime"].startswith(
                    "2022-12-08T17:00:50.000000"))
                self.assertTrue(ds["endTime"].startswith(
                    "2022-12-08T17:00:50.000000"))
                self.assertTrue(
                    ds["scientificMetadata"]["end_time"]["value"].
                    startswith("2022-12-08T17:00:50.000000"))
                self.assertTrue(
                    ds["scientificMetadata"]["start_time"]["value"].
                    startswith("2022-12-08T17:00:43.000000"))
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
                     'datasetId': '99001234/myscan_00001',
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
                     'datasetId': '99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.exists(cpmapname):
                os.remove(cpmapname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_exist_fio_attachment(self):
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
        fiofile = "mymeta2_00011.fio"
        fiosource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                 "config",
                                 fiofile)
        fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        chmod = "0o662"
        hattr = "nexdatas_source,nexdatas_strategy,units,NX_class,source," \
            "source_name,source_type,strategy,type"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        wrmodule = WRITERS[self.writer]
        filewriter.writer = wrmodule

        copymap = 'scientificMetadata.instrument_name ' \
            'scientificMetadata.instrument.name.value\n' \
            'scientificMetadata.sample_name ' \
            'scientificMetadata.sample.name.value\n' \
            'scientificMetadata.instrument.detector.intimage\n'
        cpmapname = "%s_%s.lst" % (self.__class__.__name__, fun)
        with open(cpmapname, "w+") as cf:
            cf.write(copymap)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'chmod_json_files: "{chmod}"\n' \
            'chmod_generator_switch: " -x {{chmod}} "\n' \
            'log_generator_commands: true\n' \
            'add_empty_units: False\n' \
            'hidden_attributes: "{hattr}"\n' \
            'hidden_attributes_generator_switch: ' \
            '" -n {{hiddenattributes}} "\n' \
            'metadata_copy_map_file: "{cpmapfile}"\n' \
            'metadata_copy_map_file_generator_switch: ' \
            '" --copy-map-file {{copymapfile}} "\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir,
                credfile=credfile, chmod=chmod, hattr=hattr,
                cpmapfile=cpmapname)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingestor -c %s -r15 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r15 -l debug'
                     % cfgfname).split()]
        # commands.pop()

        args = [
            [
                "myscan_00001.fio",
            ],
            [
                "myscan_00002.fio",
            ],
        ]

        try:
            for cmd in commands:
                time.sleep(1)
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                os.mkdir(fsubdirname3)

                for k, arg in enumerate(args):
                    fiofilename = os.path.join(fsubdirname2, arg[0])
                    dsfilename = fiofilename[:-4] + ".scan.json"
                    dbfilename = fiofilename[:-4] + ".origdatablock.json"
                    atfilename = fiofilename[:-4] + ".attachment.json"

                    shutil.copy(fiosource, fiofilename)

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
                status = os.stat(atfilename)
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
                        'INFO : DatasetIngestor: Generating fio metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc1}.scan.json  '
                        '--id-format \'{{proposalId}}.{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00001  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc1}.fio -r raw/special  -x 0o662  '
                        '-n nexdatas_source,nexdatas_strategy,units,NX_class,'
                        'source,source_name,source_type,strategy,type  '
                        '--copy-map-file {copymapfile}  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00001  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc1}.origdatablock.json  '
                        '-x 0o662  {subdir2}/{sc1} \n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc1} {subdir2}/{sc1}.attachment.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment command: '
                        'nxsfileinfo attachment  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        ' -n \'\' -o {subdir2}/{sc1}.attachment.json  '
                        '{subdir2}/{sc1}.fio -x 0o662 \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Generating fio metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc2}.scan.json  '
                        '--id-format \'{{proposalId}}.{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00002  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc2}.fio -r raw/special  -x 0o662  '
                        '-n nexdatas_source,nexdatas_strategy,units,NX_class,'
                        'source,source_name,source_type,strategy,type  '
                        '--copy-map-file {copymapfile}  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00002  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc2}.origdatablock.json  '
                        '-x 0o662  {subdir2}/{sc2} \n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc2} {subdir2}/{sc2}.attachment.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment command: '
                        'nxsfileinfo attachment  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        ' -n \'\' -o {subdir2}/{sc2}.attachment.json  '
                        '{subdir2}/{sc2}.fio -x 0o662 \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc2}\n'
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
                                copymapfile=cpmapname,
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
                    "Datasets: 99001234/myscan_00001\n"
                    "OrigDatablocks: 99001234/myscan_00001\n"
                    "Datasets Attachments: 99001234/myscan_00001\n"
                    "Datasets: 99001234/myscan_00002\n"
                    "OrigDatablocks: 99001234/myscan_00002\n"
                    "Datasets Attachments: 99001234/myscan_00002\n", vl)
                self.assertEqual(len(self.__server.userslogin), 1)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 2)
                ds = json.loads(self.__server.datasets[0])
                self.myAssertDict(
                    ds,
                    {'accessGroups': [
                        '99001234-dmgt',
                        '99001234-clbt',
                        '99001234-part',
                        'p00dmgt',
                        'p00staff'],
                     'contactEmail': 'appuser@fake.com',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'datasetName': 'myscan_00001',
                     'description': 'H20 distribution',
                     'creationTime': 'Thu Dec  8 17:00:50 2022',
                     'endTime': 'Thu Dec  8 17:00:50 2022',
                     'isPublished': False,
                     'owner': 'Smithson',
                     'keywords': ['scan'],
                     'ownerEmail': 'peter.smithson@fake.de',
                     'ownerGroup': '99001234-dmgt',
                     'pid': '99001234/myscan_00001',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99991173.99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'ScanCommand': 'ascan exp_mot04 0.0 4.0 4 0.5',
                         'beamtimeId': '99001234',
                         'comments': {
                             'line_1': 'ascan exp_mot04 0.0 4.0 4 0.5',
                             'line_2':
                             'user jkotan Acquisition started at '
                             'Thu Dec  8 17:00:43 2022'},
                         'end_time': {
                             'value': 'Thu Dec  8 17:00:50 2022',
                             'unit': ''
                         },
                         'parameters': {
                             'abs': 1423,
                             'anav': 5.14532,
                             'atten': 99777400.0,
                             'bpm1': 7,
                             'hkl': [
                                 0.0031110747648095565,
                                 0.0024437328201669176,
                                 0.1910783136442638],
                             'rois_p100k': [
                                 228, 115, 238, 123, 227, 97, 252, 130,
                                 238, 115, 248, 123],
                             'sdd': None,
                             'signalcounter': 'p100k_roi1',
                             'ubmatrix':
                             '[[ 0.82633922 -0.80961862 -0.01117831]; '
                             '[ 0.02460193  0.0091408   1.15661358]; '
                             '[-0.80932194 -0.82636427  0.02374563]]'
                         },
                         'start_time': {
                             'value': 'Thu Dec  8 17:00:43 2022',
                             'unit': ''
                         }
                     },
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'techniques': [],
                     'type': 'raw'},
                    skip=["creationTime", "endTime",
                          "scientificMetadata.end_time",
                          "scientificMetadata.start_time"])
                self.assertTrue(ds["creationTime"].startswith(
                    "2022-12-08T17:00:50.000000"))
                self.assertTrue(ds["endTime"].startswith(
                    "2022-12-08T17:00:50.000000"))
                self.assertTrue(
                    ds["scientificMetadata"]["end_time"]["value"].
                    startswith("2022-12-08T17:00:50.000000"))
                self.assertTrue(
                    ds["scientificMetadata"]["start_time"]["value"].
                    startswith("2022-12-08T17:00:43.000000"))
                ds = json.loads(self.__server.datasets[1])
                self.myAssertDict(
                    ds,
                    {'accessGroups': [
                        '99001234-dmgt',
                        '99001234-clbt',
                        '99001234-part',
                        'p00dmgt',
                        'p00staff'],
                     'contactEmail': 'appuser@fake.com',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'datasetName': 'myscan_00002',
                     'description': 'H20 distribution',
                     'creationTime': 'Thu Dec  8 17:00:50 2022',
                     'endTime': 'Thu Dec  8 17:00:50 2022',
                     'isPublished': False,
                     'owner': 'Smithson',
                     'keywords': ['scan'],
                     'ownerEmail': 'peter.smithson@fake.de',
                     'ownerGroup': '99001234-dmgt',
                     'pid': '99001234/myscan_00002',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99991173.99001234',
                     'scientificMetadata': {
                         'DOOR_proposalId': '99991173',
                         'ScanCommand': 'ascan exp_mot04 0.0 4.0 4 0.5',
                         'beamtimeId': '99001234',
                         'comments': {
                             'line_1': 'ascan exp_mot04 0.0 4.0 4 0.5',
                             'line_2':
                             'user jkotan Acquisition started at '
                             'Thu Dec  8 17:00:43 2022'},
                         'end_time': {
                             'value': 'Thu Dec  8 17:00:50 2022',
                             'unit': ''
                         },
                         'parameters': {
                             'abs': 1423,
                             'anav': 5.14532,
                             'atten': 99777400.0,
                             'bpm1': 7,
                             'hkl': [
                                 0.0031110747648095565,
                                 0.0024437328201669176,
                                 0.1910783136442638],
                             'rois_p100k': [
                                 228, 115, 238, 123, 227, 97, 252, 130,
                                 238, 115, 248, 123],
                             'sdd': None,
                             'signalcounter': 'p100k_roi1',
                             'ubmatrix':
                             '[[ 0.82633922 -0.80961862 -0.01117831]; '
                             '[ 0.02460193  0.0091408   1.15661358]; '
                             '[-0.80932194 -0.82636427  0.02374563]]'
                         },
                         'start_time': {
                             'value': 'Thu Dec  8 17:00:43 2022',
                             'unit': ''
                         }
                     },
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'techniques': [],
                     'type': 'raw'},
                    skip=["creationTime", "endTime",
                          "scientificMetadata.end_time",
                          "scientificMetadata.start_time"])
                self.assertTrue(ds["creationTime"].startswith(
                    "2022-12-08T17:00:50.000000"))
                self.assertTrue(ds["endTime"].startswith(
                    "2022-12-08T17:00:50.000000"))
                self.assertTrue(
                    ds["scientificMetadata"]["end_time"]["value"].
                    startswith("2022-12-08T17:00:50.000000"))
                self.assertTrue(
                    ds["scientificMetadata"]["start_time"]["value"].
                    startswith("2022-12-08T17:00:43.000000"))
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
                     'datasetId': '99001234/myscan_00001',
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
                     'datasetId': '99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'size': 629}, skip=["dataFileList", "size"])
                self.assertEqual(len(self.__server.attachments), 2)
                self.assertEqual(len(self.__server.attachments[0]), 2)
                self.assertEqual(self.__server.attachments[0][0],
                                 '99001234/myscan_00001')
                self.myAssertDict(
                    json.loads(self.__server.attachments[0][1]),
                    {
                        'ownerGroup': '99001234-dmgt',
                        'datasetId': '99001234/myscan_00001',
                        'caption': '',
                        'thumbnail':
                        "data:image/png;base64,i",
                        'accessGroups': [
                            '99001234-dmgt', '99001234-clbt', '99001234-part',
                            'p00dmgt', 'p00staff'],
                    }, skip=["thumbnail"])
                self.assertTrue(
                    json.loads(
                        self.__server.attachments[0][1])["thumbnail"].
                    startswith("data:image/png;base64,i"))
                self.assertEqual(len(self.__server.attachments[1]), 2)
                self.assertEqual(self.__server.attachments[1][0],
                                 '99001234/myscan_00002')
                self.myAssertDict(
                    json.loads(self.__server.attachments[1][1]),
                    {
                        'ownerGroup': '99001234-dmgt',
                        'datasetId': '99001234/myscan_00002',
                        'caption': '',
                        'thumbnail':
                        "data:image/png;base64,i",
                        'accessGroups': [
                            '99001234-dmgt', '99001234-clbt', '99001234-part',
                            'p00dmgt', 'p00staff'],
                    }, skip=["thumbnail"])
                self.assertTrue(
                    json.loads(
                        self.__server.attachments[1][1])["thumbnail"].
                    startswith("data:image/png;base64,i"))
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.exists(cpmapname):
                os.remove(cpmapname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_add_fio(self):
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
        fiofile = "mymeta2_00011.fio"
        fiosource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                 "config",
                                 fiofile)
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
            'oned_in_metadata: true\n' \
            'scicat_proposal_id_pattern: "{{beamtimeid}}"\n' \
            'oned_dataset_generator_switch: " --oned "\n' \
            'ingest_dataset_attachment: false\n' \
            'master_file_extension_list:\n' \
            '  - "fio"\n' \
            'add_empty_units: False\n' \
            'log_generator_commands: true\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_username: "{username}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir,
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
            "myscan_%05i.fio",
        ]

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

                for k in range(4):
                    fiofilename = os.path.join(
                        fsubdirname2, arg[0] % (k + 1))
                    shutil.copy(fiosource, fiofilename)

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
                        'INFO : DatasetIngestor: Generating fio metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc1}.scan.json  '
                        '--id-format \'{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00001  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc1}.fio -r raw/special  --oned  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00001  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc1}.origdatablock.json  '
                        '{subdir2}/{sc1} \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Generating fio metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc2}.scan.json  '
                        '--id-format \'{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00002  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc2}.fio -r raw/special  --oned  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00002  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc2}.origdatablock.json  '
                        '{subdir2}/{sc2} \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc3}\n'
                        'INFO : DatasetIngestor: Generating fio metadata: '
                        '{sc3} {subdir2}/{sc3}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc3}.scan.json  '
                        '--id-format \'{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00003  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc3}.fio -r raw/special  --oned  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc3} {subdir2}/{sc3}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00003  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc3}.origdatablock.json  '
                        '{subdir2}/{sc3} \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc4}\n'
                        'INFO : DatasetIngestor: Generating fio metadata: '
                        '{sc4} {subdir2}/{sc4}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc4}.scan.json  '
                        '--id-format \'{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00004  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc4}.fio -r raw/special  --oned  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc4} {subdir2}/{sc4}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00004  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc4}.origdatablock.json  '
                        '{subdir2}/{sc4} \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc4}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc4}\n'
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
                    "Datasets: 99001234/myscan_00001\n"
                    "OrigDatablocks: 99001234/myscan_00001\n"
                    "Datasets: 99001234/myscan_00002\n"
                    "OrigDatablocks: 99001234/myscan_00002\n"
                    'Login: myingestor\n'
                    "Datasets: 99001234/myscan_00003\n"
                    "OrigDatablocks: 99001234/myscan_00003\n"
                    "Datasets: 99001234/myscan_00004\n"
                    "OrigDatablocks: 99001234/myscan_00004\n", vl)
                self.assertEqual(len(self.__server.userslogin), 2)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 4)
                for i in range(4):
                    ds = json.loads(self.__server.datasets[i])
                    self.myAssertDict(
                        ds,
                        {'accessGroups': [
                            '99001234-dmgt',
                            '99001234-clbt',
                            '99001234-part',
                            'p00dmgt',
                            'p00staff'],
                         'contactEmail': 'appuser@fake.com',
                         'instrumentId': '/petra3/p00',
                         'creationLocation': '/DESY/PETRA III/P00',
                         'pid': '99001234/myscan_%05i' % (i + 1),
                         'datasetName': 'myscan_%05i' % (i + 1),
                         'description': 'H20 distribution',
                         'creationTime': 'Thu Dec  8 17:00:50 2022',
                         'endTime': 'Thu Dec  8 17:00:50 2022',
                         'isPublished': False,
                         'owner': 'Smithson',
                         'keywords': ['scan'],
                         'ownerEmail': 'peter.smithson@fake.de',
                         'ownerGroup': '99001234-dmgt',
                         'principalInvestigator': 'appuser@fake.com',
                         'proposalId': '99001234',
                         'scientificMetadata': {
                             'DOOR_proposalId': '99991173',
                             'ScanCommand': 'ascan exp_mot04 0.0 4.0 4 0.5',
                             'beamtimeId': '99001234',
                             'comments': {
                                 'line_1': 'ascan exp_mot04 0.0 4.0 4 0.5',
                                 'line_2':
                                 'user jkotan Acquisition started at '
                                 'Thu Dec  8 17:00:43 2022'},
                             'end_time': {
                                 'value': 'Thu Dec  8 17:00:50 2022',
                                 'unit': ''
                             },
                             'data': {
                                 'bpm1c': [-1.0, -1.0, -1.0, -1.0, -1.0],
                                 'exp_c02': [140.4539012230071,
                                             43.03782737462626,
                                             2.8750752655385,
                                             32.14813145322391,
                                             133.6535350066115],
                                 'exp_c03': [3.6754749142642638,
                                             51.76074517540757,
                                             177.15589117792712,
                                             52.376975207311894,
                                             2.9541814712878325],
                                 'exp_mot04': [0.0, 1.0, 2.0, 3.0, 4.0],
                                 'exp_t01': [0.5, 0.5, 0.5, 0.5, 0.5],
                                 'p09/motor/exp.08/Position':
                                 [1.0, 1.0, 1.0, 1.0, 1.0],
                                 'timestamp': [2.284174680709839,
                                               3.3944602012634277,
                                               4.407033920288086,
                                               5.524206161499023,
                                               6.656368255615234]
                             },
                             'parameters': {
                                 'abs': 1423,
                                 'anav': 5.14532,
                                 'atten': 99777400.0,
                                 'bpm1': 7,
                                 'hkl': [
                                     0.0031110747648095565,
                                     0.0024437328201669176,
                                     0.1910783136442638],
                                 'rois_p100k': [
                                     228, 115, 238, 123, 227, 97, 252, 130,
                                     238, 115, 248, 123],
                                 'sdd': None,
                                 'signalcounter': 'p100k_roi1',
                                 'ubmatrix':
                                 '[[ 0.82633922 -0.80961862 -0.01117831]; '
                                 '[ 0.02460193  0.0091408   1.15661358]; '
                                 '[-0.80932194 -0.82636427  0.02374563]]'
                             },
                             'start_time': {
                                 'value': 'Thu Dec  8 17:00:43 2022',
                                 'unit': ''
                             }
                         },
                         'sourceFolder':
                         '/asap3/petra3/'
                         'gpfs/p00/2022/data/9901234/raw/special',
                         'techniques': [],
                         'type': 'raw'},
                        skip=["creationTime", "endTime",
                              "scientificMetadata.end_time",
                              "scientificMetadata.start_time"])
                    self.assertTrue(ds["creationTime"].startswith(
                        "2022-12-08T17:00:50.000000"))
                    self.assertTrue(ds["endTime"].startswith(
                        "2022-12-08T17:00:50.000000"))
                    self.assertTrue(
                        ds["scientificMetadata"]["end_time"]["value"].
                        startswith("2022-12-08T17:00:50.000000"))
                    self.assertTrue(
                        ds["scientificMetadata"]["start_time"]["value"].
                        startswith("2022-12-08T17:00:43.000000"))

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
                         '99001234/myscan_%05i' % (i + 1),
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

    def test_datasetfile_add_fio_attachment(self):
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
        fiofile = "mymeta2_00011.fio"
        fiosource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                 "config",
                                 fiofile)
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
            'oned_in_metadata: true\n' \
            'oned_dataset_generator_switch: " --oned "\n' \
            'max_oned_size: 3\n' \
            'max_oned_dataset_generator_switch: ' \
            '" --max-oned-size {{maxonedsize}} "\n' \
            'master_file_extension_list:\n' \
            '  - "fio"\n' \
            'add_empty_units: False\n' \
            'log_generator_commands: true\n' \
            'attachment_signal_names: "exp_c04,exp_c02"\n' \
            'attachment_axes_names: "timestamp"\n' \
            'ingest_dataset_attachment: true\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_username: "{username}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir,
                username=username, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)

        wrmodule = WRITERS[self.writer]
        filewriter.writer = wrmodule

        commands = [('scicat_dataset_ingestor -c %s -r40 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r40 -l debug'
                     % cfgfname).split()]

        arg = [
            "myscan_%05i.fio",
        ]

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

                for k in range(4):
                    fiofilename = os.path.join(
                        fsubdirname2, arg[0] % (k + 1))
                    shutil.copy(fiosource, fiofilename)

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
                        'INFO : DatasetIngestor: Generating fio metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc1}.scan.json  '
                        '--id-format \'{{proposalId}}.{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00001  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc1}.fio -r raw/special  --oned  '
                        '--max-oned-size 3  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00001  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc1}.origdatablock.json  '
                        '{subdir2}/{sc1} \n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc1} {subdir2}/{sc1}.attachment.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment command: '
                        'nxsfileinfo attachment  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        ' -n \'\' -o {subdir2}/{sc1}.attachment.json  '
                        '{subdir2}/{sc1}.fio '
                        '-s exp_c04,exp_c02  -e timestamp \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Generating fio metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc2}.scan.json  '
                        '--id-format \'{{proposalId}}.{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00002  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc2}.fio -r raw/special  --oned  '
                        '--max-oned-size 3  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00002  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc2}.origdatablock.json  '
                        '{subdir2}/{sc2} \n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc2} {subdir2}/{sc2}.attachment.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment command: '
                        'nxsfileinfo attachment  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        ' -n \'\' -o {subdir2}/{sc2}.attachment.json  '
                        '{subdir2}/{sc2}.fio '
                        '-s exp_c04,exp_c02  -e timestamp \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc3}\n'
                        'INFO : DatasetIngestor: Generating fio metadata: '
                        '{sc3} {subdir2}/{sc3}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc3}.scan.json  '
                        '--id-format \'{{proposalId}}.{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00003  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc3}.fio -r raw/special  --oned  '
                        '--max-oned-size 3  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc3} {subdir2}/{sc3}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00003  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc3}.origdatablock.json  '
                        '{subdir2}/{sc3} \n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc3} {subdir2}/{sc3}.attachment.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment command: '
                        'nxsfileinfo attachment  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        ' -n \'\' -o {subdir2}/{sc3}.attachment.json  '
                        '{subdir2}/{sc3}.fio '
                        '-s exp_c04,exp_c02  -e timestamp \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc4}\n'
                        'INFO : DatasetIngestor: Generating fio metadata: '
                        '{sc4} {subdir2}/{sc4}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc4}.scan.json  '
                        '--id-format \'{{proposalId}}.{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00004  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc4}.fio -r raw/special  --oned  '
                        '--max-oned-size 3  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc4} {subdir2}/{sc4}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00004  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc4}.origdatablock.json  '
                        '{subdir2}/{sc4} \n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc4} {subdir2}/{sc4}.attachment.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment command: '
                        'nxsfileinfo attachment  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        ' -n \'\' -o {subdir2}/{sc4}.attachment.json  '
                        '{subdir2}/{sc4}.fio '
                        '-s exp_c04,exp_c02  -e timestamp \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc4}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc4}\n'
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
                    "Datasets: 99001234/myscan_00001\n"
                    "OrigDatablocks: 99001234/myscan_00001\n"
                    "Datasets Attachments: 99001234/myscan_00001\n"
                    "Datasets: 99001234/myscan_00002\n"
                    "OrigDatablocks: 99001234/myscan_00002\n"
                    "Datasets Attachments: 99001234/myscan_00002\n"
                    'Login: myingestor\n'
                    "Datasets: 99001234/myscan_00003\n"
                    "OrigDatablocks: 99001234/myscan_00003\n"
                    "Datasets Attachments: 99001234/myscan_00003\n"
                    "Datasets: 99001234/myscan_00004\n"
                    "OrigDatablocks: 99001234/myscan_00004\n"
                    "Datasets Attachments: 99001234/myscan_00004\n",
                    vl)
                self.assertEqual(len(self.__server.userslogin), 2)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 4)
                for i in range(4):
                    ds = json.loads(self.__server.datasets[i])
                    self.myAssertDict(
                        ds,
                        {'accessGroups': [
                            '99001234-dmgt',
                            '99001234-clbt',
                            '99001234-part',
                            'p00dmgt',
                            'p00staff'],
                         'contactEmail': 'appuser@fake.com',
                         'instrumentId': '/petra3/p00',
                         'creationLocation': '/DESY/PETRA III/P00',
                         'pid': '99001234/myscan_%05i' % (i + 1),
                         'datasetName': 'myscan_%05i' % (i + 1),
                         'description': 'H20 distribution',
                         'creationTime': 'Thu Dec  8 17:00:50 2022',
                         'endTime': 'Thu Dec  8 17:00:50 2022',
                         'isPublished': False,
                         'owner': 'Smithson',
                         'keywords': ['scan'],
                         'ownerEmail': 'peter.smithson@fake.de',
                         'ownerGroup': '99001234-dmgt',
                         'principalInvestigator': 'appuser@fake.com',
                         'proposalId': '99991173.99001234',
                         'scientificMetadata': {
                             'DOOR_proposalId': '99991173',
                             'ScanCommand': 'ascan exp_mot04 0.0 4.0 4 0.5',
                             'beamtimeId': '99001234',
                             'comments': {
                                 'line_1': 'ascan exp_mot04 0.0 4.0 4 0.5',
                                 'line_2':
                                 'user jkotan Acquisition started at '
                                 'Thu Dec  8 17:00:43 2022'},
                             'end_time': {
                                 'value': 'Thu Dec  8 17:00:50 2022',
                                 'unit': ''
                             },
                             'data': {
                                 'bpm1c': [-1.0, -1.0],
                                 'exp_c02': [140.4539012230071,
                                             133.6535350066115],
                                 'exp_c03': [3.6754749142642638,
                                             2.9541814712878325],
                                 'exp_mot04': [0.0, 4.0],
                                 'exp_t01': [0.5, 0.5],
                                 'p09/motor/exp.08/Position':
                                 [1.0, 1.0],
                                 'timestamp': [2.284174680709839,
                                               6.656368255615234]
                             },
                             'parameters': {
                                 'abs': 1423,
                                 'anav': 5.14532,
                                 'atten': 99777400.0,
                                 'bpm1': 7,
                                 'hkl': [
                                     0.0031110747648095565,
                                     0.0024437328201669176,
                                     0.1910783136442638],
                                 'rois_p100k': [
                                     228, 115, 238, 123, 227, 97, 252,
                                     130, 238, 115, 248, 123],
                                 'sdd': None,
                                 'signalcounter': 'p100k_roi1',
                                 'ubmatrix':
                                 '[[ 0.82633922 -0.80961862 -0.01117831]; '
                                 '[ 0.02460193  0.0091408   1.15661358]; '
                                 '[-0.80932194 -0.82636427  0.02374563]]'
                             },
                             'start_time': {
                                 'value': 'Thu Dec  8 17:00:43 2022',
                                 'unit': ''
                             }
                         },
                         'sourceFolder':
                         '/asap3/petra3/'
                         'gpfs/p00/2022/data/9901234/raw/special',
                         'techniques': [],
                         'type': 'raw'},
                        skip=["creationTime", "endTime",
                              "scientificMetadata.end_time",
                              "scientificMetadata.start_time"])
                    self.assertTrue(ds["creationTime"].startswith(
                        "2022-12-08T17:00:50.000000"))
                    self.assertTrue(ds["endTime"].startswith(
                        "2022-12-08T17:00:50.000000"))
                    self.assertTrue(
                        ds["scientificMetadata"]["end_time"]["value"].
                        startswith("2022-12-08T17:00:50.000000"))
                    self.assertTrue(
                        ds["scientificMetadata"]["start_time"]["value"].
                        startswith("2022-12-08T17:00:43.000000"))

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
                         '99001234/myscan_%05i' % (i + 1),
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'size': 629}, skip=["dataFileList", "size"])
                self.assertEqual(len(self.__server.attachments), 4)
                for i in range(4):
                    self.assertEqual(len(self.__server.attachments[i]), 2)
                    self.assertEqual(self.__server.attachments[i][0],
                                     '99001234/myscan_%05i' % (i + 1))
                    self.myAssertDict(
                        json.loads(self.__server.attachments[i][1]),
                        {
                            'thumbnail': "data:sdfsAAA%s" % i,
                            'caption': '',
                            'datasetId':
                            '99001234/myscan_%05i' % (i + 1),
                            'ownerGroup': '99001234-dmgt',
                            'accessGroups': [
                                '99001234-dmgt', '99001234-clbt',
                                '99001234-part', 'p00dmgt', 'p00staff']
                        },
                        skip=["thumbnail"])
                    self.assertTrue(
                        json.loads(
                            self.__server.attachments[i][1])["thumbnail"].
                        startswith("data:image/png;base64,i"))
                    # with open("myte_%05i.json" % (i + 1), "w") as cf:
                    #     cf.write(self.__server.attachments[i][1].decode("utf-8"))
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)


if __name__ == '__main__':
    unittest.main()
