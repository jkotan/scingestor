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

import sys
import unittest

import BeamtimeWatcher_test
import ScanDirWatcher_test
import DatasetWatcher_test
import DatasetIngest_test
import ModelIngest_test
import DatasetWatcherFIO_test

try:
    __import__("h5py")
    # if module h5py available
    H5PY_AVAILABLE = True
except ImportError as e:
    H5PY_AVAILABLE = False
    print("h5py is not available: %s" % e)

try:
    __import__("pninexus.h5cpp")
    # if module pninexus.h5cpp available
    H5CPP_AVAILABLE = True
except ImportError as e:
    H5CPP_AVAILABLE = False
    print("h5cpp is not available: %s" % e)
except SystemError as e:
    H5CPP_AVAILABLE = False
    print("h5cpp is not available: %s" % e)

if not H5PY_AVAILABLE and not H5CPP_AVAILABLE:
    raise Exception("Please install h5py or pninexus.h5cpp")

if H5CPP_AVAILABLE or H5PY_AVAILABLE:
    import DatasetWatcherH5_test


# main function
def main():

    basicsuite = unittest.TestSuite()
    basicsuite.addTests(
        unittest.defaultTestLoader.loadTestsFromModule(
            BeamtimeWatcher_test))
    basicsuite.addTests(
        unittest.defaultTestLoader.loadTestsFromModule(
            ScanDirWatcher_test))
    basicsuite.addTests(
        unittest.defaultTestLoader.loadTestsFromModule(
            DatasetWatcher_test))
    if H5CPP_AVAILABLE or H5PY_AVAILABLE:
        basicsuite.addTests(
            unittest.defaultTestLoader.loadTestsFromModule(
                DatasetWatcherH5_test))
    basicsuite.addTests(
        unittest.defaultTestLoader.loadTestsFromModule(
            DatasetIngest_test))
    basicsuite.addTests(
        unittest.defaultTestLoader.loadTestsFromModule(
            ModelIngest_test))
    basicsuite.addTests(
        unittest.defaultTestLoader.loadTestsFromModule(
            DatasetWatcherFIO_test))

    # test runner
    runner = unittest.TextTestRunner()

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'args', metavar='name', type=str, nargs='*',
        help='suite names: all, basic'
    )
    options = parser.parse_args()

    namesuite = {
        "basic": [basicsuite],
        "all": [basicsuite],
    }

    # print(options.args)
    if not options.args:
        options.args = ["all"]

    ts = []
    for nm in options.args:
        if nm in namesuite.keys():
            ts.extend(namesuite[nm])

    suite = unittest.TestSuite(ts)

    # test result

    tresult = runner.run(suite)
    print("Errors: %s" % tresult.errors)
    print("Failures: %s" % tresult.failures)
    print("Skipped: %s" % tresult.skipped)
    print("UnexpectedSuccesses: %s" % tresult.unexpectedSuccesses)
    print("ExpectedFailures: %s" % tresult.expectedFailures)
    result = tresult.wasSuccessful()
    print("Result: %s" % result)
    with open('testresult.txt', 'w') as fl:
        fl.write(str(int(not result)) + '\n')
    sys.exit(not result)


if __name__ == "__main__":
    main()
