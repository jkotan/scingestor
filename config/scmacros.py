#!/usr/bin/env python
""" SciCat Measurement macros """

import time
from sardana.macroserver.macro import (Macro, Type)
from sardana.macroserver.msexception import UnknownEnv

from nxstools.pyeval import scdataset


class stop_measurement(Macro):
    """ Stop SciCat measurement
    """

    param_def = [
    ]

    def run(self):
        try:
            scandir = self.getEnv("ScanDir")
        except UnknownEnv:
            scandir = ""
        if not scandir:
            self.error("ScanDir is not set")
            return

        try:
            scmeas = self.getEnv("SciCatMeasurements")
        except UnknownEnv:
            scmeas = {}
        oldname = ""
        if scandir in scmeas.keys():
            oldname = scmeas[scandir]

        scdataset.append_scicat_record(self, "__command__ stop")
        if oldname:
            name = "%s:%s" % (oldname, time.time())
            scdataset.append_scicat_record(self, name)
            scmeas[scandir] = ""
            self.setEnv("SciCatMeasurements", scmeas)
            self.output("Measurement '%s' in '%s' stopped"
                        % (oldname, scandir))


class update_measurement(Macro):
    """ Update SciCat measurement
    """

    param_def = [
    ]

    def run(self):
        try:
            scandir = self.getEnv("ScanDir")
        except UnknownEnv:
            scandir = ""
        if not scandir:
            self.error("ScanDir is not set")
            return

        try:
            scmeas = self.getEnv("SciCatMeasurements")
        except UnknownEnv:
            scmeas = {}
        oldname = ""
        if scandir in scmeas.keys():
            oldname = scmeas[scandir]

        scdataset.append_scicat_record(self, "__command__ stop")
        if oldname:
            name = "%s:%s" % (oldname, time.time())
            scdataset.append_scicat_record(self, name)
            scdataset.append_scicat_record(
                self, "__command__ start %s" % oldname)
            self.output("Measurement '%s' in '%s' updated"
                        % (oldname, scandir))


class show_current_measurement(Macro):
    """ Show current measurement
    """

    param_def = [
    ]

    def run(self):
        try:
            scandir = self.getEnv("ScanDir")
        except UnknownEnv:
            scandir = ""
        if not scandir:
            self.error("ScanDir is not set")
            return

        try:
            scmeas = self.getEnv("SciCatMeasurements")
        except UnknownEnv:
            scmeas = {}

        name = ""
        if scandir in scmeas.keys():
            name = scmeas[scandir]
        if name:
            self.output("Current Measurement for '%s' is '%s'"
                        % (scandir, name))
        else:
            self.output("Current Measurement for '%s' not set"
                        % (scandir))


class start_measurement(Macro):
    """ Start SciCat measurement
    """

    param_def = [
        ['name', Type.String, '', 'measurement name'],
        ['scandir', Type.String, '', 'scan directory'],
        ['conf', Type.String, '', 'measurement configuration file'],
    ]

    def run(self, name, scandir, conf):
        _start_measurement(self, name, scandir, conf, group_last_scan=False)


class make_measurement(Macro):
    """ make a new SciCat measurement from the last scan and next scans
    """

    param_def = [
        ['name', Type.String, '', 'measurement name'],
        ['scandir', Type.String, '', 'scan directory'],
        ['conf', Type.String, '', 'measurement configuration file'],
    ]

    def run(self, name, scandir, conf):
        _start_measurement(self, name, scandir, conf, group_last_scan=True)


def _start_measurement(macro, name, scandir, conf, group_last_scan=False):
    """start a measurement grouping the scans

    :param macro: hook macro
    :type macro: :class:`sardana.macroserver.macro.Macro`
    :param name: measurement name
    :type name: :obj:`str`
    :param scandir: scan directory
    :type scandif: :obj:`str`
    :param conf: 'measurement configuration file
    :type conf: :obj:`str`
    :param group_last_scan: group last scan flag
    :type group_last_scan: :obj:`bool`
    """
    if not scandir:
        try:
            scandir = macro.getEnv("ScanDir")
        except UnknownEnv:
            scandir = ""
    else:
        macro.setEnv("ScanDir", scandir)
    if not scandir:
        macro.error("ScanDir is not set")
        return

    try:
        scmeas = macro.getEnv("SciCatMeasurements")
    except UnknownEnv:
        scmeas = {}
    oldname = ""
    if scandir in scmeas.keys():
        oldname = scmeas[scandir]

    if oldname:
        scdataset.append_scicat_record(macro, "__command__ stop")
        oname = "%s:%s" % (oldname, time.time())
        scdataset.append_scicat_record(macro, oname)
        macro.setEnv("SciCatMeasurements", "")
        macro.output("Measurement '%s' in '%s' stopped"
                     % (oldname, scandir))
    if name:
        scdataset.append_scicat_record(
            macro, "__command__ start %s" % name)
        scmeas[scandir] = name
        macro.setEnv("SciCatMeasurements", scmeas)
        macro.output("Measurement '%s' in '%s' started"
                     % (name, scandir))
    if group_last_scan:
        scdataset.append_scicat_dataset(macro, reingest=group_last_scan)
