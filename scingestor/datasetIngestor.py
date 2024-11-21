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
import time
import enum
import socket
import pathlib
import shutil

from .logger import get_logger


class UpdateStrategy(enum.Enum):

    """ Update strategy
    """
    #: (:class:`scingestor.datasetIngestor.UpdateStrategy`)
    #:       leave datasets unchanged
    NO = 0
    #: (:class:`scingestor.datasetIngestor.UpdateStrategy`) patch datasets
    PATCH = 1
    #: (:class:`scingestor.datasetIngestor.UpdateStrategy`) recreate datasets
    CREATE = 2
    #: (:class:`scingestor.datasetIngestor.UpdateStrategy`) patch datasets only
    #:       if scientificMetadata changed otherwise recreate datasets
    MIXED = 3


class DatasetIngestor:

    """ Dataset Ingestor
    """

    def __init__(self, configuration,
                 path, dsfile, idsfile, meta, beamtimefile):
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
        :type meta: :obj:`dict` <:obj:`str`, `any`>
        :param beamtimefile: beamtime filename
        :type beamtimefile: :obj:`str`
        :param pidprefix: pidprefix
        :type pidprefix: :obj:`str`
        :param ingestorcred: ingestor credential
        :type ingestorcred: :obj:`str`
        :param scicat_url: scicat_url
        :type scicat_url: :obj:`str`
        """
        #: (:obj:`dict` <:obj:`str`, `any`>) ingestor configuration
        self.__config = configuration or {}
        #: (:obj:`str`) home directory
        self.__homepath = str(pathlib.Path.home())
        #: (:obj:`str`) master file extension
        self.__ext = 'nxs'
        #: (:obj:`str`) plot file extension
        self.__plotext = 'png'
        #: (:obj:`str`) file with a dataset list
        self.__dsfile = dsfile
        #: (:obj:`str`) file with a ingested dataset list
        self.__idsfile = idsfile
        #: (:obj:`str`) file with a ingested dataset tmp list
        self.__idsfiletmp = "%s%s" % (idsfile, ".tmp")
        #: (:obj:`str`) scan path dir
        self.__path = path
        #: (:obj:`str`) metadata path dir
        self.__metapath = path
        #: (:obj:`str`) beamtime id
        self.__bid = meta["beamtimeId"]
        #: (:obj:`str`) desy proposal id
        self.__dpid = meta["proposalId"]
        #: (:obj:`str`) beamline name
        self.__bl = meta["beamline"]
        #: (:obj:`str`) beamtime id
        self.__bfile = beamtimefile
        #: (:obj:`dict` <:obj:`str`, `any`>) beamtime metadata
        self.__meta = meta
        #: (:obj:`str`) indested scicat dataset file pattern
        self.__hostname = socket.gethostname()

        bpath, _ = os.path.split(beamtimefile)
        #: (:obj:`str`) relative scan path to beamtime path
        self.__relpath = os.path.relpath(path, bpath)

        #: (:obj:`str`) doi prefix
        self.__pidprefix = ""
        # self.__pidprefix = "10.3204/"
        #: (:obj:`str`) username
        self.__username = 'ingestor'
        #: (:obj:`str`) update strategy
        self.__strategy = UpdateStrategy.PATCH
        #: (:obj:`str`) beamtime id
        self.__incd = None
        #: (:obj:`bool`) relative path in datablock flag
        self.__relpath_in_datablock = False
        #: (:obj:`str`) scicat url
        self.__scicat_url = "http://localhost:3000/api/v3"
        #: (:obj:`str`) scicat users login
        self.__scicat_users_login = "Users/login"
        #: (:obj:`str`) scicat datasets class
        self.__scicat_datasets = "Datasets"
        #: (:obj:`str`) scicat proposal class
        self.__scicat_proposals = "Proposals"
        #: (:obj:`str`) scicat datablock class
        self.__scicat_datablocks = "OrigDatablocks"
        #: (:obj:`str`) scicat attachment class
        self.__scicat_attachments = "Datasets/{pid}/Attachments"
        #: (:obj:`str`) chmod string for json metadata
        self.__chmod = None
        #: (:obj:`str`) hidden attributes
        self.__hiddenattributes = None
        #: (:obj:`str`) attachment signals
        self.__attachmentsignals = None
        #: (:obj:`str`) attachment axes
        self.__attachmentaxes = None
        #: (:obj:`str`) attachment frame
        self.__attachmentframe = None
        #: (:obj:`bool`) ingest attachment flag
        self.__ingest_attachment = True
        #: (:obj:`bool`) retry failed dataset ingestion on next event
        self.__retry_failed_dataset_ingestion = True
        #: (:obj:`bool`) retry failed attachment ingestion on next event
        self.__retry_failed_attachment_ingestion = False
        #: (:obj:`str`) metadata copy map file
        self.__copymapfile = None
        #: (:obj:`str`) metadata group map file
        self.__groupmapfile = None
        #: (:obj:`bool`) oned metadata flag
        self.__oned = False
        #: (:obj:`bool`) raw groups flag
        self.__raw_groups = False
        #: (:obj:`int`) max oned size of metadata record
        self.__max_oned_size = None
        #: (:obj:`bool`) override attachment signals flag
        self.__override = False
        #: (:obj:`bool`) log generator command flag
        self.__logcommands = False
        #: (:obj:`bool`) empty units flag
        self.__emptyunits = True
        #: (:obj:`bool`) force measurement keyword flag
        self.__forcemeasurementkeyword = True
        #: (:obj:`bool`) force generate measurement flag
        self.__forcegeneratemeasurement = False
        #: (:obj:`bool`) skip multiple datablock ingestion
        self.__skip_multi_datablock = False
        #: (:obj:`bool`) single datablock ingestion
        self.__single_datablock = False
        #: (:obj:`bool`) skip multiple attachment ingestion
        self.__skip_multi_attachment = False
        #: (:obj:`bool`) skip scan dataset ingestion
        self.__skip_scan_dataset_ingestion = False

        #: (:obj:`int`) maximal counter value for post tries
        self.__maxcounter = 100

        #: (:obj:`str`) raw dataset scan postfix
        self.__scanpostfix = ".scan.json"
        #: (:obj:`str`) origin datablock scan postfix
        self.__datablockpostfix = ".origdatablock.json"
        #: (:obj:`str`) origin datablock scan postfix
        self.__attachmentpostfix = ".attachment.json"

        #: (:obj:`str`) nexus dataset shell command
        self.__datasetcommandfile = "nxsfileinfo metadata -k4 " \
            " -o {metapath}/{scanname}{scanpostfix} " \
            " --id-format '{idpattern}'" \
            " -z '{measurement}'" \
            " -e '{entryname}'" \
            " -b {beamtimefile} -p {beamtimeid}/{scanname} " \
            " -w {ownergroup}" \
            " -c {accessgroups}" \
            " {masterfile}"
        #: (:obj:`str`) datablock shell command
        self.__datasetcommand = "nxsfileinfo metadata -k4 " \
            " -o {metapath}/{scanname}{scanpostfix} " \
            " --id-format '{idpattern}'" \
            " -c {accessgroups}" \
            " -w {ownergroup}" \
            " -z '{measurement}'" \
            " -e '{entryname}'" \
            " -b {beamtimefile} -p {beamtimeid}/{scanname}"
        #: (:obj:`str`) datablock shell command
        self.__datablockcommand = "nxsfileinfo origdatablock " \
            " -s *.pyc,*{datablockpostfix},*{scanpostfix}," \
            "*{attachmentpostfix},*~  " \
            " -r '{dbrelpath}' " \
            " -p {pidprefix}{beamtimeid}/{scanname} " \
            " -w {ownergroup}" \
            " -c {accessgroups}" \
            " -o {metapath}/{scanname}{datablockpostfix} "
        #: (:obj:`str`) datablock shell command
        self.__datablockmemcommand = "nxsfileinfo origdatablock " \
            " -s *.pyc,*{datablockpostfix},*{scanpostfix}," \
            "*{attachmentpostfix},*~ " \
            " -w {ownergroup}" \
            " -c {accessgroups} " \
            " -r '{dbrelpath}' " \
            " -p {pidprefix}{beamtimeid}/{scanname} "
        #: (:obj:`str`) datablock path postfix
        self.__datablockscanpath = " {scanpath}/{scanname} "
        #: (:obj:`str`) attachment shell command
        self.__attachmentcommand = "nxsfileinfo attachment " \
            " -w {ownergroup} -c {accessgroups} " \
            " -n '{entryname}'" \
            " -o {metapath}/{scanname}{attachmentpostfix} " \
            " {plotfile}"
        #: (:obj:`str`) last measurement
        self.__measurement = ""
        #: (:obj:`set`<:obj:`str`>) current  measurements
        self.__measurements = set()
        #: (:obj:`bool`) measurement status
        self.__measurement_status = False
        #: (:obj:`bool`) call callback after each step
        self.__callcallback = False
        #: (:obj:`str`) metadata generated  shell callback
        self.__metadatageneratedcallback = "nxsfileinfo groupmetadata " \
            " {lastmeasurement} -m {metapath}/{scanname}{scanpostfix}" \
            " -d {metapath}/{scanname}{datablockpostfix}" \
            " -a {metapath}/{scanname}{attachmentpostfix}" \
            " -o {metapath}/{lastmeasurement}{scanpostfix}" \
            " -l {metapath}/{lastmeasurement}{datablockpostfix}" \
            " -t {metapath}/{lastmeasurement}{attachmentpostfix}" \
            " -p {beamtimeid}/{lastmeasurement} -f -k4 "

        #: (:obj:`str`) oned generator switch
        self.__oned_switch = " --oned "
        #: (:obj:`str`) raw group generator switch
        self.__raw_groups_switch = " --raw "
        #: (:obj:`str`) max oned size generator switch
        self.__max_oned_switch = " --max-oned-size {maxonedsize} "
        #: (:obj:`str`) copy map file generator switch
        self.__copymapfile_switch = " --copy-map-file {copymapfile} "
        #: (:obj:`str`) group map file generator switch
        self.__groupmapfile_switch = " --group-map-file {groupmapfile} "
        #: (:obj:`str`) empty units generator switch
        self.__emptyunits_switch = " --add-empty-units "
        #: (:obj:`str`) chmod generator switch
        self.__chmod_switch = " -x {chmod} "
        #: (:obj:`str`) hidden attributes generator switch
        self.__hiddenattributes_switch = " -n {hiddenattributes} "
        #: (:obj:`str`) relpath generator switch
        self.__relpath_switch = " -r {relpath} "
        #: (:obj:`str`) attachment signals generator switch
        self.__attachmentsignals_switch = " -s {signals} "
        #: (:obj:`str`) attachment axes generator switch
        self.__attachmentaxes_switch = " -e {axes} "
        #: (:obj:`str`) attachment frame generator switch
        self.__attachmentframe_switch = " -m {frame} "
        #: (:obj:`str`) attachment override signals switch
        self.__attachmentoverride_switch = " --override "

        #: (:obj:`dict` <:obj:`str`, :obj:`str`>) request headers
        self.__headers = {'Content-Type': 'application/json',
                          'Accept': 'application/json'}

        #: (:obj:`list`<:obj:`str`>) metadata keywords without checks
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

        #: (:obj:`list`<:obj:`str`>) ingested scan names
        self.__sc_ingested = []
        #: (:obj:`list`<:obj:`str`>) waiting scan names
        self.__sc_waiting = []
        #: (:obj:`dict`<:obj:`str`, :obj:`list`<:obj:`str`>>)
        #:   ingested scan names
        self.__sc_ingested_map = {}
        #: (:obj:`dict`<:obj:`str`, :obj:`list`<:obj:`str`>>)
        #:   semi-ingested scan names
        self.__sc_seingested_map = {}

        #: (:obj:`list` <:obj:`str`>) master file extension list
        self.__master_file_extension_list = ["nxs", "h5", "ndf", "nx", "fio"]

        #: (:obj:`list` <:obj:`str`>) plot file extension list
        self.__plot_file_extension_list = \
            ["png", "nxs", "h5", "ndf", "nx", "fio"]

        #: (:obj:`str`) proposalId pattern
        self.__idpattern = "{proposalId}.{beamtimeId}"
        # self.__idpattern = "{beamtimeId}"
        if "scicat_proposal_id_pattern" in self.__config.keys():
            self.__idpattern = \
                self.__config["scicat_proposal_id_pattern"].replace(
                    "{proposalid}", "{proposalId}").replace(
                        "{beamtimeid}", "{beamtimeId}")

        if "master_file_extension_list" in self.__config.keys() \
           and isinstance(self.__config["master_file_extension_list"], list):
            self.__master_file_extension_list = []
            for ext in self.__config["master_file_extension_list"]:
                if ext:
                    self.__master_file_extension_list.append(ext)
            if self.__master_file_extension_list:
                self.__ext = self.__master_file_extension_list[0]

        if "plot_file_extension_list" in self.__config.keys() \
           and isinstance(self.__config["plot_file_extension_list"], list):
            self.__plot_file_extension_list = []
            for ext in self.__config["plot_file_extension_list"]:
                if ext:
                    self.__plot_file_extension_list.append(ext)
            if self.__plot_file_extension_list:
                self.__plotext = self.__plot_file_extension_list[0]

        #: (:obj:`str`) access groups
        self.__accessgroups = \
            "{beamtimeid}-dmgt,{beamtimeid}-clbt,{beamtimeid}-part," \
            "{beamline}dmgt,{beamline}staff".format(
                beamtimeid=self.__bid, beamline=self.__bl)
        if "accessGroups" in self.__meta:
            self.__accessgroups = ",".join(self.__meta["accessGroups"])

        #: (:obj:`str`) owner group
        self.__ownergroup = \
            "{beamtimeid}-dmgt".format(
                beamtimeid=self.__bid)
        if "ownerGroup" in self.__meta:
            self.__ownergroup = self.__meta["ownerGroup"]

        #: (:obj:`bool`) metadata in log dir flag
        self.__meta_in_var_dir = True
        if "metadata_in_var_dir" in self.__config.keys():
            self.__meta_in_var_dir = self.__config["metadata_in_var_dir"]

        #: (:obj:`str`) ingestor log directory
        self.__var_dir = ""
        if "ingestor_var_dir" in self.__config.keys():
            self.__var_dir = str(
                self.__config["ingestor_var_dir"]).format(
                    beamtimeid=self.__bid,
                    homepath=self.__homepath)
        if self.__var_dir == "/":
            self.__var_dir = ""
        if self.__meta_in_var_dir and self.__var_dir:
            self.__metapath = "%s%s" % (self.__var_dir, self.__metapath)
            if not os.path.isdir(self.__metapath):
                os.makedirs(self.__metapath, exist_ok=True)

        if "dataset_pid_prefix" in self.__config.keys():
            self.__pidprefix = self.__config["dataset_pid_prefix"]
        if "ingestor_credential_file" in self.__config.keys():
            with open(self.__config["ingestor_credential_file"].format(
                    homepath=self.__homepath)) as fl:
                self.__incd = fl.read().strip()
        if "ingestor_username" in self.__config.keys():
            self.__username = self.__config["ingestor_username"]
        if "dataset_update_strategy" in self.__config.keys():
            try:
                self.__strategy = UpdateStrategy[
                    str(self.__config["dataset_update_strategy"]).upper()]
            except Exception as e:
                get_logger().warning(
                    'Wrong UpdateStrategy value: %s' % str(e))

        if "scicat_url" in self.__config.keys():
            self.__scicat_url = self.__config["scicat_url"]
        if "scicat_datasets_path" in self.__config.keys():
            self.__scicat_datasets = self.__config["scicat_datasets_path"]
        if "scicat_proposals_path" in self.__config.keys():
            self.__scicat_proposals = self.__config["scicat_proposals_path"]
        if "scicat_datablocks_path" in self.__config.keys():
            self.__scicat_datablocks = self.__config["scicat_datablocks_path"]
        if "scicat_attachments_path" in self.__config.keys():
            self.__scicat_attachments = \
                self.__config["scicat_attachments_path"]
        if "scicat_users_login_path" in self.__config.keys():
            self.__scicat_users_login = \
                self.__config["scicat_users_login_path"]

        if "relative_path_in_datablock" in self.__config.keys():
            self.__relpath_in_datablock = \
                self.__config["relative_path_in_datablock"]
        if "chmod_json_files" in self.__config.keys():
            self.__chmod = self.__config["chmod_json_files"]
        if "hidden_attributes" in self.__config.keys():
            self.__hiddenattributes = self.__config["hidden_attributes"]
        if "attachment_signal_names" in self.__config.keys():
            self.__attachmentsignals = self.__config["attachment_signal_names"]
        if "attachment_axes_names" in self.__config.keys():
            self.__attachmentaxes = self.__config["attachment_axes_names"]
        if "attachment_image_frame_number" in self.__config.keys():
            self.__attachmentframe = \
                self.__config["attachment_image_frame_number"]
        if "metadata_copy_map_file" in self.__config.keys():
            self.__copymapfile = \
                self.__config["metadata_copy_map_file"].format(
                    homepath=self.__homepath)
        if "metadata_group_map_file" in self.__config.keys():
            self.__groupmapfile = \
                self.__config["metadata_group_map_file"].format(
                    homepath=self.__homepath)
        if "oned_in_metadata" in self.__config.keys():
            self.__oned = self.__config["oned_in_metadata"]
        if "raw_metadata_callback" in self.__config.keys():
            self.__raw_groups = self.__config["raw_metadata_callback"]
        if "max_oned_size" in self.__config.keys():
            self.__max_oned_size = self.__config["max_oned_size"]
        if "override_attachment_signals" in self.__config.keys():
            self.__override = self.__config["override_attachment_signals"]
        if "log_generator_commands" in self.__config.keys():
            self.__logcommands = self.__config["log_generator_commands"]
        if "ingest_dataset_attachment" in self.__config.keys():
            self.__ingest_attachment = \
                self.__config["ingest_dataset_attachment"]
        if "retry_failed_dataset_ingestion" in self.__config.keys():
            self.__retry_failed_dataset_ingestion = \
                self.__config["retry_failed_dataset_ingestion"]
        if "retry_failed_attachment_ingestion" in self.__config.keys():
            self.__retry_failed_attachment_ingestion = \
                self.__config["retry_failed_attachment_ingestion"]
        if "add_empty_units" in self.__config.keys():
            self.__emptyunits = self.__config["add_empty_units"]

        if "force_measurement_keyword" in self.__config.keys():
            self.__forcemeasurementkeyword = \
                self.__config["force_measurement_keyword"]

        if "force_generate_measurement" in self.__config.keys():
            self.__forcegeneratemeasurement = \
                self.__config["force_generate_measurement"]

        if "skip_multi_datablock_ingestion" in self.__config.keys():
            self.__skip_multi_datablock = \
                self.__config["skip_multi_datablock_ingestion"]
        if "single_datablock_ingestion" in self.__config.keys():
            self.__single_datablock = \
                self.__config["single_datablock_ingestion"]
        if "skip_multi_attachment_ingestion" in self.__config.keys():
            self.__skip_multi_attachment = \
                self.__config["skip_multi_attachment_ingestion"]
        if "skip_scan_dataset_ingestion" in self.__config.keys():
            self.__skip_scan_dataset_ingestion = \
                self.__config["skip_scan_dataset_ingestion"]

        if "scan_metadata_postfix" in self.__config.keys():
            self.__scanpostfix = self.__config["scan_metadata_postfix"]
        if "datablock_metadata_postfix" in self.__config.keys():
            self.__datablockpostfix = \
                self.__config["datablock_metadata_postfix"]
        if "attachment_metadata_postfix" in self.__config.keys():
            self.__attachmentpostfix = \
                self.__config["attachment_metadata_postfix"]

        if "file_dataset_metadata_generator" in self.__config.keys():
            self.__datasetcommandfile = \
                self.__config["file_dataset_metadata_generator"]
        if "dataset_metadata_generator" in self.__config.keys():
            self.__datasetcommand = \
                self.__config["dataset_metadata_generator"]
        if "datablock_metadata_generator" in self.__config.keys():
            self.__datablockcommand = \
                self.__config["datablock_metadata_generator"]
        if "datablock_metadata_stream_generator" in self.__config.keys():
            self.__datablockmemcommand = \
                self.__config["datablock_metadata_stream_generator"]
        if "datablock_metadata_generator_scanpath_postfix" \
           in self.__config.keys():
            self.__datablockscanpath = \
                self.__config["datablock_metadata_generator_scanpath_postfix"]
        if "attachment_metadata_generator" in self.__config.keys():
            self.__attachmentcommand = \
                self.__config["attachment_metadata_generator"]

        if "call_metadata_generated_callback" in self.__config.keys():
            self.__callcallback = bool(
                self.__config["call_metadata_generated_callback"])

        if "metadata_generated_callback" in self.__config.keys():
            self.__metadatageneratedcallback = \
                self.__config["metadata_generated_callback"]

        if "chmod_generator_switch" in self.__config.keys():
            self.__chmod_switch = \
                self.__config["chmod_generator_switch"]

        if "hidden_attributes_generator_switch" in self.__config.keys():
            self.__hiddenattributes_switch = \
                self.__config["hidden_attributes_generator_switch"]

        if "attachment_signals_generator_switch" in self.__config.keys():
            self.__attachmentsignals_switch = \
                self.__config["attachment_signals_generator_switch"]

        if "attachment_axes_generator_switch" in self.__config.keys():
            self.__attachmentaxes_switch = \
                self.__config["attachment_axes_generator_switch"]

        if "attachment_frame_generator_switch" in self.__config.keys():
            self.__attachmentframe_switch = \
                self.__config["attachment_frame_generator_switch"]

        if "metadata_copy_map_file_generator_switch" in self.__config.keys():
            self.__copymapfile_switch = \
                self.__config["metadata_copy_map_file_generator_switch"]

        if "metadata_group_map_file_generator_switch" in self.__config.keys():
            self.__groupmapfile_switch = \
                self.__config["metadata_group_map_file_generator_switch"]

        if "relative_path_generator_switch" in self.__config.keys():
            self.__relpath_switch = \
                self.__config["relative_path_generator_switch"]

        if "oned_dataset_generator_switch" in self.__config.keys():
            self.__oned_switch = \
                self.__config["oned_dataset_generator_switch"]

        if "raw_metadata_callback_switch" in self.__config.keys():
            self.__raw_groups_switch = \
                self.__config["raw_metadata_callback_switch"]

        if "max_oned_dataset_generator_switch" in self.__config.keys():
            self.__max_oned_switch = \
                self.__config["max_oned_dataset_generator_switch"]

        if "override_attachment_signals_generator_switch" \
                in self.__config.keys():
            self.__attachmentoverride_switch = \
                self.__config["override_attachment_signals_generator_switch"]

        if "add_empty_units_generator_switch" in self.__config.keys():
            self.__emptyunits_switch = \
                self.__config["add_empty_units_generator_switch"]

        if not self.__relpath_in_datablock:
            if "dataset_metadata_generator" not in self.__config.keys():
                self.__datasetcommand = \
                    self.__datasetcommand + self.__relpath_switch
            if "file_dataset_metadata_generator" not in self.__config.keys():
                self.__datasetcommandfile = \
                    self.__datasetcommandfile + self.__relpath_switch

        if self.__chmod is not None:
            if "dataset_metadata_generator" not in self.__config.keys():
                self.__datasetcommand = \
                    self.__datasetcommand + self.__chmod_switch
            if "file_dataset_metadata_generator" not in self.__config.keys():
                self.__datasetcommandfile = \
                    self.__datasetcommandfile + self.__chmod_switch
            if "datablock_metadata_generator" not in self.__config.keys():
                self.__datablockcommand = \
                    self.__datablockcommand + self.__chmod_switch
            if "datablock_metadata_stream_generator" \
               not in self.__config.keys():
                self.__datablockmemcommand = \
                    self.__datablockmemcommand + self.__chmod_switch
            if "attachment_metadata_generator" not in self.__config.keys():
                self.__attachmentcommand = \
                    self.__attachmentcommand + self.__chmod_switch
            if "metadata_generated_callback" not in self.__config.keys():
                self.__metadatageneratedcallback = \
                    self.__metadatageneratedcallback + self.__chmod_switch

        if self.__groupmapfile is not None:
            if "metadata_generated_callback" not in self.__config.keys():
                self.__metadatageneratedcallback = \
                    self.__metadatageneratedcallback + \
                    self.__groupmapfile_switch

        if self.__raw_groups:
            if "metadata_generated_callback" not in self.__config.keys():
                self.__metadatageneratedcallback = \
                    self.__metadatageneratedcallback + \
                    self.__raw_groups_switch

        if self.__hiddenattributes is not None:
            if "dataset_metadata_generator" not in self.__config.keys():
                self.__datasetcommand = \
                    self.__datasetcommand + self.__hiddenattributes_switch
            if "file_dataset_metadata_generator" not in self.__config.keys():
                self.__datasetcommandfile = \
                    self.__datasetcommandfile + self.__hiddenattributes_switch
        if self.__copymapfile is not None:
            if "dataset_metadata_generator" not in self.__config.keys():
                self.__datasetcommand = \
                    self.__datasetcommand + self.__copymapfile_switch
            if "file_dataset_metadata_generator" not in self.__config.keys():
                self.__datasetcommandfile = \
                    self.__datasetcommandfile + self.__copymapfile_switch
        if self.__oned:
            if "dataset_metadata_generator" not in self.__config.keys():
                self.__datasetcommand = \
                    self.__datasetcommand + self.__oned_switch
            if "file_dataset_metadata_generator" not in self.__config.keys():
                self.__datasetcommandfile = \
                    self.__datasetcommandfile + self.__oned_switch

        if self.__oned and self.__max_oned_size:
            if "dataset_metadata_generator" not in self.__config.keys():
                self.__datasetcommand = \
                    self.__datasetcommand + self.__max_oned_switch
            if "file_dataset_metadata_generator" not in self.__config.keys():
                self.__datasetcommandfile = \
                    self.__datasetcommandfile + self.__max_oned_switch

        if self.__emptyunits:
            if "dataset_metadata_generator" not in self.__config.keys():
                self.__datasetcommand = \
                    self.__datasetcommand + self.__emptyunits_switch
            if "file_dataset_metadata_generator" not in self.__config.keys():
                self.__datasetcommandfile = \
                    self.__datasetcommandfile + self.__emptyunits_switch

        if self.__attachmentsignals is not None:
            if "attachment_metadata_generator" not in self.__config.keys():
                self.__attachmentcommand = \
                    self.__attachmentcommand + self.__attachmentsignals_switch

        if self.__attachmentaxes is not None:
            if "attachment_metadata_generator" not in self.__config.keys():
                self.__attachmentcommand = \
                    self.__attachmentcommand + self.__attachmentaxes_switch

        if self.__attachmentframe is not None:
            if "attachment_metadata_generator" not in self.__config.keys():
                self.__attachmentcommand = \
                    self.__attachmentcommand + self.__attachmentframe_switch

        if self.__override:
            if "attachment_metadata_generator" not in self.__config.keys():
                self.__attachmentcommand = \
                    self.__attachmentcommand + self.__attachmentoverride_switch

        if "max_request_tries_number" in self.__config.keys():
            try:
                self.__maxcounter = int(
                    self.__config["max_request_tries_number"])
            except Exception as e:
                get_logger().warning('%s' % (str(e)))

        if "request_headers" in self.__config.keys():
            try:
                self.__headers = dict(
                    self.__config["request_headers"])
            except Exception as e:
                get_logger().warning('%s' % (str(e)))

        if "metadata_fields_without_checks" in self.__config.keys():
            try:
                self.__withoutsm = list(
                    self.__config["metadata_fields_without_checks"])
            except Exception as e:
                get_logger().warning('%s' % (str(e)))

        #: (:obj:`dict` <:obj:`str`, :obj:`str`>) command format parameters
        self.__dctfmt = {
            "scanname": None,
            "chmod": self.__chmod,
            "hiddenattributes": self.__hiddenattributes,
            "copymapfile": self.__copymapfile,
            "plotfile": "",
            "masterfile": "",
            "scanpath": self.__path,
            "metapath": self.__metapath,
            "relpath": self.__relpath,
            "dbrelpath": "",
            "beamtimeid": self.__bid,
            "beamline": self.__bl,
            "pidprefix": self.__pidprefix,
            "beamtimefile": self.__bfile,
            "scanpostfix": self.__scanpostfix,
            "datablockpostfix": self.__datablockpostfix,
            "attachmentpostfix": self.__attachmentpostfix,
            "ownergroup": self.__ownergroup,
            "accessgroups": self.__accessgroups,
            "hostname": self.__hostname,
            "homepath": self.__homepath,
            "ext": self.__ext,
            "plotext": self.__plotext,
            "signals": self.__attachmentsignals,
            "axes": self.__attachmentaxes,
            "frame": self.__attachmentframe,
            "maxonedsize": self.__max_oned_size,
            "measurement": self.__measurement,
            "lastmeasurement": self.__measurement,
            "groupmapfile": self.__groupmapfile,
            "masterscanname": "",
            "entryname": "",
            "idpattern": self.__idpattern,
        }
        self.__dctfmt["masterfile"] = \
            "{scanpath}/{masterscanname}.{ext}".format(**self.__dctfmt)
        self.__dctfmt["plotfile"] = \
            "{scanpath}/{masterscanname}.{plotext}".format(**self.__dctfmt)

        get_logger().debug(
            'DatasetIngestor: Parameters: %s' % str(self.__dctfmt))

        # self.__tokenurl = "http://www-science3d.desy.de:3000/api/v3/" \
        #       "Users/login"
        if not self.__scicat_url.endswith("/"):
            self.__scicat_url = self.__scicat_url + "/"
        #: (:obj:`str`) token url
        self.__tokenurl = self.__scicat_url + self.__scicat_users_login
        # get_logger().info(
        #     'DatasetIngestor: LOGIN %s' % self.__tokenurl)

        #: (:obj:`str`) dataset url
        self.__dataseturl = self.__scicat_url + self.__scicat_datasets
        # self.__dataseturl = "http://www-science3d.desy.de:3000/api/v3/" \
        #    "Datasets"
        #: (:obj:`str`) dataset url
        self.__proposalurl = self.__scicat_url + self.__scicat_proposals
        # self.__proposalurl = "http://www-science3d.desy.de:3000/api/v3/" \
        #    "Proposals"

        #: (:obj:`str`) origdatablock url
        self.__datablockurl = self.__scicat_url + self.__scicat_datablocks
        # self.__dataseturl = "http://www-science3d.desy.de:3000/api/v3/" \
        #     "OrigDatablocks"
        #: (:obj:`str`) origdatablock url

        #: (:obj:`str`) attachment url
        self.__attachmenturl = self.__scicat_url + self.__scicat_attachments
        # self.__dataseturl = "http://www-science3d.desy.de:3000/api/v3/" \
        #     "Datasets/{pid}/Attachments"
        #: (:obj:`str`) origdatablock url

    def _generate_rawdataset_metadata(self, scan):
        """ generate raw dataset metadata

        :param scan: scan name
        :type scan: :obj:`str`
        :returns: a file name of generate file
        :rtype: :obj:`str`
        """
        self.__ext = ""

        self.__dctfmt["masterfile"] = \
            "{scanpath}/{masterscanname}.{ext}".format(**self.__dctfmt)
        for ext in self.__master_file_extension_list:
            self.__dctfmt["ext"] = ext

            if os.path.isfile(
                    "{scanpath}/{masterscanname}.{ext}".format(
                        **self.__dctfmt)):
                self.__ext = ext
                self.__dctfmt["masterfile"] = \
                    "{scanpath}/{masterscanname}.{ext}".format(
                        **self.__dctfmt)
                break
        else:
            for ext in self.__master_file_extension_list:
                self.__dctfmt["ext"] = ext

                if os.path.isfile(
                        "{scanpath}/{scanname}/{scanname}.{ext}".
                        format(**self.__dctfmt)):
                    self.__ext = ext
                    self.__dctfmt["masterfile"] = \
                        "{scanpath}/{scanname}/{scanname}.{ext}".format(
                            **self.__dctfmt)
                    break

        self.__dctfmt["ext"] = self.__ext

        if self.__ext:

            if self.__dctfmt["masterscanname"] != self.__dctfmt["scanname"]:
                masterfile = self.__dctfmt["masterfile"]
                mdir, mfile = os.path.split(masterfile)
                if self.__meta_in_var_dir and self.__var_dir:
                    mdir = "%s%s" % (self.__var_dir, mdir)
                    if not os.path.isdir(mdir):
                        os.makedirs(mdir, exist_ok=True)
                fcnt = 1
                ffname = os.path.join(
                    mdir, "_tmp_scingestor_%s_%s" % (fcnt, mfile))
                while os.path.isfile(ffname):
                    fcnt += 1
                    ffname = os.path.join(
                        mdir, "_tmp_scingestor_%s_%s" % (fcnt, mfile))
                self.__dctfmt["masterfile"] = ffname

                shutil.copy(masterfile, self.__dctfmt["masterfile"])

            get_logger().info(
                'DatasetIngestor: Generating %s metadata: %s %s' % (
                    self.__ext, scan,
                    "{metapath}/{scanname}{scanpostfix}".format(
                        **self.__dctfmt)))
            command = self.__datasetcommandfile.format(**self.__dctfmt)
            if self.__logcommands:
                get_logger().info(
                    'DatasetIngestor: Generating dataset command: %s ' % (
                        command))
            else:
                get_logger().debug(
                    'DatasetIngestor: Generating dataset command: %s ' % (
                        command))
            subprocess.run(command, shell=True, check=True)

            if self.__dctfmt["masterscanname"] != self.__dctfmt["scanname"]:
                if os.path.isfile(self.__dctfmt["masterfile"]):
                    os.remove(self.__dctfmt["masterfile"])
                self.__dctfmt["masterfile"] = masterfile

        else:
            get_logger().info(
                'DatasetIngestor: Generating metadata: %s %s' % (
                    scan,
                    "{metapath}/{scanname}{scanpostfix}".format(
                        **self.__dctfmt)))
            command = self.__datasetcommand.format(**self.__dctfmt)
            if self.__logcommands:
                get_logger().info(
                    'DatasetIngestor: Generating dataset command: %s'
                    % (command))
            else:
                get_logger().debug(
                    'DatasetIngestor: Generating dataset command: %s'
                    % (command))
            subprocess.run(command, shell=True, check=True)

        rdss = glob.glob(
            "{metapath}/{scanname}{scanpostfix}".format(**self.__dctfmt))
        if rdss and rdss[0]:
            return rdss[0]

        return ""

    def _generate_origdatablock_metadata(self, scan):
        """ generate origdatablock metadata

        :param scan: scan name
        :type scan: :obj:`str`
        :returns: a file name of generate file
        :rtype: :obj:`str`
        """
        get_logger().info(
            'DatasetIngestor: Generating origdatablock metadata: %s %s' % (
                scan,
                "{metapath}/{scanname}{datablockpostfix}".format(
                    **self.__dctfmt)))
        cmd = self.__datablockcommand.format(**self.__dctfmt)
        sscan = (scan or "").split(" ")
        for sc in sscan:
            cmd += self.__datablockscanpath.format(
                scanpath=self.__dctfmt["scanpath"], scanname=sc)
        if self.__logcommands:
            get_logger().info(
                'DatasetIngestor: Generating origdatablock command: %s' % cmd)
        else:
            get_logger().debug(
                'DatasetIngestor: Generating origdatablock command: %s' % cmd)
        subprocess.run(cmd, shell=True, check=True)
        odbs = glob.glob(
            "{metapath}/{scanname}{datablockpostfix}".format(
                    **self.__dctfmt))
        if odbs and odbs[0]:
            return odbs[0]
        return ""

    def _generate_attachment_metadata(self, scan):
        """ generate origdatablock metadata

        :param scan: scan name
        :type scan: :obj:`str`
        :returns: a file name of generate file
        :rtype: :obj:`str`
        """
        self.__plotext = ""

        self.__dctfmt["plotfile"] = \
            "{scanpath}/{masterscanname}.{plotext}".format(**self.__dctfmt)
        for ext in self.__plot_file_extension_list:
            self.__dctfmt["plotext"] = ext

            if os.path.isfile(
                    "{scanpath}/{masterscanname}.{plotext}".format(
                        **self.__dctfmt)):
                self.__plotext = ext
                self.__dctfmt["plotfile"] = \
                    "{scanpath}/{masterscanname}.{plotext}".format(
                        **self.__dctfmt)
                break
        else:
            for ext in self.__plot_file_extension_list:
                self.__dctfmt["plotext"] = ext

                if os.path.isfile(
                        "{scanpath}/{scanname}/{scanname}.{plotext}".
                        format(**self.__dctfmt)):
                    self.__plotext = ext
                    self.__dctfmt["plotfile"] = \
                        "{scanpath}/{scanname}/{scanname}.{plotext}".format(
                            **self.__dctfmt)
                    break
        self.__dctfmt["plotext"] = self.__plotext

        if self.__dctfmt["plotext"]:

            if self.__dctfmt["masterscanname"] != self.__dctfmt["scanname"]:
                plotfile = self.__dctfmt["plotfile"]
                mdir, mfile = os.path.split(plotfile)
                if self.__meta_in_var_dir and self.__var_dir:
                    mdir = "%s%s" % (self.__var_dir, mdir)
                    if not os.path.isdir(mdir):
                        os.makedirs(mdir, exist_ok=True)
                fcnt = 1
                ffname = os.path.join(
                    mdir, "_tmp_scingestor_%s_%s" % (fcnt, mfile))
                while os.path.isfile(ffname):
                    fcnt += 1
                    ffname = os.path.join(
                        mdir, "_tmp_scingestor_%s_%s" % (fcnt, mfile))
                self.__dctfmt["plotfile"] = ffname
                shutil.copy(plotfile, self.__dctfmt["plotfile"])

            get_logger().info(
                'DatasetIngestor: Generating attachment metadata: %s %s' % (
                    scan,
                    "{metapath}/{scanname}{attachmentpostfix}".format(
                        **self.__dctfmt)))
            cmd = self.__attachmentcommand.format(**self.__dctfmt)
            if self.__logcommands:
                get_logger().info(
                    'DatasetIngestor: Generating attachment command: %s' % cmd)
            else:
                get_logger().debug(
                    'DatasetIngestor: Generating attachment command: %s' % cmd)
            subprocess.run(cmd, shell=True, check=True)

            if self.__dctfmt["masterscanname"] != self.__dctfmt["scanname"]:
                if os.path.isfile(self.__dctfmt["plotfile"]):
                    os.remove(self.__dctfmt["plotfile"])
                self.__dctfmt["plotfile"] = plotfile

            adss = glob.glob(
                "{metapath}/{scanname}{attachmentpostfix}".format(
                    **self.__dctfmt))
            if adss and adss[0]:
                return adss[0]
        return ""

    def _regenerate_origdatablock_metadata(self, scan,
                                           force=False, mfilename=""):
        """regenerate origdatablock metadata

        :param scan: scan name
        :type scan: :obj:`str`
        :param force: force flag
        :type force: :obj:`bool`
        :param mfilename: metadata file name
        :type mfilename: :obj:`str`
        :returns: a file name of generate file
        :rtype: :obj:`str`
        """
        mfilename = "{metapath}/{scanname}{datablockpostfix}".format(
            **self.__dctfmt)

        get_logger().info(
            'DatasetIngestor: Checking origdatablock metadata: %s %s' % (
                scan, mfilename))

        # cmd = self.__datablockcommand.format(**self.__dctfmt)
        dmeta = None
        try:
            with open(mfilename, "r") as mf:
                meta = mf.read()
                dmeta = json.loads(meta)
        except Exception as e:
            if not force:
                get_logger().warning('%s: %s' % (scan, str(e)))
        cmd = self.__datablockmemcommand.format(**self.__dctfmt)
        sscan = (scan or "").split(" ")
        if self.__datablockscanpath:
            dctfmt = dict(self.__dctfmt)
            for sc in sscan:
                dctfmt["scanname"] = sc
                cmd += self.__datablockscanpath.format(**dctfmt)
        get_logger().debug(
            'DatasetIngestor: Checking origdatablock command: %s ' % cmd)
        if self.__logcommands:
            get_logger().info(
                'DatasetIngestor: Generating origdatablock command: %s'
                % cmd)
        else:
            get_logger().debug(
                'DatasetIngestor: Generating origdatablock command: %s'
                % cmd)
        result = subprocess.run(
            cmd, shell=True,
            text=True, capture_output=True, check=True)
        nwmeta = str(result.stdout)
        if dmeta is None:
            with open(mfilename, "w") as mf:
                mf.write(nwmeta)
        else:
            try:
                dnwmeta = json.loads(nwmeta)
            except Exception as e:
                get_logger().warning('%s: %s' % (scan, str(e)))
                dnwmeta = None
            if dnwmeta is not None:
                if not self._metadataEqual(dmeta, dnwmeta) or force:
                    get_logger().info(
                        'DatasetIngestor: '
                        'Generating origdatablock metadata: %s %s' % (
                            scan,
                            "{metapath}/{scanname}{datablockpostfix}".format(
                                **self.__dctfmt)))
                    with open(mfilename, "w") as mf:
                        mf.write(nwmeta)

        odbs = glob.glob(mfilename)
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
                json={"username": self.__username, "password": self.__incd})
            if response.ok:
                return json.loads(response.content)["id"]
            else:
                raise Exception("%s" % response.text)
        except Exception as e:
            get_logger().error(
                'DatasetIngestor: %s' % (str(e)))
        return ""

    def append_proposal_groups(self):
        """ appends owner and access groups to beamtime

        :param meta: beamtime configuration
        :type meta: :obj:`dict` <:obj:`str`, `any`>
        :param path: base file path
        :type path: :obj:`str`
        :returns: updated beamtime configuration
        :rtype: :obj:`dict` <:obj:`str`, `any`>
        """
        token = self.get_token()
        bid = self.__meta["beamtimeId"]
        try:
            self.__headers["Authorization"] = "Bearer {}".format(token)
            propid = self.__idpattern.format(
                beamtimeId=self.__bid.replace("/", "%2F"),
                proposalId=self.__dpid.replace("/", "%2F"))
            resexists = requests.get(
                "{url}/{pid}"
                .format(
                    url=self.__proposalurl,
                    pid=propid),
                headers=self.__headers,
                params={"access_token": token}
            )

            if resexists.ok:
                pexists = bool(resexists.content)
            else:
                raise Exception(
                    "Proposal %s: %s"
                    % (propid, resexists.text or '{\"exists\": false}'))
            if pexists:
                resget = resexists
                if resget.ok:
                    proposal = json.loads(resget.content)
                    if "ownerGroup" not in self.__meta and \
                       "ownerGroup" in proposal:
                        self.__meta["ownerGroup"] = proposal["ownerGroup"]
                        self.__ownergroup = self.__meta["ownerGroup"]
                        self.__dctfmt["ownergroup"] = self.__ownergroup

                    if "accessGroups" not in self.__meta and \
                       "accessGroups" in proposal:
                        self.__meta["accessGroups"] = list(
                            proposal["accessGroups"])
                        self.__accessgroups = \
                            ",".join(self.__meta["accessGroups"])
                        self.__dctfmt["accessgroups"] = self.__accessgroups
                else:
                    raise Exception(
                        "Proposal %s: %s" % (bid, resget.text))
            else:
                raise Exception("Proposal %s: %s" %
                                (bid, resexists.text or '{\"exists\": false}'))
        except Exception as e:
            get_logger().warning('%s' % (str(e)))
        return self.__meta

    def _post_dataset(self, mdic, token, mdct):
        """ post dataset

        :param mdic: metadata in dct
        :type mdic: :obj:`dct` <:obj:`str`, `any`>
        :param token: ingestor token
        :type token: :obj:`str`
        :param mdct: metadata in dct
        :type mdct: :obj:`dct` <:obj:`str`, `any`>
        :returns: dataset pid
        :rtype: :obj:`str`
        """
        # create a new dataset since
        # core metadata of dataset were changed
        # find a new pid
        pexist = True
        npid = mdic["pid"]
        ipid = mdct["pid"]
        while pexist:
            npre = ""
            if npid.startswith(self.__pidprefix):
                npre = self.__pidprefix
                npid = npid[len(npre):]
            spid = npid.split("/")
            if len(spid) > 2:
                try:
                    ver = int(spid[-1])
                    spid[-1] = str(ver + 1)
                except Exception:
                    spid.append("2")
            else:
                spid.append("2")
            npid = npre + "/".join(spid)
            if len(spid) > 0:
                ipid = npre + "/".join(spid)
            self.__headers["Authorization"] = "Bearer {}".format(token)
            resexists = requests.get(
                "{url}/{pid}"
                .format(
                    url=self.__dataseturl,
                    pid=(npre + npid.replace("/", "%2F"))),
                headers=self.__headers,
                params={"access_token": token})
            if resexists.ok:
                pexist = bool(resexists.content)
            else:
                raise Exception("%s" % resexists.text)

        mdic["pid"] = ipid
        nmeta = json.dumps(mdic)
        get_logger().info(
            'DatasetIngestor: '
            'Post the dataset with a new pid: %s' % (npid))

        # post the dataset with the new pid
        self.__headers["Authorization"] = "Bearer {}".format(token)
        response = requests.post(
            self.__dataseturl,
            params={"access_token": token},
            headers=self.__headers,
            data=nmeta)
        if response.ok:
            return mdic["pid"]
        else:
            raise Exception("%s" % response.text)

    def _patch_dataset(self, nmeta, pid, token, mdct):
        """ post dataset

        :param nmeta: metadata in json string
        :type nmeta: :obj:`str`
        :param pid: dataset pid
        :type pid: :obj:`str`
        :param token: ingestor token
        :type token: :obj:`str`
        :param mdct: metadata in dct
        :type mdct: :obj:`dct` <:obj:`str`, `any`>
        :returns: dataset pid
        :rtype: :obj:`str`
        """
        get_logger().info(
            'DatasetIngestor: '
            'Patch scientificMetadata of dataset:'
            ' %s' % (pid))
        self.__headers["Authorization"] = "Bearer {}".format(token)
        response = requests.patch(
            "{url}/{pid}"
            .format(
                url=self.__dataseturl,
                pid=pid.replace("/", "%2F")),
            params={"access_token": token},
            headers=self.__headers,
            data=nmeta)
        if response.ok:
            return mdct["pid"]
        else:
            raise Exception("%s" % response.text)

    def _ingest_dataset(self, metadata, token, mdct):
        """ ingests dataset

        :param metadata: metadata in json string
        :type metadata: :obj:`str`
        :param token: ingestor token
        :type token: :obj:`str`
        :param mdct: metadata in dct
        :type mdct: :obj:`dct` <:obj:`str`, `any`>
        :returns: dataset pid
        :rtype: :obj:`str`
        """
        try:
            pid = "%s%s" % (self.__pidprefix, mdct["pid"])
            # check if dataset with the pid exists
            get_logger().info(
                'DatasetIngestor: Check if dataset exists: %s' % (pid))
            checking = True
            counter = 0
            self.__headers["Authorization"] = "Bearer {}".format(token)
            while checking:
                resexists = requests.get(
                    "{url}/{pid}".format(
                        url=self.__dataseturl,
                        pid=pid.replace("/", "%2F")),
                    headers=self.__headers,
                    params={"access_token": token}
                )
                if hasattr(resexists, "content"):
                    try:
                        json.loads(resexists.content)
                        checking = False
                    except Exception:
                        time.sleep(0.1)
                        if resexists.ok and hasattr(resexists, "content") and \
                           not bool(resexists.content):
                            checking = False
                        else:
                            time.sleep(0.1)
                if counter == self.__maxcounter:
                    checking = False
                counter += 1
            if resexists.ok and hasattr(resexists, "content"):
                try:
                    exists = bool(resexists.content)
                except Exception:
                    exists = False
                if not exists:
                    # post the new dataset since it does not exist
                    get_logger().info(
                        'DatasetIngestor: Post the dataset: %s' % (pid))
                    self.__headers["Authorization"] = "Bearer {}".format(token)
                    response = requests.post(
                        self.__dataseturl,
                        headers=self.__headers,
                        params={"access_token": token},
                        data=metadata)
                    if response.ok:
                        return mdct["pid"]
                    else:
                        raise Exception("%s" % response.text)
                elif self.__strategy != UpdateStrategy.NO:
                    # find dataset by pid
                    get_logger().info(
                        'DatasetIngestor: Find the dataset by id: %s' % (pid))
                    resds = requests.get(
                        "{url}/{pid}".format(
                            url=self.__dataseturl,
                            pid=pid.replace("/", "%2F")),
                        headers=self.__headers,
                        params={"access_token": token}
                    )
                    if resds.ok:
                        dsmeta = json.loads(resds.content)
                        mdic = dict(mdct)
                        mdic["pid"] = pid
                        if self.__forcemeasurementkeyword and \
                           self.__dctfmt["measurement"] and \
                           "keywords" in mdic and \
                           isinstance(mdic["keywords"], list) and \
                           self.__dctfmt["measurement"] \
                           not in mdic["keywords"]:
                            mdic["keywords"].append(
                                self.__dctfmt["measurement"])
                        if not self._metadataEqual(
                                dsmeta, mdic, skip=self.__withoutsm):
                            if self.__strategy in [
                                    UpdateStrategy.PATCH, UpdateStrategy.NO]:
                                nmeta = json.dumps(mdic)
                                # mm = dict(mdic)
                                # mm["scientificMetadata"] = {}
                                # get_logger().info(
                                #     'DatasetIngestor: PATCH: %s' % str(mm))
                                return self._patch_dataset(
                                    nmeta, pid, token, mdct)
                            else:
                                return self._post_dataset(mdic, token, mdct)
                        else:
                            if "scientificMetadata" in dsmeta.keys() and \
                               "scientificMetadata" in mdic.keys():
                                smmeta = dsmeta["scientificMetadata"]
                                smnmeta = mdic["scientificMetadata"]
                                nmeta = json.dumps(mdic)
                                if not self._metadataEqual(smmeta, smnmeta):
                                    if self.__strategy == \
                                       UpdateStrategy.CREATE:
                                        return self._post_dataset(
                                            mdic, token, mdct)
                                    else:
                                        return self._patch_dataset(
                                            nmeta, pid, token, mdct)
                    else:
                        raise Exception("%s" % resds.text)
                else:
                    return pid
            else:
                raise Exception("%s" % resexists.text)
        except Exception as e:
            get_logger().error(
                'DatasetIngestor: %s' % (str(e)))
        return None

    def _ingest_origdatablock(self, metadata, token):
        """ ingets origdatablock

        :param metadata: metadata in json string
        :type metadata: :obj:`str`
        :param token: ingestor token
        :type token: :obj:`str`
        :returns: rewquest startus
        :rtype: :obj:`bool`
        """
        try:
            self.__headers["Authorization"] = "Bearer {}".format(token)
            response = requests.post(
                self.__datablockurl,
                headers=self.__headers,
                params={"access_token": token},
                data=metadata)
            if response.ok:
                return True
            else:
                raise Exception("%s" % response.text)
        except Exception as e:
            get_logger().error(
                'DatasetIngestor: %s' % (str(e)))
        return False

    def _ingest_attachment(self, metadata, datasetid, token):
        """ ingets origdatablock

        :param metadata: metadata in json string
        :type metadata: :obj:`str`
        :param datasetid: dataset id
        :type datasetid: :obj:`str`
        :param token: ingestor token
        :type token: :obj:`str`
        :returns: rewquest startus
        :rtype: :obj:`bool`
        """
        try:
            dsid = datasetid.replace("/", "%2F")
            url = self.__attachmenturl
            # get_logger().debug(
            #     'DatasetIngestor: ingest attachment %s' % (
            #         url.format(pid=dsid, token=token)))
            self.__headers["Authorization"] = "Bearer {}".format(token)
            response = requests.post(
                url.format(pid=dsid),
                headers=self.__headers,
                params={"access_token": token},
                data=metadata)
            if response.ok:
                return True
            else:
                raise Exception("%s" % response.text)
        except Exception as e:
            get_logger().error(
                'DatasetIngestor: %s' % (str(e)))
        return False

    def _get_origdatablocks(self, datasetid, token):
        """ get origdatablocks with datasetid

        :param datasetid: dataset id
        :type datasetid: :obj:`str`
        :param token: ingestor token
        :type token: :obj:`str`
        :returns: list of  origdatablocks
        :rtype: :obj:`str` <:obj:`str`>
        """
        try:
            self.__headers["Authorization"] = "Bearer {}".format(token)
            response = requests.get(
                self.__dataseturl + "/%s/%s" %
                (datasetid.replace("/", "%2F"), self.__scicat_datablocks),
                params={"access_token": token},
                headers=self.__headers)
            if response.ok:
                js = response.json()
                return js
        except Exception as e:
            get_logger().error(
                'DatasetIngestor: %s' % (str(e)))
        return None

    def _get_delete_origdatablock(self, did, token):
        """ ingets origdatablock

        :param did: origdatablock id
        :type did: :obj:`str`
        :param token: ingestor token
        :type token: :obj:`str`
        """
        try:
            self.__headers["Authorization"] = "Bearer {}".format(token)
            response = requests.delete(
                "{url}/{pid}"
                .format(
                    url=self.__datablockurl,
                    pid=did.replace("/", "%2F")),
                params={"access_token": token},
                headers=self.__headers,
            )
            if response.ok:
                return True
            else:
                raise Exception("%s" % response.text)
        except Exception as e:
            get_logger().error(
                'DatasetIngestor: %s' % (str(e)))
        return None

    def _get_attachments(self, datasetid, token):
        """ get attachments with datasetid

        :param datasetid: dataset id
        :type datasetid: :obj:`str`
        :param token: ingestor token
        :type token: :obj:`str`
        :returns: list of  attachments
        :rtype: :obj:`str` <:obj:`str`>
        """
        try:
            self.__headers["Authorization"] = "Bearer {}".format(token)
            response = requests.get(self.__attachmenturl.format(
                pid=datasetid.replace("/", "%2F")),
                params={"access_token": token},
                headers=self.__headers
            )
            if response.ok:
                js = response.json()
                return js
        except Exception as e:
            get_logger().error(
                'DatasetIngestor: %s' % (str(e)))
        return None

    def _get_delete_attachment(self, datasetid, aid, token):
        """ ingets attachment

        :param datasetid: dataset id
        :type datasetid: :obj:`str`
        :param aid: attachment id
        :type aid: :obj:`str`
        :param token: ingestor token
        :type token: :obj:`str`
        """
        try:
            self.__headers["Authorization"] = "Bearer {}".format(token)
            response = requests.delete(
                self.__attachmenturl.format(
                    pid=datasetid.replace("/", "%2F"))
                + "/{aid}".format(aid=aid.replace("/", "%2F")),
                params={"access_token": token},
                headers=self.__headers)
            if response.ok:
                return True
            else:
                raise Exception("%s" % response.text)
        except Exception as e:
            get_logger().error(
                'DatasetIngestor: %s' % (str(e)))
        return None

    def _get_pid(self, metafile):
        """ get pid from raw dataset metadata

        :param metafile: metadata file name
        :type metafile: :obj:`str`
        :returns: dataset pid
        :rtype: :obj:`str`
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
        :type metafile: :obj:`str`
        :param token: ingestor token
        :type token: :obj:`str`
        :returns: dataset id
        :rtype: :obj:`str`
        """
        try:
            with open(metafile) as fl:
                smt = fl.read()
                mt = json.loads(smt)
            spid = self.__idpattern.format(
                beamtimeId=self.__bid, proposalId=self.__dpid)
            if mt["type"] == "raw" and mt["proposalId"] != spid:
                raise Exception(
                    "Wrong SC proposalId %s for DESY beamtimeId %s in %s"
                    % (mt["proposalId"], self.__bid, metafile))
            if not mt['pid'] or \
               not mt["pid"].startswith("%s/" % (self.__bid)):
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

    def _delete_origdatablocks(self, pid, token):
        """ delete origdatablock with given dataset pid

        :param pid: dataset id
        :type pid: :obj:`str`
        :param token: ingestor token
        :type token: :obj:`str`
        :returns: dataset id
        :rtype: :obj:`str`
        """
        try:
            datasetid = "%s%s" % (self.__pidprefix, pid)
            odbs = self._get_origdatablocks(datasetid, token) or []
            for odb in odbs:
                if "id" in odb:
                    self._get_delete_origdatablock(odb["id"], token)
        except Exception as e:
            get_logger().error(
                'DatasetIngestor: %s' % (str(e)))
        return ""

    def _delete_attachments(self, pid, token):
        """ delete attachment with given dataset pid

        :param pid: dataset id
        :type pid: :obj:`str`
        :param token: ingestor token
        :type token: :obj:`str`
        :returns: dataset id
        :rtype: :obj:`str`
        """
        try:
            datasetid = "%s%s" % (self.__pidprefix, pid)
            # get_logger().info("DA %s %s" % (pid, datasetid))
            odbs = self._get_attachments(datasetid, token) or []
            # get_logger().info("DA2 %s %s" % (pid, odbs))
            for odb in odbs:
                if "id" in odb:
                    # get_logger().info("DA3 %s %s" % (odb["id"], odb))
                    self._get_delete_attachment(datasetid, odb["id"], token)
        except Exception as e:
            get_logger().error(
                'DatasetIngestor: %s' % (str(e)))
        return ""

    def _update_attachments(self, tads, pid, token):
        """ delete attachment with given dataset pid

        :param pid: dataset id
        :type pid: :obj:`str`
        :param token: ingestor token
        :type token: :obj:`str`
        :returns: dataset id
        :rtype: :obj:`str`
        """
        dastatus = ""
        try:
            datasetid = "%s%s" % (self.__pidprefix, pid)
            # get_logger().info("DA %s %s" % (pid, datasetid))
            odbs = self._get_attachments(datasetid, token) or []
            # get_logger().info("DA2 %s %s" % (pid, odbs))
            found = []
            for fads in tads:
                with open(fads) as fl:
                    smt = fl.read()
                    ads = json.loads(smt)
                if "thumbnail" in ads:
                    for odb in odbs:
                        if "thumbnail" in odb and \
                           odb["thumbnail"] == ads["thumbnail"]:
                            if "id" in odb:
                                found.append(odb["id"])
                            break
                    else:
                        dastatus = self._ingest_attachment_metadata(
                            fads, pid, token)
                        get_logger().info(
                            "DatasetIngestor: Ingest attachment: %s"
                            % (fads))
            for odb in odbs:
                if "id" in odb and odb["id"] not in found:
                    self._get_delete_attachment(datasetid, odb["id"], token)

        except Exception as e:
            get_logger().error(
                'DatasetIngestor: %s' % (str(e)))
        return dastatus

    def _ingest_origdatablock_metadata(self, metafile, pid, token):
        """ ingest origdatablock metadata

        :param metafile: metadata file name
        :type metafile: :obj:`str`
        :param pid: dataset id
        :type pid: :obj:`str`
        :returns: dataset id
        :rtype: :obj:`str`
        """
        try:
            with open(metafile) as fl:
                smt = fl.read()
                mt = json.loads(smt)
            if not pid or not pid.startswith(self.__bid):
                raise Exception(
                    "Wrong origdatablock datasetId %s for DESY beamtimeId "
                    "%s in  %s"
                    % (pid, self.__bid, metafile))
            if mt["datasetId"] != "%s%s" % (self.__pidprefix, pid):
                mt["datasetId"] = "%s%s" % (self.__pidprefix, pid)
                smt = json.dumps(mt)
                with open(metafile, "w") as mf:
                    mf.write(smt)
            status = time.time()
            if "dataFileList" in mt and mt["dataFileList"]:
                status = self._ingest_origdatablock(smt, token)
            if status:
                return mt["datasetId"]
        except Exception as e:
            get_logger().error(
                'DatasetIngestor: %s' % (str(e)))
        return ""

    def _ingest_attachment_metadata(self, metafile, pid, token):
        """ ingest attachment metadata

        :param metafile: metadata file name
        :type metafile: :obj:`str`
        :param pid: dataset id
        :type pid: :obj:`str`
        :returns: dataset id
        :rtype: :obj:`str`
        """
        try:
            with open(metafile) as fl:
                smt = fl.read()
                mt = json.loads(smt)
            if "datasetId" in mt:
                if not pid or not pid.startswith(self.__bid):
                    raise Exception(
                        "Wrong attachment datasetId %s for DESY beamtimeId "
                        "%s in  %s"
                        % (pid, self.__bid, metafile))
                if mt["datasetId"] != "%s%s" % (self.__pidprefix, pid):
                    mt["datasetId"] = "%s%s" % (self.__pidprefix, pid)
                    smt = json.dumps(mt)
                    with open(metafile, "w") as mf:
                        mf.write(smt)
            else:
                mt["datasetId"] = "%s%s" % (self.__pidprefix, pid)
                smt = json.dumps(mt)
                with open(metafile, "w") as mf:
                    mf.write(smt)
            dsid = "%s%s" % (self.__pidprefix, pid)
            status = self._ingest_attachment(smt, dsid, token)
            if status:
                return dsid
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

        sscan = scan.split(" ")
        self.__dctfmt["entryname"] = ""
        self.__dctfmt["scanname"] = sscan[0] if len(sscan) > 0 else ""
        self.__dctfmt["masterscanname"] = self.__dctfmt["scanname"]
        sndir, snname = os.path.split(str(self.__dctfmt["scanname"]))
        plist = []
        self.__dctfmt["dbrelpath"] = ""
        if self.__relpath_in_datablock:
            plist.append(self.__dctfmt["relpath"])
        if sndir:
            plist.append(sndir)
        if plist:
            self.__dctfmt["dbrelpath"] = os.path.join(*plist)
        rdss = glob.glob(
            "{metapath}/{scan}{postfix}".format(
                scan=self.__dctfmt["scanname"],
                postfix=self.__scanpostfix,
                metapath=self.__dctfmt["metapath"]))
        if rdss and rdss[0]:
            rds = rdss[0]
        elif self.__forcegeneratemeasurement or \
                self.__dctfmt["scanname"] not in self.__measurements:
            rds = self._generate_rawdataset_metadata(self.__dctfmt["scanname"])
        else:
            rds = []
        mtmds = 0
        ads = None
        if rds:
            mtmds = os.path.getmtime(rds)

        odbs = glob.glob(
            "{metapath}/{scan}{postfix}".format(
                scan=self.__dctfmt["scanname"],
                postfix=self.__datablockpostfix,
                metapath=self.__dctfmt["metapath"]))
        if odbs and odbs[0] and not self.__single_datablock:
            odb = odbs[0]
            todb = [odb]
            with open(odb) as fl:
                dbmt = json.loads(fl.read())
                if isinstance(dbmt, list):
                    if self.__skip_multi_datablock:
                        todb = []
                    else:
                        todb = dbmt
        else:
            odb = self._generate_origdatablock_metadata(scan)
            todb = [odb]
        mtmdb = 0
        if odb:
            mtmdb = os.path.getmtime(odb)
        mtmda = 0
        if self.__ingest_attachment:
            adss = glob.glob(
                "{metapath}/{scan}{postfix}".format(
                    scan=self.__dctfmt["scanname"],
                    postfix=self.__attachmentpostfix,
                    metapath=self.__dctfmt["metapath"]))
            if adss and adss[0]:
                ads = adss[0]
                tads = [ads]
                with open(ads) as fl:
                    admt = json.loads(fl.read())
                    if isinstance(admt, list):
                        if self.__skip_multi_attachment:
                            tads = []
                        else:
                            tads = admt
            else:
                ads = self._generate_attachment_metadata(
                    self.__dctfmt["scanname"])
                tads = [ads]
            if ads:
                mtmda = os.path.getmtime(ads)

        if (self.__callcallback or self.__measurement_status) \
           and self.__metadatageneratedcallback \
           and rds and odb:
            command = self.__metadatageneratedcallback.format(**self.__dctfmt)
            get_logger().info(
                'DatasetIngestor: Metadata generated callback: %s ' % (
                    command))
            subprocess.run(command, shell=True, check=True)
        dbstatus = None
        dastatus = None
        pid = None
        if rds and odb and not self.__skip_scan_dataset_ingestion:
            if rds and rds[0]:
                pid = self._ingest_rawdataset_metadata(rds, token)
            if todb and todb[0] and pid:
                if pid is None and rdss and rdss[0]:
                    pid = self._get_pid(rdss[0])
                for odb in todb:
                    dbstatus = self._ingest_origdatablock_metadata(
                        odb, pid, token)
                    if not dbstatus:
                        mtmdb = -1
            if pid is None and rdss and rdss[0]:
                pid = self._get_pid(rdss[0])
            if self.__ingest_attachment and tads and tads[0] and pid:
                if pid is None and rdss and rdss[0]:
                    pid = self._get_pid(rdss[0])
                for ads in tads:
                    dastatus = self._ingest_attachment_metadata(
                        ads, pid, token)
                    if not dastatus:
                        mtmda = -1
        if pid is None:
            if scan in self.__sc_seingested_map.keys():
                mtmds = self.__sc_seingested_map[scan][-3]
            else:
                mtmds = 0
        if dbstatus is None:
            if scan in self.__sc_seingested_map.keys():
                mtmdb = self.__sc_seingested_map[scan][-2]
            else:
                mtmdb = 0
        if dastatus is None:
            if scan in self.__sc_seingested_map.keys():
                mtmda = self.__sc_seingested_map[scan][-2]
            else:
                mtmda = 0

        sscan.extend([str(mtmds), str(mtmdb), str(mtmda)])
        self.__sc_ingested.append(sscan)
        self.__sc_seingested_map[scan] = [mtmds, mtmdb, mtmda]
        with open(self.__idsfile, 'a+') as f:
            f.write("%s %s %s %s\n" % (scan, mtmds, mtmdb, mtmda))

    def reingest(self, scan, token, notmp=False):
        """ re-ingest scan

        :param scan: scan name
        :type scan: :obj:`str`
        :param token: access token
        :type token: :obj:`str`
        :param token: no tmp file flag
        :type token: :obj:`book`
        """
        get_logger().info(
            'DatasetIngestor: Checking: %s %s' % (
                self.__dsfile, scan))

        reingest_dataset = False
        reingest_origdatablock = False
        reingest_attachment = False
        sscan = scan.split(" ")
        pscan = scan

        self.__dctfmt["scanname"] = ""
        self.__dctfmt["masterscanname"] = ""
        self.__dctfmt["entryname"] = ""
        if len(sscan) > 0:
            if "::/" in sscan[0]:
                if ";" in sscan[0]:
                    pscan, scanname = sscan[0].split(";")[:2]
                else:
                    pscan = sscan[0]
                    scanname = sscan[0]
                if ":" in scanname:
                    scanname = scanname.split(":")[0]
                if "::/" in pscan:
                    gname, entryname = pscan.split("::/")[:2]
                else:
                    gname, entryname = sscan[0].split("::/")[:2]
                self.__dctfmt["scanname"] = scanname
                self.__dctfmt["masterscanname"] = gname
                self.__dctfmt["entryname"] = entryname
            elif ":" in sscan[0]:
                self.__dctfmt["scanname"] = sscan[0].split(":")[0]
                pscan = " ".join([self.__dctfmt["scanname"]] + sscan[1:])
                self.__dctfmt["masterscanname"] = self.__dctfmt["scanname"]
            else:
                self.__dctfmt["scanname"] = sscan[0]
                self.__dctfmt["masterscanname"] = self.__dctfmt["scanname"]
        sndir, snname = os.path.split(str(self.__dctfmt["scanname"]))
        plist = []
        self.__dctfmt["dbrelpath"] = ""
        if self.__relpath_in_datablock:
            plist.append(self.__dctfmt["relpath"])
        if sndir:
            plist.append(sndir)
        if plist:
            self.__dctfmt["dbrelpath"] = os.path.join(*plist)

        rds = None
        rdss = glob.glob(
            "{metapath}/{scan}{postfix}".format(
                scan=self.__dctfmt["scanname"],
                postfix=self.__scanpostfix,
                metapath=self.__dctfmt["metapath"]))
        if rdss and rdss[0]:
            rds = rdss[0]
            mtm = os.path.getmtime(rds)
            # print(self.__sc_ingested_map.keys())
            get_logger().debug("MAP: %s" % (self.__sc_ingested_map))

            if scan in self.__sc_ingested_map.keys():
                get_logger().debug("DS Timestamps: %s %s %s %s" % (
                    scan,
                    mtm, self.__sc_ingested_map[scan][-3],
                    mtm > self.__sc_ingested_map[scan][-3]))
            if scan not in self.__sc_ingested_map.keys() \
               or mtm > self.__sc_ingested_map[scan][-3]:
                if self.__strategy != UpdateStrategy.NO:
                    reingest_dataset = True
        elif self.__forcegeneratemeasurement or \
                self.__dctfmt["scanname"] not in self.__measurements:
            rds = self._generate_rawdataset_metadata(
                self.__dctfmt["scanname"])
            get_logger().debug("DS No File: %s True" % (scan))
            reingest_dataset = True
        else:
            rds = []
        mtmds = 0
        if rds:
            mtmds = os.path.getmtime(rds)

        odbs = glob.glob(
            "{metapath}/{scan}{postfix}".format(
                scan=self.__dctfmt["scanname"],
                postfix=self.__datablockpostfix,
                metapath=self.__dctfmt["metapath"]))
        if odbs and odbs[0] and not self.__single_datablock:
            odb = odbs[0]
            todb = [odb]
            olst = False
            with open(odb) as fl:
                dbmt = json.loads(fl.read())
                if isinstance(dbmt, list):
                    olst = True
                    if self.__skip_multi_datablock:
                        todb = []
                    else:
                        todb = dbmt

            mtm0 = os.path.getmtime(odb)
            if scan not in self.__sc_ingested_map.keys() \
               or mtm0 > self.__sc_ingested_map[scan][-2]:
                reingest_origdatablock = True
            if scan in self.__sc_ingested_map.keys():
                get_logger().debug("DB0 Timestamps: %s %s %s %s %s" % (
                    scan,
                    mtm0, self.__sc_ingested_map[scan][-2],
                    mtm0 - self.__sc_ingested_map[scan][-2],
                    reingest_origdatablock)
                )
            if not olst:
                self._regenerate_origdatablock_metadata(
                    pscan, reingest_origdatablock)

            mtm = os.path.getmtime(odb)

            if scan in self.__sc_ingested_map.keys():
                get_logger().debug("DB Timestamps: %s %s %s %s" % (
                    scan,
                    mtm, self.__sc_ingested_map[scan][-2],
                    mtm > self.__sc_ingested_map[scan][-2]))

            if scan not in self.__sc_ingested_map.keys() \
               or mtm > self.__sc_ingested_map[scan][-2]:
                reingest_origdatablock = True
        else:
            odb = self._generate_origdatablock_metadata(pscan)
            todb = [odb]
            get_logger().debug("DB No File: %s True" % (scan))
            reingest_origdatablock = True

        mfilename = ""
        odb2 = ""
        if self.__dctfmt["masterscanname"] != self.__dctfmt["scanname"]:
            mfilename = "{metapath}/_{masterscanname}{datablockpostfix}". \
                format(**self.__dctfmt)
            odb2 = self._regenerate_origdatablock_metadata(
                self.__dctfmt["masterscanname"], True, mfilename=mfilename)
            if odb2 and os.path.isfile(odb2) and odb2 not in todb:
                todb.insert(0, odb2)
                reingest_origdatablock = True
        mtmdb = 0
        if odb:
            mtmdb = os.path.getmtime(odb)

        if (self.__callcallback or self.__measurement_status) \
           and self.__metadatageneratedcallback \
           and rds and odb:
            command = self.__metadatageneratedcallback.format(**self.__dctfmt)
            get_logger().info(
                'DatasetIngestor: Metadata generated callback: %s ' % (
                    command))
            subprocess.run(command, shell=True, check=True)
        dastatus = None
        dbstatus = None
        ads = None
        tads = []
        if self.__ingest_attachment:
            adss = glob.glob(
                "{metapath}/{scan}{postfix}".format(
                    scan=self.__dctfmt["scanname"],
                    postfix=self.__attachmentpostfix,
                    metapath=self.__dctfmt["metapath"]))
            if adss and adss[0]:
                ads = adss[0]
                tads = [ads]
                with open(ads) as fl:
                    admt = json.loads(fl.read())
                    if isinstance(admt, list):
                        if self.__skip_multi_attachment:
                            tads = []
                        else:
                            tads = admt
                mtm0 = os.path.getmtime(ads)
                if scan in self.__sc_ingested_map.keys():
                    get_logger().debug(
                        "ATTRIBUTE REINGEST check: %s ?? %s"
                        % (mtm0, self.__sc_ingested_map[scan][-1]))
                if scan not in self.__sc_ingested_map.keys() \
                   or mtm0 > self.__sc_ingested_map[scan][-1]:
                    reingest_attachment = True
            else:
                ads = self._generate_attachment_metadata(
                    self.__dctfmt["scanname"])
                reingest_attachment = True
                tads = [ads]
            if ads:
                mtm0 = os.path.getmtime(ads)

        pid = None
        if (rds and odb) or ads:
            if rds and reingest_dataset:
                pid = self._ingest_rawdataset_metadata(rds, token)
                get_logger().info(
                    "DatasetIngestor: Ingest dataset: %s" % (rds))
                oldpid = self._get_pid(rds)
                if pid and oldpid != pid:
                    if not olst:
                        odb = self._generate_origdatablock_metadata(scan)
                    reingest_origdatablock = True
            if todb and todb[0] and reingest_origdatablock:
                if pid is None and rdss and rdss[0]:
                    pid = self._get_pid(rdss[0])
                self._delete_origdatablocks(pid, token)
                for odb in todb:
                    dbstatus = self._ingest_origdatablock_metadata(
                        odb, pid, token)
                    get_logger().info(
                        "DatasetIngestor: Ingest origdatablock: %s" % (odb))
                if not dbstatus:
                    mtmdb = -1

            get_logger().debug("Ingest Attachment %s %s %s" % (
                self.__ingest_attachment, tads, reingest_attachment))
            if self.__ingest_attachment:
                if tads and tads[0] and reingest_attachment:
                    if pid is None and rdss and rdss[0]:
                        pid = self._get_pid(rdss[0])
                    if not pid:
                        get_logger().error(
                            "DatasetIngestor: No dataset pid "
                            "for the attachment found: %s" % (ads))
                    else:
                        get_logger().debug("Attachment PID  %s %s"
                                           % (tads, pid))
                        if self.__strategy in [UpdateStrategy.PATCH,
                                               UpdateStrategy.MIXED]:
                            dastatus = self._update_attachments(
                                tads, pid, token)
                        else:
                            self._delete_attachments(pid, token)
                            for ads in tads:
                                dastatus = self._ingest_attachment_metadata(
                                    ads, pid, token)
                                get_logger().info(
                                    "DatasetIngestor: Ingest attachment: %s"
                                    % (ads))
                        if not dastatus:
                            mtmda = -1
        mtmda = 0
        if ads:
            mtmda = os.path.getmtime(ads)

        if (pid and reingest_dataset):
            pass
        elif scan in self.__sc_ingested_map.keys():
            mtmds = self.__sc_ingested_map[scan][-3]
        else:
            mtmds = 0
        if (dbstatus and reingest_origdatablock):
            pass
        elif scan in self.__sc_ingested_map.keys():
            mtmdb = self.__sc_ingested_map[scan][-2]
        else:
            mtmdb = 0

        if (dastatus and reingest_attachment):
            pass
        elif scan in self.__sc_ingested_map.keys():
            mtmda = self.__sc_ingested_map[scan][-1]
        else:
            mtmda = 0

        sscan.extend([str(mtmds), str(mtmdb), str(mtmda)])
        self.__sc_ingested.append(sscan)
        self.__sc_seingested_map[scan] = [mtmds, mtmdb, mtmda]
        lfile = self.__idsfiletmp
        if notmp:
            lfile = self.__idsfile
        with open(lfile, 'a+') as f:
            f.write("%s %s %s %s\n" % (scan, mtmds, mtmdb, mtmda))

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
                for sc in self.__sc_ingested:
                    try:
                        if len(sc) > 3:
                            self.__sc_seingested_map[" ".join(sc[:-3])] = \
                                                     [float(sc[-3]),
                                                      float(sc[-2]),
                                                      float(sc[-1])]
                    except Exception as e:
                        get_logger().debug("%s" % str(e))
        if not reingest:
            if self.__retry_failed_dataset_ingestion:
                check_attach = self.__retry_failed_attachment_ingestion \
                    and self.__ingest_attachment
                ingested = []
                for sc in self.__sc_ingested:
                    if len(sc) > 3:
                        try:
                            if float(sc[-1]) != -1 \
                               and (not check_attach or float(sc[-1]) > 0) \
                               and float(sc[-2]) > 0 and float(sc[-3]) > 0:
                                ingested.append(" ".join(sc[:-3]))
                        except Exception as e:
                            get_logger().debug("%s" % str(e))
                    else:
                        ingested.append(sc[0])
            else:
                ingested = [(" ".join(sc[:-3]) if len(sc) > 3 else sc[0])
                            for sc in self.__sc_ingested]
            self.__sc_ingested_map = {}
            for sc in self.__sc_ingested:
                try:
                    if len(sc) > 3 and float(sc[-1]) >= 0 \
                       and float(sc[-2]) > 0 and float(sc[-3]) > 0:
                        sc[-1] = float(sc[-1])
                        sc[-2] = float(sc[-2])
                        sc[-3] = float(sc[-3])
                        self.__sc_ingested_map[" ".join(sc[:-3])] = sc
                except Exception as e:
                    get_logger().debug("%s" % str(e))
            self.__sc_waiting = [
                sc for sc in scans if sc not in ingested]
        else:
            self.__sc_waiting = [sc for sc in scans]
            self.__sc_ingested_map = {}
            for sc in self.__sc_ingested:
                try:
                    if len(sc) > 3 and float(sc[-1]) >= 0 \
                       and float(sc[-2]) > 0 and float(sc[-3]) > 0:
                        sc[-1] = float(sc[-1])
                        sc[-2] = float(sc[-2])
                        sc[-3] = float(sc[-3])
                        self.__sc_ingested_map[" ".join(sc[:-3])] = sc
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
        self.__measurements = set()

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

    def stop_measurement(self):
        """ stop measurement

        """
        self.__measurement_status = False
        self.__dctfmt["measurement"] = ""
        get_logger().debug("Stop Measurement: %s" % self.__measurement)

    def start_measurement(self, measurement):
        """ start measurement

        :param measurement:  measurement name
        :type measurement: :obj:`str`

        """
        self.__measurement = measurement
        self.__dctfmt["measurement"] = self.__measurement
        self.__dctfmt["lastmeasurement"] = self.__measurement
        self.__measurements.add(self.__measurement)
        self.__measurement_status = True
        get_logger().debug("Start Measurement: %s" % self.__measurement)
