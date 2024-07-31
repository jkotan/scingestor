# Scicat Dataset Ingestor

![github workflow](https://github.com/jkotan/scingestor/actions/workflows/tests.yml/badge.svg) [![docs](https://img.shields.io/badge/Documentation-webpages-ADD8E6.svg)](https://jkotan.github.io/scingestor/index.html) [![Pypi Version](https://img.shields.io/pypi/v/scingestor.svg)](https://pypi.python.org/pypi/scingestor) [![Python Versions](https://img.shields.io/pypi/pyversions/scingestor.svg)](https://pypi.python.org/pypi/scingestor/)

The `scingestor` python package provides a support for scripts which ingest Datasets and OrigDatablocks into the SciCat metadata server.

## scicat_dataset_ingestor
SciCat Dataset ingestor server ingests scan metadata just after a scan is finished. It can be executed by

```
scicat_dataset_ingestor -c ~/.scingestor.yaml
```
### Configuration variables
The configuration written in YAML can contain the following variables
* **scicat_url** *(str)* , default: `"http://localhost:3000/api/v3"`
* **ingestor_credential_file** *(str)* , default: `None`
* **beamtime_dirs** *(list\<str\>)* , default: `[]`
* **beamtime_base_dir** *(str)* , default: `""`
* **ingestor_var_dir** *(str)* , default: `""`
* **ingestor_username** *(str)* , default: `"ingestor"`
* **dataset_pid_prefix** *(str)* , default: `""`
* **dataset_update_strategy** (`"no"`, `"patch"`, `"create"`, `"mixed"`) , default: `"patch"`
* **relative_path_in_datablock** *(bool)* , default: `False`
* **scandir_blacklist** *(list\<str\>)* , default: `["/gpfs/current/scratch_bl", "/gpfs/current/processed", "/gpfs/current/shared"]`
* **beamtimeid_blacklist_file** *(str)* , default: `None`
* **beamtime_type_blacklist** *(list\<str\>)* , default: `["P"]`
* **chmod_json_files** *(str)* , default: `None`
* **max_scandir_depth** *(int)*, default: `-1`
* **oned_in_metadata** *(bool)* , default: `False`
* **force_measurement_keyword** *(bool)* , default: `True`
* **force_generate_measurement** *(bool)* , default: `False`
* **max_oned_size** *(int)* , default: `None`
* **scan_metadata_postfix** *(str)* , default: `".scan.json"`
* **datablock_metadata_postfix** *(str)* , default: `".origdatablock.json"`
* **attachment_metadata_postfix** *(str)* , default: `".attachment.json"`
* **metadata_in_var_dir** *(bool)* , default: `True`
* **use_corepath_as_scandir** *(bool)* , default: `False`
* **watch_scandir_subdir** *(bool)* , default: `False`
* **beamtime_filename_postfix** *(str)* , default: `"beamtime-metadata-"`
* **beamtime_filename_prefix** *(str)* , default: `".json"`
* **scicat_proposal_id_pattern** *(str)* , default: `"{proposalid}.{beamtimeid}"`
* **datasets_filename_pattern** *(str)* , default: `"scicat-datasets-{beamtimeid}.lst"`
* **ingested_datasets_filename_pattern** *(str)* , default: `"scicat-ingested-datasets-{beamtimeid}.lst"`
* **file_dataset_metadata_generator** *(str)* , default: `"nxsfileinfo metadata -k4  -o {metapath}/{scanname}{scanpostfix} --id-format {idpattern} -b {beamtimefile} -p {beamtimeid}/{scanname}  -w {ownergroup} -c {accessgroups} {mastefile}"`
* **dataset_metadata_generator** *(str)* , default: `"nxsfileinfo metadata -k4  -o {metapath}/{scanname}{scanpostfix}   --id-format {idpattern} -c {accessgroups} -w {ownergroup} -b {beamtimefile} -p {beamtimeid}/{scanname}"`
* **datablock_metadata_generator** *(str)* , default: `"nxsfileinfo origdatablock  -s *.pyc,*{datablockpostfix},*{scanpostfix},*~  -p {pidprefix}{beamtimeid}/{scanname}  -w {ownergroup} -c {accessgroups} -o {metapath}/{scanname}{datablockpostfix} "`
* **datablock_metadata_stream_generator** *(str)* , default: `"nxsfileinfo origdatablock  -s *.pyc,*{datablockpostfix},*{scanpostfix},*~  -w {ownergroup} -c {accessgroups} -p {pidprefix}{beamtimeid}/{scanname} "`
* **datablock_metadata_generator_scanpath_postfix** *(str)* , default: `" {scanpath}/{scanname} "`
* **attachment_metadata_generator** *(str)* , default: `"nxsfileinfo attachment  -w {ownergroup} -c {accessgroups} -o {metapath}/{scanname}{attachmentpostfix} {plotfile} "`
* **metadata_generated_callback** *(str)* , default: `"nxsfileinfo groupmetadata  {lastmeasurement} -m {metapath}/{scanname}{scanpostfix} -d {metapath}/{scanname}{datablockpostfix} -a {metapath}/{scanname}{attachmentpostfix} -p {beamtimeid}/{lastmeasurement} -f -k4 "`
* **metadata_group_map_file** *(str)* , default: `""`
* **raw_metadata_callback** *(bool)* , default: `False`
* **single_datablock_ingestion** *(bool)* , default: `False`
* **skip_multi_datablock_ingestion** *(bool)* , default: `False`
* **skip_multi_attachment_ingestion** *(bool)* , default: `False`
* **skip_scan_dataset_ingestion** *(bool)* , default: `False`
* **call_metadata_generated_callback** *(bool)* , default: `False`
* **metadata_group_map_file_generator_switch** *(str)* , default: `" --group-map-file {groupmapfile} "`
* **raw_metadata_callback_switch** *(str)* , default: `" --raw "`
* **execute_commands** *(bool)* , default: `True`
* **plot_file_extension_list** *(list\<str\>)* , default: `["png", "nxs", "h5", "ndf", "nx", "fio"]`
* **master_file_extension_list** *(list\<str\>)* , default: `["nxs", "h5", "ndf", "nx", "fio"]`
* **chmod_generator_switch** *(str)* , default: `" -x {chmod} "`
* **relative_path_generator_switch** *(str)* , default: `" -r {relpath} "`
* **oned_dataset_generator_switch** *(str)* , default: `" --oned "`
* **max_oned_dataset_generator_switch** *(str)* , default: `" --max-oned-size {maxonedsize} "`
* **override_attachment_signals_generator_switch** *(bool)* , default: `" --override "`
* **hidden_attributes_generator_switch** *(str)* , default: `" -n {hiddenattributes} "`
* **hidden_attributes** *(str)* , default: `"nexdatas_source,nexdatas_strategy,units"`
* **attachment_signals_generator_switch** *(str)* , default: `" -s {signals} "`
* **attachment_axes_generator_switch** *(str)* , default: `" -e {axes} "`
* **attachment_frame_generator_switch** *(str)* , default: `" -m {frame} "`
* **attachment_signal_names** *(str)* , default: `""`
* **attachment_axes_names** *(str)* , default: `""`
* **attachment_image_frame_number** *(str)* , default: `""`
* **ingest_dataset_attachment** *(bool)* , default: `True`
* **override_attachment_signals** *(bool)* , default:`False`
* **retry_failed_dataset_ingestion** *(bool)* , default:`True`
* **retry_failed_attachement_ingestion** *(bool)* , default:`False`
* **log_generator_commands** *(bool)* , default: `False`
* **add_empty_units_generator_switch** *(str)* , default: `" --add-empty-units "`
* **add_empty_units** *(bool)* , default: `True`
* **metadata_copy_map_file** *(str)* , default: `None`
* **metadata_copy_map_file_generator_switch** *(str)* , default: `" --copy-map-file {copymapfile} "`
* **inotify_timeout** *(float)* , default: `1.0`
* **get_event_timeout** *(float)* , default: `0.1`
* **ingestion_delay_time** *(float)* , default: `5.0`
* **max_request_tries_number** *(int)* , default: `100`
* **recheck_dataset_list_interval** *(int)* , default: `1000`
* **recheck_beamtime_file_interval** *(int)* , default: `1000`
* **request_headers** *(dict\<str,str\>)* , default: `{"Content-Type": "application/json", "Accept": "application/json"}`
* **scicat_datasets_path** *(str)* , default: `"Datasets"`
* **scicat_proposals_path** *(str)* , default: `"Proposals"`
* **scicat_datablocks_path** *(str)*, default: `"OrigDatablocks"`
* **scicat_attachments_path** *(str)*, default: `"Datasets/{pid}/Attachments"`
* **scicat_users_login_path** *(str)*, default: `"Users/login"`
* **owner_access_groups_from_proposal** *(bool)*, default: `False`
* **metadata_fields_without_checks** *(list\<str\>)*, default: `["techniques", "classification", "createdBy", "updatedBy", "datasetlifecycle", "numberOfFiles", "size", "createdAt", "updatedAt", "history", "creationTime", "version", "scientificMetadata", "endTime"]`

e.g.
```
beamtime_dirs:
  - "{homepath}/gpfs/current"
  - "{homepath}/gpfs/commissioning"
scicat_url: http://localhost:3000/api/v3
ingestor_credential_file: "{homepath}/gpfs/pwd"
```

### Pattern keywords for configuration variables

The  **datasets_filename_pattern**, **ingested_datasets_filename_pattern**  and **ingestor_var_dir** can contain the *{beamtimeid}* and *{hostname}* keywords,  e.g. `"scicat-ingested-datasets-{beamtimeid}.lst"` or `"scicat-ingested-datasets-{hostname}-{beamtimeid}.lst"` which is instantiated during the ingestor execution.

The  **beamtime_dirs**, **beamtime_base_dir**, **ingestor_var_dir**, **ingestor_credential_file**, **scandir_blacklist** can contain the *{homepath}* keyword.

Similarly, **file_dataset_metadata_generator**, **dataset_metadata_generator**, **datablock_metadata_generator**,  **datablock_metadata_stream_generator**, **datablock_metadata_generator_scanpath_postfix**, **attachment_metadata_generator**, **chmod_generator_switch**, **relative_path_generator_switch** can contain the following keywords: *{beamtimeid}* , *{scanname}*, *{chmod}*, *{scanpath}*, *{metapath}*, *{relpath}*, *{beamtimeid}*, *{beamline}*, *{pidprefix}*, *{beamtimefile}*, *{scanpostfix}*, *{datablockpostfix}*, *{ownergroup}*, *{accessgroups}*, *{hostname}*, *{homepath}*, *{hiddenattributes}*, *{ext}*, "{masterfile}", "{plotfile}", "{masterscanname}", "{entryname}"

The "{masterfile}" is either equal to   "{scanpath}/{scanname}.{ext}" or "{scanpath}/{scanname}/{scanname}.{ext}". Also
the "{plotfile}" is either equal to  "{scanpath}/{scanname}.{plotext}" or "{scanpath}/{scanname}/{scanname}.{plotext}".


## scicat_dataset_ingest

Re-ingestion script for SciCat Datasets and OrigDatablocks is usually launched at the end of the beamtime.
```
scicat_dataset_ingest -c ~/.scingestor.yaml
```
Its configuration written YAML like for `scicat_dataset_ingestor`


## scicat_ingest

General ingestion script for SciCat Models could be used for manual scicat model ingestion, e.g. Sample, Instrument or DerivedDataset.
```
scicat_ingest  -m Samples  -c ~/.scingestor.yaml  ./metadata.json
```
Its configuration written YAML like for `scicat_dataset_ingestor`

## Installation

### Required packages

* python3 >= 3.7
* nxstools >= 3.38.0
* inotifyx (python3 version)
* requests
* setuptools
* pyyaml
* pytest (to run tests)
* sphinx (to build the documentation)


### Install from sources

The code from https://github.com/jkotan/scingestor can be built with

```
python3 setup.py install
```


To build the documentation use

```
python3 setup.py build_sphinx
```

The resulting documentation can be found below `build/sphinx/html` in the root
directory of the source distribution.

Finally, the package can be tested using

```
python3 -m pytest test
```

### Install in conda or pip environment

The code can be installed in your conda environment by
```
conda create -n myenv python=3.9
conda activate myenv

pip install inotifyx-py3
pip install scingestor
```

or in your pip environment by
```
python3 -m venv myvenv
. myvenv/bin/activate

pip install inotifyx-py3
pip install scingestor
```


### Debian and Ubuntu packages

Debian  `bookworm`, `bullseye`, `buster`  or Ubuntu  `lunar`, `jammy`, `focal` packages can be found in the HDRI repository.

To install the debian packages, add the PGP repository key

```
sudo su
curl -s http://repos.pni-hdri.de/debian_repo.pub.gpg  | gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/debian-hdri-repo.gpg --import
chmod 644 /etc/apt/trusted.gpg.d/debian-hdri-repo.gpg
```

and then download the corresponding source list, e.g.
for `bookworm`

```
cd /etc/apt/sources.list.d
wget http://repos.pni-hdri.de/bookworm-pni-hdri.list
```

or `jammy`

```
cd /etc/apt/sources.list.d
wget http://repos.pni-hdri.de/jammy-pni-hdri.list
```
respectively.

Finally,

```
apt-get update
apt-get install python3-scingestor
```

## Dataset list file content

The scicat ingestor triggers its actions on append a new line in the dataset list file.
The dataset list file is located in the scan directory and its filename is defined
by **datasets_filename_pattern** variable, i.e. by default "scicat-datasets-{beamtimeid}.lst".

By default the scan dataset metadata are fetched from the corresponding the master file with its filename given by \<scanname\>.\<ext\> where usually \<ext\> is `nxs` or `fio`. The detector files related to the particular scan are placed in the \<scanname\> subdirectory and they are added to the scan origindatablock.

A separete line in the dataset list file may contain
* scanname to ingest e.g.  `myscan_00012`
* scanname to re-ingest with a unique identifier (timestamp), e.g. `myscan_00012:1702988846.0770347`
* scanname and additional detector subdirectories to ingest, e.g.  `myscan_00012 pilatus1 lambda`
* string with a  base master filename, a NXentry NeXus path and a scanname representing scan metadata from the multi-scan nexus file to ingest, e.g.  `myscan::/scan12;myscan_00012`
* command to start a measurement with a given name which groups related scans,  e.g. `__command__ start mycalib6`
* command to stop a measurement which groups related scans,  e.g. `__command__ stop`


## Measurment Datasets which group scan metadata

The `__command__ start \<measurement\>` and `__command__ stop` allow to pass information to scicat ingestor which scan datasets should be grouped into the measurement dataset, i.e. by default of scan datasets between start and stop commands are grouped to the one measurement.

### Sardana Measurement macros

The config/scmacros.py module provides sardana macros which help to start/stop the measurement
* **start_measurement \<measurement\>** starts a new measurment with the given name
* **make_measurement \<measurement\>** starts a new measurment with the given name and adds to the measurement the last scan
* **update_measurement** updates the current measurement dataset in the SciCat database
* **stop_measurement** updates the current measurement dataset in the SciCat database and stops the  measurement
* **show_current_measurement** shows the current measurement name

### Sardana Measurement with SciCatAutoGrouping

Setting the **SciCatAutoGrouping** sardana environment variable to `True` we can switch on the autogrouping mode. In this mode scan metadata is grouped automatically into the measurement dataset and the measurement dataset updated after each scan. The name of measurement is taken from the base scanname after removing ScanID, e.g. for `<scanname>` = "mycalib2_00012" the measurement name is "mycalib2"
