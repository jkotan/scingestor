# Scicat Dataset Ingestor

![github workflow](https://github.com/jkotan/scingestor/actions/workflows/tests.yml/badge.svg)

The `scingestor` python package provides a support for ingestor scripts for ingesting RawDatasets and OrigDatablocks into the SciCat server.

## scicat_dataset_ingestor

BeamtimeWatcher service SciCat Dataset ingestor. It can be executed by

```
    scicat_dataset_ingestor -c ~/.scingestor.yaml
```
Its configuration written in a YAML file  can contain the following variables `scicat_url`, `ingestor_credential_file`, `beamtime_dirs` or  `beamtime_base_dir` e.g.
```
beamtime_dirs:
  - /home/jkotan/gpfs/current
  - /home/jkotan/gpfs/commissioning
scicat_url: http://localhost:8881
ingestor_credential_file: /home/jkotan/gpfs/pwd
```

## scicat_dataset_ingest

Re-ingestion script for SciCat RawDatasets.

    scicat_dataset_ingestor -c ~/.scingestor.yaml

Its configuration written in a YAML file as `scicat_dataset_ingestor`

## Installation

### Required packages

* python3 >= 3.7
* nxstools >= 3.23.0
* requests
* setuptools
* pyyaml
* pytest (to run tests)
* sphinx (to build the documentation)


### Install from sources

The code can be built with

```
    $ python3 setup.py install
```


To build the documentation use

```
    $ python3 setup.py build_sphinx
```

The resulting documentation can be found below `build/sphinx/html` in the root
directory of the source distribution.

Finally, the package can be tested using

```
    $ python3 -m pytest test
```


### Debian and Ubuntu packages

Debian  `bullseye`, `buster`  or Ubuntu  `jammy`, `focal` packages can be found in the HDRI repository.

To install the debian packages, add the PGP repository key

```
    $ sudo su
    $ curl -s http://repos.pni-hdri.de/debian_repo.pub.gpg  | gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/debian-hdri-repo.gpg --import
    $ chmod 644 /etc/apt/trusted.gpg.d/debian-hdri-repo.gpg
```

and then download the corresponding source list, e.g.
for `bullseye`

```
    $ cd /etc/apt/sources.list.d
    $ wget http://repos.pni-hdri.de/bullseye-pni-hdri.list
```

or `jammy`

```
    $ cd /etc/apt/sources.list.d
    $ wget http://repos.pni-hdri.de/jammy-pni-hdri.list
```
respectively.

Finally,

```
    $ apt-get update
    $ apt-get install python3-scingestor
```
