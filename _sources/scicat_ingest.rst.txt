=============
scicat_ingest
=============

Description
-----------

Re-ingestion script for SciCat RawDatasets.


Synopsis
--------

.. code:: bash

	  scicat_ingest [-h] [-c CONFIG] [-r RUNTIME] [-l LOG] [-f LOGFILE] [-t] [-p TOKENFILE]   metadata_json_file [metadata_json_file ...]


Arguments:
  metadata_json_file    metadata json file(s)

Options:
  -h, --help            show this help message and exit
  -c CONFIG, --configuration CONFIG
                        configuration file name
  -l LOG, --log LOG     logging level, i.e. debug, info, warning, error, critical
  -f LOGFILE, --log-file LOGFILE
                        log file name
  -t, --timestamps      timestamps in logs
  -p TOKENFILE, --token-file TOKENFILE
                        file with a user token


Example
-------

.. code:: bash

	  scicat_ingest -m Samples -c ~/.scingestor.yaml ./metadata.json

	  scicat_ingest -m Attachments -c ~/.scingestor.yaml -p ~/.mytoken.cfg ./metadata.json
	  
