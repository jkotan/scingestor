=====================
scicat_dataset_ingest
=====================

Description
-----------

Re-ingestion script for SciCat Datasets.


Synopsis
--------

.. code:: bash

	  scicat_dataset_ingest [-h] [-c CONFIG] [-r RUNTIME] [-l LOG] [-f LOGFILE] [-t]



Options:
  -h, --help            show this help message and exit
  -c CONFIG, --configuration CONFIG
                        configuration file name
  -l LOG, --log LOG     logging level, i.e. debug, info, warning, error, critical
  -f LOGFILE, --log-file LOGFILE
                        log file name
  -t, --timestamps      timestamps in logs


Example
-------

.. code:: bash

	  scicat_dataset_ingest -c ~/.scingestor.yaml

	  scicat_dataset_ingest -c ~/.scingestor.yaml -l debug
	  
