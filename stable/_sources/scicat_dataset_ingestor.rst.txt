=======================
scicat_dataset_ingestor
=======================

Description
-----------

BeamtimeWatcher service SciCat Dataset ingestor.


Synopsis
--------

.. code:: bash

	  scicat_dataset_ingestor [-h] [-c CONFIG] [-r RUNTIME] [-l LOG] [-f LOGFILE] [-t]



Options:
  -h, --help            show this help message and exit
  -c CONFIG, --configuration CONFIG
                        configuration file name
  -r RUNTIME, --runtime RUNTIME
                        stop program after runtime in seconds
  -l LOG, --log LOG     logging level, i.e. debug, info, warning, error, critical
  -f LOGFILE, --log-file LOGFILE
                        log file name
  -t, --timestamps      timestamps in logs


Example
-------

.. code:: bash

	  scicat_dataset_ingestor -c ~/.scingestor.yaml

	  scicat_dataset_ingestor -c ~/.scingestor.yaml -l debug
	  
