#!/usr/bin/env bash

# read parameters from the command-line
if [[ -z $2 ]] ; then
    echo "usage: format_default.sh  <INPUT_FILE>  <OUTPUT_FILE> [ <SCICAT_URL> <BEAMTIME_DIR> <INGESTOR_VAR_DIR>]"
    exit 1
else
    # template_default.yaml
    export INFILE="$1"
    # default.yaml
    export OUTFILE="$2"
fi

if [[ ! -z $3 ]]; then
    export SCICAT_URL="$3"
fi
if [[ ! -z $4 ]]; then
    export BEAMTIME_DIR="$4"
fi
if [[ ! -z $5 ]]; then
    export INGESTOR_VAR_DIR="$5"
fi

# default parameters
if [[ -z $SCICAT_URL ]]; then
    export SCICAT_URL="http://scicat.desy.de/api/v3"
fi
if [[ -z $BEAMTIME_DIR ]]; then
    export BEAMTIME_DIR="/gpfs/current"
fi
if [[ -z $INGESTOR_VAR_DIR ]]; then
    export INGESTOR_VAR_DIR="/gpfs/current/scratch_bl/scingestor"
fi

rm -f ${OUTFILE} ${OUTFILE}.tmp

( echo "cat <<EOF >${OUTFILE}";
  cat ${INFILE};
  echo "EOF";
) >${OUTFILE}.tmp
. ${OUTFILE}.tmp

rm -f ${OUTFILE}.tmp
