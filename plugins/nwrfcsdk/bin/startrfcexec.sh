#!/bin/bash
# replace rfcexec (or any other started server) executable in the destination SM59 in the ABAP backend
# with this script to enable the trace more easily, to set other environment variables or redirect output, etc....
# Here we are interested in the command line arguments passed by the gateway, the user and the process IDs.
OUTPUT_FILE="${PWD}/startrfcexec.txt"

for var in "$@"
  do
      echo "$var" >> $OUTPUT_FILE
  done

EXEC_PATH="${PWD}/rfcexec"

echo user=$USER pid=$$ ppid=$PPID >> $OUTPUT_FILE
echo $(date -u) $PWD "start rfcexec" >> $OUTPUT_FILE
exec $EXEC_PATH "${@}" -f ./rfcexec.sec -t RFC_TRACE=4 CPIC_TRACE=3 >> $OUTPUT_FILE
echo $(date -u) $PWD "end rfcexec" >> $OUTPUT_FILE