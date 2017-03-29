#!/bin/sh

ROOT_DIR=`pwd`
# ROOT_DIR=~/workspace/aaspark
DATA_DIR=/yjh/
TMP_DIR=/tmp/aaspark-yjh

hadoop fs -mkdir -p $DATA_DIR/ch05

mkdir -p $TMP_DIR
cd $TMP_DIR/
curl -o kddcup.data.gz http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data.gz
gzip -d kddcup.data.gz
#mv profiledata_06-May-2005/*.txt $DATA_DIR/ch03/
hadoop fs -put kddcup.data $DATA_DIR/ch05/
rm $TMP_DIR -rf

