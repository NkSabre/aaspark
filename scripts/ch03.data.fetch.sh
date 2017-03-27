#!/bin/sh

ROOT_DIR=`pwd`
# ROOT_DIR=~/workspace/aaspark
DATA_DIR=/yjh/
TMP_DIR=/tmp/aaspark-yjh

hadoop fs -mkdir -p $DATA_DIR/ch03

mkdir -p $TMP_DIR
cd $TMP_DIR/
curl -o profiledata.tar.gz  http://www.iro.umontreal.ca/~lisa/datasets/profiledata_06-May-2005.tar.gz
tar zxvf profiledata.tar.gz
#mv profiledata_06-May-2005/*.txt $DATA_DIR/ch03/
hadoop fs -put profiledata_06-May-2005/*.txt $DATA_DIR/ch03/
rm $TMP_DIR -rf

