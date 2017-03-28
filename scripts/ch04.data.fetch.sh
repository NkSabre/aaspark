#!/bin/sh

ROOT_DIR=`pwd`
# ROOT_DIR=~/workspace/aaspark
DATA_DIR=/yjh/
TMP_DIR=/tmp/aaspark-yjh

hadoop fs -mkdir -p $DATA_DIR/ch04

mkdir -p $TMP_DIR
cd $TMP_DIR/
curl -o covtype.data.gz  https://archive.ics.uci.edu/ml/machine-learning-databases/covtype/covtype.data.gz
gzip -d covtype.data.gz
#mv profiledata_06-May-2005/*.txt $DATA_DIR/ch03/
hadoop fs -put covtype.data $DATA_DIR/ch04/
rm $TMP_DIR -rf

