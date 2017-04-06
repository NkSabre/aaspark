#!/bin/sh

ROOT_DIR=`pwd`
# ROOT_DIR=~/workspace/aaspark
DATA_DIR=/yjh/
TMP_DIR=/tmp/aaspark-yjh

hadoop fs -mkdir -p $DATA_DIR/ch07

mkdir -p $TMP_DIR
cd $TMP_DIR/
wget ftp://ftp.nlm.nih.gov/nlmdata/sample/medline/*.gz
gunzip *.gz
hadoop fs -put *.xml $DATA_DIR/ch07/
rm $TMP_DIR -rf

