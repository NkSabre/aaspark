#!/bin/sh

ROOT_DIR=`pwd`
# ROOT_DIR=~/workspace/aaspark
DATA_DIR=/yjh/
TMP_DIR=/tmp/aaspark-yjh

hadoop fs -mkdir -p $DATA_DIR/ch06

mkdir -p $TMP_DIR
cd $TMP_DIR/
curl -s -L http://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles-multistream.xml.bz2 | bzip2 -cd  | hadoop fs -put - $DATA_DIR/ch06/wikidump.xml
rm $TMP_DIR -rf

