#!/bin/sh

ROOT_DIR=`pwd`
# ROOT_DIR=~/workspace/aaspark
DATA_DIR=/yjh/
TMP_DIR=/tmp/aaspark-yjh

hadoop fs -mkdir -p $DATA_DIR/ch06

mkdir -p $TMP_DIR
cd $TMP_DIR/
curl -o enwiki-latest-pages-articles-multistream.xml.bz2 https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles-multistream.xml.bz2
bzip2 -cd enwiki-latest-pages-articles-multistream.xml.bz2 | hadoop fs -put - $DATA_DIR/ch06/wikidump.xml
rm $TMP_DIR -rf

