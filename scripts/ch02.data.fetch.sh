#!/bin/sh

ROOT_DIR=`pwd`
# ROOT_DIR=~/workspace/aaspark
DATA_DIR=/yjh
TMP_DIR=/tmp/aaspark-yjh

hadoop fs -mkdir -p $DATA_DIR/linkage

mkdir -p $TMP_DIR
cd $TMP_DIR/
curl -o donation.zip http://bit.ly/1Aoywaq
# cp ~/Downloads/donation.zip ./donation.zip
unzip donation.zip
unzip 'block_*.zip'
#mv *.csv $DATA_DIR/ch02/
hadoop fs -put block_*.csv /yjh/linkage
rm $TMP_DIR -rf
