#!/usr/bin/env bash
spark-submit --total-executor-cores 16 --master spark://cubeheader1:7077 --class xyz.yjh.aaspark.ch03.Recommender  ~/aaspark-assembly-1.0.jar