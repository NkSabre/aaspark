#!/usr/bin/env bash
spark-submit --master spark://cubeheader1:7077 --class xyz.yjh.aaspark.ch05.AnomalyDetection  ~/aaspark-assembly-1.0.jar