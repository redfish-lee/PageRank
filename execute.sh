#!/bin/bash

# Do not uncomment these lines to directly execute the script
# Modify the path to fit your need before using this script

if [ -z $1 ] && [ -z $2 ] 
then
    echo "(Option): ./execute.sh <filename> <iteraion>"
    echo "      -): filename : input-100M/input-1G/input-10G/input-50G"
    echo "      -): iteration: default to converge (max = 30).."
    exit 1
elif [ -z $2 ]
then
    name=$1
    echo "set input: $name with max(30) iterations.."
else
    name=$1
    ITER=$2
    echo "set input: $name with $ITER iterations..."

fi

# INPUT_FILE=/path/to/pagerank/input/$name
# OUTPUT_FILE=/path/to/pagerank/output/directory/$name
# RESULT_FILE=/path/to/pagerank/output/directory/$name/result
JAR=pageRank.jar

echo "INPUT  FILE: $INPUT_FILE"
echo "OUTPUT FILE: $OUTPUT_FILE"
echo "RESULT FILE: $RESULT_FILE"

hdfs dfs -rm -r $OUTPUT_FILE
hadoop jar $JAR PageRank.PageRanking $INPUT_FILE $RESULT_FILE $ITER

echo "MERGE OUTPUT FILENAME: $name.out"
hdfs dfs -getmerge $RESULT_FILE/final $name.out
