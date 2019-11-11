#!/bin/bash
# convienence shell script for execution
if  [ -f "/common.conf"]; then
  java -jar /consumer.jar start --config /common.conf
else
  java -jar /consumer.jar start
fi