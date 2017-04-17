#! /bin/bash
set -eu

basedir=$HOME/code/spark-tracing

#dev/make-distribution.sh --name spark-tracing -Pyarn -Phive -Phadoop-2.7 -Dhadoop.version=2.7.3
#build/mvn -Phadoop-2.7 -Dhadoop.version=2.7.3 -DskipTests package

$basedir/sbin/stop-all.sh
$basedir/sbin/start-master.sh
$basedir/sbin/start-slave.sh spark://localhost:7077

firefox http://localhost:5010 &
$basedir/bin/spark-shell --master spark://localhost:7077 < $basedir/matt/sparkpi.scala

