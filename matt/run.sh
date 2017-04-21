#! /bin/bash
set -eu

basedir="$HOME/code/spark-tracing"
distdir="$basedir/mindist"
resultdir="$basedir/matt/runs"
user="dev-user"
#master="stcindia-node-1.fyre.ibm.com"
#slaves="stcindia-node-{2..4}.fyre.ibm.com"
master="dynalloc1.fyre.ibm.com"
slaves="dynalloc2.fyre.ibm.com dynalloc3.fyre.ibm.com dynalloc4.fyre.ibm.com"
dest="/test/spark-tracing"
traceout="/tmp/spark-trace"
port="5010"

while [[ $# > 0 ]]
	do case "$1" in
	"dist")
		dev/make-distribution.sh --name spark-tracing -Pyarn -Phive -Phadoop-2.7 -Dhadoop.version=2.7.3
		;;
	"clean")
		build/mvn -Phive -Pyarn -Phadoop-2.7 -Dhadoop.version=2.7.3 -DskipTests clean
		;;
	"build")
		build/mvn -Phive -Pyarn -Phadoop-2.7 -Dhadoop.version=2.7.3 -DskipTests package

		cat <<- ! > $basedir/conf/spark-defaults.conf
		spark.master.ui.port $port
		spark.worker.ui.port $port
		spark.extraListeners org.apache.spark.util.tracing.ListenerTraceLogger
		spark.hadoop.yarn.timeline-service.enabled false
		spark.executor.instances 1
		!
		cp $basedir/conf/log4j.properties{.template,}
		cat <<- ! >> $basedir/conf/log4j.properties

		log4j.rootCategory=WARN, console

		log4j.logger.org.apache.spark.util.tracing.TraceLogger=TRACE, eventtrace
		log4j.additivity.org.apache.spark.util.tracing.TraceLogger=false

		log4j.appender.eventtrace=org.apache.spark.util.tracing.IdFileAppender
		log4j.appender.eventtrace.File=$traceout/%i.tsv
		log4j.appender.eventtrace.Append=false
		log4j.appender.eventtrace.layout=org.apache.log4j.PatternLayout
		log4j.appender.eventtrace.layout.ConversionPattern=%m%n
		!
		#chmod -R 777 $basedir
		;;
	"remote")
		rsync -av --copy-unsafe-links --delete --progress $distdir/ $user@$master:$dest
		ssh -t $user@$master "cat /etc/spark2/conf/spark-defaults.conf | sed '/spark.yarn.archive/s/^/#/' >> $dest/conf/spark-defaults.conf"
		ssh -t $user@$master "for host in $slaves; do rsync -av --copy-unsafe-links --delete $dest/ \$host:$dest; done"
		ssh -t $user@$master "sudo rm -rf $traceout; for host in $slaves; do ssh -t \$host sudo rm -rf $traceout; done"
		#ssh -t $user@$master "$dest/sbin/stop-all.sh; for host in $slaves; do ssh \$host $dest/sbin/stop-all.sh; done"
		#ssh -t $user@$master "$dest/sbin/start-master.sh; for host in $slaves; do ssh \$host $dest/sbin/start-slave.sh $master:7077; done"

		#firefox http://$master:$port &
		ssh -t $user@$master sudo -iu notebook HADOOP_CONF_DIR=/etc/hadoop/conf $dest/bin/spark-shell --master yarn < $basedir/matt/sparkpi.scala
		ssh -t $user@$master "sudo chown -R dev-user $traceout; for host in $slaves; do ssh -t \$host sudo chown -R dev-user $traceout; done"
		ssh -t $user@$master "for host in $slaves; do rsync -av \$host:$traceout/ $traceout; done"
		localout="$resultdir/trace_$(date +"%Y%m%d-%H%M%S")"
		mkdir "$localout"
		rsync -av --progress $user@$master:$traceout/ "$localout"
		ssh -t $user@$master "rm -r $traceout; for host in $slaves; do ssh -t \$host rm -r $traceout; done"
		;;
	"local")
		rm -f $traceout
		$distdir/sbin/stop-all.sh
		#$distdir/sbin/start-master.sh
		#$distdir/sbin/start-slave.sh spark://localhost:7077

		#firefox http://localhost:8088 &
		$distdir/bin/spark-shell --master yarn < $basedir/matt/sparkpi.scala
		;;
	*)
		echo "Unknown action $1"
		;;
	esac
	shift
done

