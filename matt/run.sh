#! /bin/bash
set -eu

basedir="$HOME/code/spark-tracing"
distdir="$basedir/mindist"
user="dev-user"
#master="stcindia-node-1.fyre.ibm.com"
#slaves="stcindia-node-{2..4}.fyre.ibm.com"
master="dynalloc1.fyre.ibm.com"
slaves="dynalloc2.fyre.ibm.com dynalloc3.fyre.ibm.com dynalloc4.fyre.ibm.com"
dest="/home/$user/spark-tracing"
port="5010"

while [[ $# > 0 ]]
	do case "$1" in
	"dist")
		dev/make-distribution.sh --name spark-tracing -Pyarn -Phive -Phadoop-2.7 -Dhadoop.version=2.7.3
		;;
	"clean")
		build/mvn -Phadoop-2.7 -Dhadoop.version=2.7.3 -DskipTests clean
		;;
	"build")
		build/mvn -Phadoop-2.7 -Dhadoop.version=2.7.3 -DskipTests package

		cat <<- ! > $basedir/conf/spark-defaults.conf
		spark.master.ui.port $port
		spark.worker.ui.port $port
		!
		cp $basedir/conf/log4j.properties{.template,}
		cat <<- ! >> $basedir/conf/log4j.properties
		
		log4j.logger.org.apache.spark.util.tracing.RpcTraceLogger=TRACE, rpctrace
		log4j.additivity.org.apache.spark.util.tracing.RpcTraceLogger=false
		log4j.logger.org.apache.spark.util.tracing.EventTraceLogger=TRACE, eventtrace
		log4j.additivity.org.apache.spark.util.tracing.EventTraceLogger=false

		log4j.appender.rpctrace=org.apache.spark.util.tracing.IdFileAppender
		log4j.appender.rpctrace.File=/tmp/spark-rpc_%i.tsv
		log4j.appender.rpctrace.Append=false
		log4j.appender.rpctrace.layout=org.apache.log4j.PatternLayout
		log4j.appender.rpctrace.layout.ConversionPattern=%m%n
		log4j.appender.eventtrace=org.apache.spark.util.tracing.IdFileAppender
		log4j.appender.eventtrace.File=/tmp/spark-event_%i.tsv
		log4j.appender.eventtrace.Append=false
		log4j.appender.eventtrace.layout=org.apache.log4j.PatternLayout
		log4j.appender.eventtrace.layout.ConversionPattern=%m%n
		!
		#log4j.additivity.org.apache.spark.util.tracing=false
		#log4j.logger.org.apache.spark.util.tracing.EventTraceLogger=TRACE, tracelog.event
		#log4j.logger.org.apache.spark.util.tracing.RpcTraceLogger=TRACE, tracelog.rpc

		#log4j.appender.tracelog=org.apache.spark.util.tracing.IdFileAppender
		#log4j.appender.tracelog.Append=false
		#log4j.appender.tracelog.layout=org.apache.log4j.PatternLayout
		#log4j.appender.tracelog.layout.ConversionPattern=%m%n
		#log4j.appender.tracelog.rpc.File=/tmp/spark-rpc_%i.tsv
		#log4j.appender.tracelog.event.File=/tmp/spark-event_%i.tsv
		;;
	"remote")	
		rsync -av --copy-unsafe-links --delete --progress $distdir/ $user@$master:$dest
		ssh -t $user@$master "for host in $slaves; do rsync -av --copy-unsafe-links --delete $dest/ \$host:$dest; done"
		ssh -t $user@$master "$dest/sbin/stop-all.sh; for host in $slaves; do ssh \$host $dest/sbin/stop-all.sh; done"
		ssh -t $user@$master "$dest/sbin/start-master.sh; for host in $slaves; do ssh \$host $dest/sbin/start-slave.sh $master:7077; done"

		firefox http://$master:$port &
		ssh -t $user@$master $dest/bin/spark-shell --master spark://$master:7077 < $basedir/matt/sparkpi.scala
		;;
	"local")
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

