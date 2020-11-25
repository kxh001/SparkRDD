@echo off
call mvn clean package
call spark-submit --class edu.ucr.cs.cs226.xkong016.SparkRDD target/SparkRDD-1.0-SNAPSHOT.jar %1