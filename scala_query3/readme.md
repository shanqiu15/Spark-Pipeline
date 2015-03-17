Step 1. create build.sbt

```
name := "scala-join-test"
version := "0.0.1"
scalaVersion := "2.11.6"
// additional libraries
libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "1.2.0" % "provided"
)
```

Step2. build the package

```
sbt clean package
```

Step3. Submit the spark job.
(--class: name of the class)
$SPARK_HOME/bin/spark-submit --class <classname> <jar_file_path> <ranking_path> <uservisit_path> <start_date> <end_date>

For example:
```
$SPARK_HOME/bin/spark-submit --class ScalaJoin target/scala-2.11/scala-join-test_2.11-0.0.1.jar hdfs://localhost:8020/tmp/benchmark/text/tiny/rankings hdfs://localhost:8020/tmp/benchmark/text/tiny/uservisits 1980-01-01 1980-04-01
```
