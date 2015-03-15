Step 1. create build.sbt

```
name := "scala-aggregation-test"
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
$SPARK_HOME/bin/spark-submit --class <classname> <jar_file_path> <file_path> <str_start_index> <str_end_index> <num_of_print_lines>

For example:
```
$SPARK_HOME/bin/spark-submit --class ScalaAggregation target/scala-2.11/scala-aggregation-test_2.11-0.0.1.jar hdfs://localhost:8020/tmp/benchmark/text/tiny/uservisits 0 9 10
```
