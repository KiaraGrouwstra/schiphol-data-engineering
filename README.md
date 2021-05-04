# schiphol data engineering assessment

## requirements

- [openjdk](https://openjdk.java.net/)
- [sbt](https://www.scala-sbt.org/)
- [spark](https://spark.apache.org/)

## usage

```bash
# open explorational notebook
docker run -p 8888:8888 -p 4040:4040 -v $PWD:/home/jovyan jupyter/all-spark-notebook

# run tests
sbt test

# run tests through docker
docker run -v $PWD:/schiphol mozilla/sbt bash -c "cd /schiphol; sbt test"

# run program
# set SPARK_HOME as per https://stackoverflow.com/questions/46613651/how-to-setup-spark-home-variable

sbt "runMain  com.schiphol.kiara.assignment.batch"
sbt "testOnly com.schiphol.kiara.assignment.BatchSpec"

sbt "runMain  com.schiphol.kiara.assignment.streaming"
sbt "testOnly com.schiphol.kiara.assignment.StreamingSpec"

sbt "runMain  com.schiphol.kiara.assignment.sliding"
sbt "testOnly com.schiphol.kiara.assignment.SlidingSpec"

# package
sbt package
# submit to cluster
spark-submit [options] `ls ./target/scala-*/*.jar`
```
