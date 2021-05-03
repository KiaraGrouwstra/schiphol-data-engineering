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

# run program
# set SPARK_HOME as per https://stackoverflow.com/questions/46613651/how-to-setup-spark-home-variable

# works
sbt "runMain com.schiphol.kiara.assignment.batch"
# passes
sbt "testOnly com.schiphol.kiara.assignment.BatchSpec"

# seems to just run without writing content (data/out/stream-top10)
sbt "runMain com.schiphol.kiara.assignment.streaming"
# got issues around not being able to get top 10 since streaming doesn't support sorting (or at least not in a way I can use outside of tests as well)
sbt "testOnly com.schiphol.kiara.assignment.StreamingSpec"

# seems to just run without writing content (data/out/window-top10)
sbt "runMain com.schiphol.kiara.assignment.window"
# no expected result yet, still trying to see if output looks sane (but not writing to data/out/window-top10...)
sbt "testOnly com.schiphol.kiara.assignment.WindowSpec"
```
