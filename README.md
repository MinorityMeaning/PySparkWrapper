# Symbiosis of Scala Spark and PySpark

The library solves the problem of interaction between spark applications developed in Scala and Python.
This can help out when Spark manipulations need to be performed in Scala and then in Python within a single run.
It is possible to observe some need for such functionality:

- Invoking Pyspark script from Scala Spark Code
  https://stackoverflow.com/questions/68763164/invoking-pyspark-script-from-scala-spark-code
- Running PySpark from Scala/Java Spark
  https://stackoverflow.com/questions/56132948/running-pyspark-from-scala-java-spark
- Passing sparkSession Between Scala Spark and PySpark
  https://stackoverflow.com/questions/58185042/passing-sparksession-between-scala-spark-and-pyspark

The need may be caused by the lack of the ability to rewrite the code from one language to another.

## How to use:
