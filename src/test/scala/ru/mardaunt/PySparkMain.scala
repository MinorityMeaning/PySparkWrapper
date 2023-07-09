package ru.mardaunt

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import ru.mardaunt.python.PySparkApp
import ru.mardaunt.python.logger.SimpleLogger

object PySparkMain extends App {

  lazy val spark = SparkSession.builder()
    .master("local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  class SimplePySparkApp(implicit spark: SparkSession, logger: Logger)
    extends PySparkApp(mainPyName = "pyspark_main.py", needKerberosAuth = false) {
    /* override protected val starterTool: String = "C:\\Spark\\spark-3.4.1-bin-hadoop3\\bin\\spark-submit.cmd" */
    override protected val starterTool: String = "spark-submit" /* unix os */
  }

  new SimplePySparkApp()(spark, SimpleLogger()).run()

}
