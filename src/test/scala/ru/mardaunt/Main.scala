package ru.mardaunt

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import ru.mardaunt.python.logger.SimpleLogger
import ru.mardaunt.python.{PySparkApp, PythonApp}

object Main extends App {

  lazy val spark = SparkSession.builder()
    .master("local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  class SimplePySparkApp(implicit spark: SparkSession, logger: Logger) extends PySparkApp(needKerberosAuth = false) {
    override protected val starterTool: String = "spark-submit"
  }

  class SimplePythonApp(logger: Logger) extends PythonApp("main.py")(logger) {
    override protected val starterTool: String = "python3"
  }

  //new SimplePythonApp(SimpleLogger()).run()

  new SimplePySparkApp()(spark, SimpleLogger()).run()

  //println(new File(getClass.getResource("this_is_file.txt").getPath).isFile)
  //println(getClass.getResource("this_is_file.txt").getPath)
}
