package ru.mardaunt

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import ru.mardaunt.python.logger.SimpleLogger
import ru.mardaunt.python.PythonApp

object PythonMain extends App {

  lazy val spark = SparkSession.builder()
    .master("local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  class SimplePythonApp(logger: Logger) extends PythonApp("main.py")(logger) {
    override protected val starterTool: String =
      if (System.getProperty("os.name") contains "Windows") "python" else "python3"
  }


  println(System.getProperty("os.name"))
  println("Python exit code: " + new SimplePythonApp(SimpleLogger()).run())

}
