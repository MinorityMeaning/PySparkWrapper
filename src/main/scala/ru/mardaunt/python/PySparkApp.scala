package ru.mardaunt.python

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import ru.mardaunt.python.logger.SysProcessLogger

import java.io.File
import java.nio.file.{Path => JavaPath}
import scala.language.{implicitConversions, postfixOps}
import scala.sys.process._

/** The class provides the ability to launch a PySpark application.
 * Python-app must be located in resources. The package (directory) where the project is stored
 * must match the package of the child class.
 * @param mainPyName The name of the main python file from which the application starts.
 * @param needKerberosAuth Flag of the need to pass Kerberos authorization.
 * @param pyFilesDirName The name of the directory from resources where the files for the --py-files option are located.
 * @param spark Spark-session.
 * @param logger Application Process Logger.
 */
abstract class PySparkApp(mainPyName: String = "main.py",
                          needKerberosAuth: Boolean = true,
                          pyFilesDirName: Option[String] = None)
                         (implicit spark: SparkSession, logger: Logger) extends PythonApp(mainPyName) {

  /** Additional Spark parameters for the spark-submit command. For example: List("--conf", "spark.security.enabled=true").
   * Note that there should be NO spaces in the values of the list.
   */
  protected val additionalSparkConfList: List[String] = Nil

  private lazy val pythonAppZip: File = getSourceZip

  /** The list of files to be transferred to the --py-files option. */
  private lazy val pyFileList: List[File] = pyFilesDirName.map(getPythonFileRoutes(_).map(moveToContainer(_, preserveFolders = false)))
                                                            .getOrElse(Nil)

  /** The value for the --py-files option. */
  private lazy val pyFiles: String = pythonAppZip::pyFileList mkString ","

  /** Call the spark-submit command, which launches the PySpark application.
   * @param args A list of arguments passed to the application.
   * @return The code for executing the spark-submit command. Execution success will return 0.
   */
  override def run(args: List[String] = pythonArgs): Int = {

    logger.info(s"Arguments for --py-files: $pyFiles")

    lazy val keytabFile = new File(spark.conf.get("spark.kerberos.keytab"))
    lazy val principal  = spark.conf.get("spark.kerberos.principal")

    val kerberosOptionList =
      if (needKerberosAuth)
        "--keytab"::s"$keytabFile" ::
        "--principal"::principal   :: Nil
      else Nil

    val sparkSubmitCommand =
      starterTool                                                      ::
        kerberosOptionList                                             :::
        additionalSparkConfList                                        :::
        "--conf"::"spark.security.credentials.hadoopfs.enabled=true"   ::
        "--master"::s"${if (wasLaunchedFromJAR) "yarn" else "local"}"  ::
        "--verbose"                                                    ::
        "--py-files"::s"$pyFiles"                                      ::
        s"$mainPy"::args                                               ::: Nil

    val echo         = if (System.getProperty("os.name") contains "Windows") "cmd.exe /c echo" else "echo"
    val kinitCommand = if (needKerberosAuth) s"kinit -kt $keytabFile $principal" else s"$echo Local Kinit"

    logger.info("Start of the SPARK-SUBMIT command to launch the PySpark application:")
    logger.info(sparkSubmitCommand.mkString(" "))
    val executionCode = (kinitCommand #&& sparkSubmitCommand).!(SysProcessLogger(logger))
    logger.info(s"The work of the PySpark application has been completed with the code: $executionCode.")
    executionCode
  }


  /** Get the archive of the child package directory. The Spark application's py files will be stored inside the archive.
   * The archive will be created in the spark application container. Will be used to pass to the --py-files option.
   *
   * @return An instance of the File archive of the source directory from the container.
   */
  private def getSourceZip: File = {

    logger.info(s"Program path of the Main class: ${new File(getClass.getResource("").getPath).getParent}")
    logger.info(s"Path to the Scala application JAR file: $resourcesStorage")
    logger.info(s"The Scala application was launched from a JAR: $wasLaunchedFromJAR")
    logger.info(s"Number of files in the Python module: ${pythonAppFileList.length}")

    logger.info("Listing Python module files from a directory in a container: ")
    pythonAppFileList.foreach(file => logger.info(s"File path: $file ${file.isFile}"))

    val zipFilePath: JavaPath = containerPath.resolve("user_python_app.zip")
    val sourceFolder: JavaPath = targetPythonDirectory

    logger.info(s"The directory in the container: $sourceFolder")
    logger.info(s"The path to the created user_python_app.zip: $zipFilePath")

    logger.info("Starting the creation of an archive in a container from a Python module.")
    new ZipFileCompressor().createZipFile(zipFilePath, sourceFolder.toFile)
    logger.info("The process of creating the Python module archive has been completed successfully.")

    val pythonAppZip: File = zipFilePath.toFile
    logger.info(s"Is there a file in the Scala application container user_python_app.zip?: ${pythonAppZip.isFile}")
    pythonAppZip
  }

}
