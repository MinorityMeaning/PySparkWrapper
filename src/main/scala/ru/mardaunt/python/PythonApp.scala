package ru.mardaunt.python

import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
import ru.mardaunt.python.logger.SysProcessLogger

import scala.sys.process._
import scala.language.implicitConversions
import scala.collection.convert.ImplicitConversions.`enumeration AsScalaIterator`
import java.io.File
import java.nio.file.{Paths, Path => JavaPath}
import java.util.jar.JarFile

/** The class provides the ability to run a Python application.
 * The Python project should be located in resources. The package (directory) where the project is stored
 * must match the package of the child class.
 * @param mainPyName The name of the main python file from which the application starts.
 * @param logger Application process logger.
 */
abstract class PythonApp(mainPyName: String = "main.py")(implicit logger: Logger) {

  /** The name of the startup utility or the path to it.
   * Example: "python" or "/home/bin/python"; "spark-submit" or "/home/spark/bin/spark-submit" */
  protected val starterTool: String

  /** Arguments for Main.py */
  protected val pythonArgs: List[String] = Nil

  /** The path to the contents of the Scala application files.
   * Stores the path to the JAR file if the application is launched via JAR (in particular, via the spark-submit command).
   * Stores the path to the target directory if launched in the IDE development environment.
   */
  protected lazy val resourcesStorage: String =
  /*
    Example of a call result `new File(getClass.getResource("").getPath).getParent`, when `wasLaunchedFromJAR = true`:
    file:/Users/a19562665/IdeaProjects/TestJar/target/scala-2.12/testjar_2.12-0.1.jar!/ru/mardaunt/microservice/quality
  */
    new File(getClass.getResource("").getPath).getParent
                                                     .replaceAll("file:", "")
                                                     .split("!").head

  /** Determines whether the application was launched via a JAR file.
   */
  lazy val wasLaunchedFromJAR: Boolean = new File(resourcesStorage).isFile


  protected val containerPath: JavaPath = new File(if (wasLaunchedFromJAR) System.getProperty("user.dir")
                                                   else getClass.getResource("").getPath)
                                                  .toPath

  // Example: /data/yarn/local/usercache/19562665_ipa/appcache/application_1662463096126_8060/container_e100_1662463096126_8060_01_000001/user_python_app/my_pyspark.py
  protected val targetPythonDirectory: JavaPath = containerPath.normalize().resolve("user_python_app")

  /** A list of bread crumbs from the class package. */
  protected val packageBreadCrumbList: Array[String] = getClass.getPackage.getName.split("\\.")

  /** The list of py files in the container and the base Main py file are the starting point of the Python application. */
  protected val (pythonAppFileList, mainPy): (List[File], File) = movePythonAppToContainer()

  /** Launch the Python application.
   * @param args A list of arguments passed to the application.
   */
  def run(args: List[String] = pythonArgs): Int = {
    val command = Seq(starterTool, mainPy.toString) ++ args
    logger.info(s"Executable command: $command")
    command.!(SysProcessLogger(logger))
  }

  /** Return a list of route strings to all Python module files.
   * The route does not contain the path of the Scala Main class package.
   * Route example: "source/utils/increment.py"
   * The route is used to access JAR application resources using the method
   * getClass.getResourceAsStream("file_route"). The peculiarity of the method of accessing resources
   * is related to the unavailability of project files in runtime when launching a JAR application.
   * https://stackoverflow.com/questions/20389255/reading-a-resource-file-from-within-jar
   * @return A list of lines of routes to the files of the Python quality module.
   */
  protected def getPythonFileRoutes(folderName: String = packageBreadCrumbList.last): List[String] = {
    /*
      In the case of launching a JAR application, by means of `java.util.jar.{JarEntry, JarFile}`
      going through the entire JAR archive, and we get a list of all the files of the Python module.
    */
    val resourcesFileRoutes =
      if (wasLaunchedFromJAR) {
        val jarFile = new JarFile(resourcesStorage)
        val entryList = jarFile.entries()
        val resourceFileRoutes = entryList.map(_.getName)
                                          .toList
                                          .filter(_.contains(packageBreadCrumbList.mkString(File.separator)))
                                          .filter(_.contains(Seq("", folderName, "").mkString(File.separator)))
                                          .filterNot(_.endsWith(File.separator))
        resourceFileRoutes
      }
      else {
      /* This is a condition for the IDE. */
      val directoryRoute = if (packageBreadCrumbList.last == folderName) packageBreadCrumbList.mkString(File.separator)
                           else (packageBreadCrumbList ++ folderName).mkString(File.separator)
        val targetDir = Paths.get(getClass.getProtectionDomain.getCodeSource.getLocation.toURI).getParent.toFile
        val pyProjectMainDirectory: File = new File(targetDir, s"classes/$directoryRoute")
        val pyProjectTestDirectory: File = new File(targetDir, s"test-classes/$directoryRoute")

        recursiveGetFileList(pyProjectMainDirectory).map(_.getPath).toList ++
          recursiveGetFileList(pyProjectTestDirectory).map(_.getPath).toList
      }

    /*
      The file name from the resourcesFileRoutes list will look like "ru/mardaunt/source/someone.py"
      We need to remove the package name of the Scala Main class from the routes. So that only remains "source/someone.py"
    */
    val separator = File.separator
    val regex = s"\\$separator${packageBreadCrumbList.last}\\$separator"
    val shortFileRoutes = resourcesFileRoutes.map(_.split(regex).last)
    shortFileRoutes
  }

  /** Recursively crawl the folder to get a list of files. Used in local launches from the development environment.
   * @param folder An instance of the File folder.
   * @return A list of all instances of files in the folder.
   */
  private def recursiveGetFileList(folder: File): Array[File] = {
    if (folder.exists())
      folder.listFiles.filter(_.isFile) ++ folder.listFiles.filter(_.isDirectory)
                                                           .flatMap(recursiveGetFileList)
    else Array()
  }

  /** Move the Python project to a container.
   * @return A tuple from the list of py files and the Main py file.
   */
  protected def movePythonAppToContainer(): (List[File], File) = {
    val pyFileList: List[File] = getPythonFileRoutes().map(moveToContainer(_))
    logger.info(s"List of py files moved to the container: $pyFileList")
    val mainPyFile = pyFileList.find(_.getPath contains mainPyName).get
    (pyFileList, mainPyFile)
  }

  /** Transfer a file from resources to a Scala application container.
   * Moving to a container is required to use such a file in subprocesses called using
   * the scala.sys.process._ library
   * @param fileRoute The route of an existing file to be moved to the application container
   *                  (example, source/utils/config.py).
   * @param preserveFolders The flag allows you to disable the creation of folders specified in the fileRoute.
   *                        Example fileRoute = "py_files/my_folder/my_file.txt", and `preserveFolders = false`.
   *                        Then a directory will not be created in the container py_files/my_folder, and my_file.txt
   *                        will be moved to the root of the container.
   * @return Instance of the moved file.
   */
  protected def moveToContainer(fileRoute: String, preserveFolders: Boolean = true): File = {
    val fileSourceStream = getClass.getResourceAsStream(fileRoute)
    val fileName = if (preserveFolders) fileRoute else fileRoute.split(File.separator).last
    val file = targetPythonDirectory.resolve(fileName).toFile
    FileUtils.copyInputStreamToFile(fileSourceStream, file)
    fileSourceStream.close()
    file
  }

}
