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

/** Класс предоставляет возможность запускать Python-приложение.
 * Python-проект должен быть расположен в resources. Пакет(директория), где хранится проект должен совпадать
 * с пакетом дочернего класса.
 * @param mainPyName Имя main файла python, с которого стартует приложение.
 * @param logger Логгер процессов приложения.
 */
abstract class PythonApp(mainPyName: String = "main.py")(implicit logger: Logger) {

  /** Имя утилиты запуска или путь до него.
   * Например: "python" или "/home/bin/python"; "spark-submit" или "/home/spark/bin/spark-submit" */
  protected val starterTool: String

  /** Аргументы для Main.py */
  protected val pythonArgs: List[String] = Nil

  /** Путь к содержимому файлов Scala-приложения.
   * Хранит путь до JAR-файла, если запуск приложения произведён через JAR (В частности через команду spark-submit).
   * Хранит путь до target директории, если запуск произведён в среде разработки IDE.
   */
  protected lazy val resourcesStorage: String =
  /*
    Пример результата вызова new File(getClass.getResource("").getPath).getParent, когда wasLaunchedFromJAR = true:
    file:/Users/a19562665/IdeaProjects/TestJar/target/scala-2.12/testjar_2.12-0.1.jar!/ru/mardaunt/microservice/quality
  */
    new File(getClass.getResource("").getPath).getParent
                                                     .replaceAll("file:", "")
                                                     .split("!").head

  /** Определяет, был ли запуск приложения через JAR-файл.
   */
  lazy val wasLaunchedFromJAR: Boolean = new File(resourcesStorage).isFile


  protected val containerPath: JavaPath = new File(if (wasLaunchedFromJAR) System.getProperty("user.dir")
                                                   else getClass.getResource("").getPath)
                                                  .toPath

  // Пример: /data/yarn/local/usercache/19562665_ipa/appcache/application_1662463096126_8060/container_e100_1662463096126_8060_01_000001/user_python_app/my_pyspark.py
  protected val targetPythonDirectory: JavaPath = containerPath.normalize().resolve("user_python_app")

  /** Список хлебных крошек из пакета класса. */
  protected val packageBreadCrumbList: Array[String] = getClass.getPackage.getName.split("\\.")

  /** Список py-файлов в контейнере и базовый Main py-файл - точка старта Python-приложения. */
  protected val (pythonAppFileList, mainPy): (List[File], File) = movePythonAppToContainer()

  /** Запустить Python-приложение.
   * @param args Список аргументов, передаваемых в приложение.
   */
  def run(args: List[String] = pythonArgs): Int = {
    val command = Seq(starterTool, mainPy.toString) ++ args
    logger.info(s"Исполняемая команда: $command")
    command.!(SysProcessLogger(logger))
  }

  /** Вернуть список из строк маршрутов ко всем файлам Python-модуля качества.
   * Маршрут не содержит путь пакета Main-класса Scala.
   * Пример маршрута: "source/utils/increment.py"
   * Маршрут используется для получения доступа к ресурсам JAR-приложения
   * средствами метода getClass.getResourceAsStream("маршрут к файлу"). Особенность способа обращения к ресурсам
   * связана с недоступностью файлов проекта в runtime при запуска JAR-приложения.
   * https://stackoverflow.com/questions/20389255/reading-a-resource-file-from-within-jar
   * @return Список из строк маршрутов к файлам Python-модуля качества.
   */
  protected def getPythonFileRoutes(folderName: String = packageBreadCrumbList.last): List[String] = {
    /*
      В случае запуска JAR-приложения, средствами java.util.jar.{JarEntry, JarFile} перебираем весь JAR-архив,
      и достаём список всех файлов Python-модуля качества.
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
      /* Это условие для IDE. */
      val directoryRoute = if (packageBreadCrumbList.last == folderName) packageBreadCrumbList.mkString(File.separator)
                           else (packageBreadCrumbList ++ folderName).mkString(File.separator)
        val targetDir = Paths.get(getClass.getProtectionDomain.getCodeSource.getLocation.toURI).getParent.toFile
        val pyProjectMainDirectory: File = new File(targetDir, s"classes/$directoryRoute")
        val pyProjectTestDirectory: File = new File(targetDir, s"test-classes/$directoryRoute")

        recursiveGetFileList(pyProjectMainDirectory).map(_.getPath).toList ++
          recursiveGetFileList(pyProjectTestDirectory).map(_.getPath).toList
      }

    /*
      Имя файла из списка resourcesFileRoutes будет иметь вид "ru/mardaunt/source/someone.py"
      Нам необходимо удалить из маршрутов имя пакета Main-класса Scala. Чтобы осталось только "source/someone.py"
    */
    val separator = File.separator
    val regex = s"\\$separator${packageBreadCrumbList.last}\\$separator"
    val shortFileRoutes = resourcesFileRoutes.map(_.split(regex).last)
    shortFileRoutes
  }

  /** Рекурсивно обойти папку, чтобы получить список файлов. Используется в локальных запусках из среды разработки.
   * @param folder Экземпляр File папки.
   * @return Список всех экземпляров файлов в папке.
   */
  private def recursiveGetFileList(folder: File): Array[File] = {
    if (folder.exists())
      folder.listFiles.filter(_.isFile) ++ folder.listFiles.filter(_.isDirectory)
                                                           .flatMap(recursiveGetFileList)
    else Array()
  }

  /** Переместить Python-проект в контейнер.
   * @return Кортеж из списка py-файлов и Main py-файл.
   */
  protected def movePythonAppToContainer(): (List[File], File) = {
    val pyFileList: List[File] = getPythonFileRoutes().map(moveToContainer(_))
    logger.info(s"Список py-файлов перемещённых в контейнер: $pyFileList")
    val mainPyFile = pyFileList.find(_.getPath contains mainPyName).get
    (pyFileList, mainPyFile)
  }

  /** Перенести файл из ресурсов в контейнер Scala-приложения.
   * Перемещение в контейнер требуется для использования такого файла в субпроцессах, вызываемых с помощью
   * библиотеки scala.sys.process._
   * @param fileRoute Маршрут существующего файла, которое требуется переместить в контейнер приложения
   *                  (например, source/utils/config.py).
   * @param preserveFolders Флаг даёт возможность отключить создание папок указанных в маршруте fileRoute.
   *                        Например fileRoute = "py_files/my_folder/my_file.txt", и preserveFolders = false.
   *                        Тогда в контейнере не будет создана директория py_files/my_folder, и my_file.txt
   *                        будет перемещён в корень контейнера.
   * @return Экземпляр перемещённого файла.
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
