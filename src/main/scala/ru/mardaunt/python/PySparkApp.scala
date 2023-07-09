package ru.mardaunt.python

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import ru.mardaunt.python.logger.SysProcessLogger

import java.io.File
import java.nio.file.{Path => JavaPath}
import scala.language.{implicitConversions, postfixOps}
import scala.sys.process._

/** Класс предоставляет возможность запускать PySpark-приложение.
 * Python-проект должен быть расположен в resources. Пакет(директория), где хранится проект должен совпадать
 * с пакетом дочернего класса.
 * @param mainPyName Имя main файла python, с которого стартует приложение.
 * @param needKerberosAuth Флаг необходимости проходить Kerberos авторизацию.
 * @param pyFilesDirName Имя директории из resources, где расположены файлы для опции --py-files.
 * @param spark Spark-сессия.
 * @param logger Логгер процессов приложения.
 */
abstract class PySparkApp(mainPyName: String = "main.py",
                          needKerberosAuth: Boolean = true,
                          pyFilesDirName: Option[String] = None)
                         (implicit spark: SparkSession, logger: Logger) extends PythonApp(mainPyName) {

  /** Дополнительные параметры Spark для команды spark-submit. Например: List("--conf", "spark.security.enabled=true").
   * Обратите внимание, что в значениях списка НЕ должно быть пробелов.
   */
  protected val additionalSparkConfList: List[String] = Nil

  private lazy val pythonAppZip: File = getSourceZip

  /** Список файлов, которые будут переданы  в опцию --py-files*/
  private lazy val pyFileList: List[File] = pyFilesDirName.map(getPythonFileRoutes(_).map(moveToContainer(_, preserveFolders = false)))
                                                            .getOrElse(Nil)

  /** Значение для опции --py-files */
  private lazy val pyFiles: String = pythonAppZip::pyFileList mkString ","

  /** Вызвать команду spark-submit, которая запускает Pyspark-приложение.
   * @param args Список аргументов, передаваемых в приложение.
   * @return Код выполнения команды spark-submit. Успех выполнения вернёт 0.
   */
  override def run(args: List[String] = pythonArgs): Int = {

    logger.info(s"Аргументы для --py-files: $pyFiles")

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

    val kinitCommand = if (needKerberosAuth) s"kinit -kt $keytabFile $principal" else "echo Local Kinit"

    logger.info("Старт выполнения команды SPARK-SUBMIT для запуска Pyspark-приложения:")
    logger.info(sparkSubmitCommand.mkString(" "))
    val executionCode = (kinitCommand #&& sparkSubmitCommand).!(SysProcessLogger(logger))
    logger.info(s"Завершена работа Pyspark-приложения с кодом $executionCode.")
    executionCode
  }


  /** Получить архив директории дочернего пакета. Внутри архива будут храниться py-файлы Spark-приложения.
   * Архив будет создан в контейнере spark-приложения. Будет использован для передачи в опцию --py-files.
   *
   * @return Экземпляр File архива директории source из контейнера.
   */
  private def getSourceZip: File = {

    logger.info(s"Программный путь Main-класса: ${new File(getClass.getResource("").getPath).getParent}")
    logger.info(s"Путь до JAR файла Scala-приложения: $resourcesStorage")
    logger.info(s"Запуск Scala-приложения произведён из JAR: $wasLaunchedFromJAR")
    logger.info(s"Количество файлов в Python-модуле: ${pythonAppFileList.length}")

    logger.info("Перечисление файлов Python-модуля из директории в контейнере: ")
    pythonAppFileList.foreach(file => logger.info(s"Путь к файлу: $file ${file.isFile}"))

    val zipFilePath: JavaPath = containerPath.resolve("user_python_app.zip")
    val sourceFolder: JavaPath = targetPythonDirectory

    logger.info(s"Директория в контейнере : $sourceFolder")
    logger.info(s"Путь до создаваемого user_python_app.zip : $zipFilePath")

    logger.info("Запуск создания архива в контейнере из Python-модуля.")
    new ZipFileCompressor().createZipFile(zipFilePath, sourceFolder.toFile)
    logger.info("Процесс создания архива Python-модуля завершен успешно.")

    val pythonAppZip: File = zipFilePath.toFile
    logger.info(s"Существует ли в контейнере Scala-приложения файл user_python_app.zip? : ${pythonAppZip.isFile}")
    pythonAppZip
  }

}
