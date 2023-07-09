package ru.mardaunt.python

import org.apache.commons.compress.archivers.zip.{ZipArchiveEntry, ZipArchiveOutputStream}
import org.apache.commons.compress.utils.IOUtils

import java.io.{BufferedOutputStream, File, FileInputStream}
import java.nio.file.{Files, Path}

/** Утилита для архивирования файлов и директорий.
 */
class ZipFileCompressor {

  /** Создать zip-архив.
   * @param zipFilePath Полный путь архива, включая имя будущего zip-архива.
   * @param fileOrDirectoryToZip Экземпляр файла или директории, которую необходимо архивировать.
   */
  def createZipFile(zipFilePath: Path, fileOrDirectoryToZip: File): Unit = {
    val outputStream = Files.newOutputStream(zipFilePath)
    val bufferedOutputStream   = new BufferedOutputStream(outputStream)
    val zipArchiveOutputStream = new ZipArchiveOutputStream(bufferedOutputStream)
    addFileToZipStream(zipArchiveOutputStream, fileOrDirectoryToZip, "")
    zipArchiveOutputStream.close()
    bufferedOutputStream.close()
    outputStream.close()
  }

  /** Добавить файл или директорию в поток будущего архива.
   * @param zipArchiveOutputStream Экземпляр потока ZipArchiveOutputStream.
   * @param fileToZip Файл или директория для добавления в поток ZipArchiveOutputStream.
   * @param base Разделитель для имени добавляемого файла.
   */
  private def addFileToZipStream(zipArchiveOutputStream: ZipArchiveOutputStream, fileToZip: File, base: String): Unit = {

    val entryName       = base + fileToZip.getName
    val zipArchiveEntry = new ZipArchiveEntry(fileToZip, entryName)
    zipArchiveOutputStream.putArchiveEntry(zipArchiveEntry)
    fileToZip match {
      case file if file.isFile =>
        val fileInputStream = new FileInputStream(fileToZip)
        IOUtils.copy(fileInputStream, zipArchiveOutputStream)
        zipArchiveOutputStream.closeArchiveEntry()
        fileInputStream.close()

      case directory if directory.isDirectory =>
        zipArchiveOutputStream.closeArchiveEntry()
        val files = fileToZip.listFiles
        files.foreach(addFileToZipStream(zipArchiveOutputStream, _, entryName + File.separator))
    }
  }

}