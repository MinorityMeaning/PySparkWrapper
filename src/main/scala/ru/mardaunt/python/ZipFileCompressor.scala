package ru.mardaunt.python

import org.apache.commons.compress.archivers.zip.{ZipArchiveEntry, ZipArchiveOutputStream}
import org.apache.commons.compress.utils.IOUtils

import java.io.{BufferedOutputStream, File, FileInputStream}
import java.nio.file.{Files, Path}

/** Utility for archiving files and directories.
 */
class ZipFileCompressor {

  /** Create a zip archive.
   * @param zipFilePath The full path of the archive, including the name of the future zip archive.
   * @param fileOrDirectoryToZip An instance of the file or directory to archive.
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

  /** Add a file or directory to the stream of the future archive.
   * @param zipArchiveOutputStream Stream Instance ZipArchiveOutputStream.
   * @param fileToZip File or directory to add to the stream ZipArchiveOutputStream.
   * @param base Separator for the name of the file being added.
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