package ru.mardaunt.python.logger

import org.apache.log4j.Logger

import scala.sys.process.ProcessLogger

/** Класс, создающий экземпляр ProcessLogger, для логирования подпроцессов, запускаемых библиотекой sys.
 */
class SysProcessLogger(logger: Logger) {

  /** Получить экземпляр ProcessLogger, пользующийся средствами логирования фреймворка Murphy.
   */
  protected def get: ProcessLogger = ProcessLogger(out(_), err(_))

  protected def out(s: => String): Unit = logger.info(s)
  protected def err(s: => String): Unit = logger.error(s, null)
}

/** Объект-компаньон, предоставляющий экземпляр ProcessLogger, для логирования подпроцессов, запускаемых библиотекой sys.
 *
 * Пример:
 * {{{
 * import ru.sberbank.xops.murphy.core.log.SysProcessLogger
 * import scala.sys.process._
 *
 * class Example {
 *   "python hello.py".!(SysProcessLogger())
 * }
 * }}}
 */
object SysProcessLogger {

  /** Создать экземпляр ProcessLogger.
   * @return Экземпляр ProcessLogger.
   */
  def apply(): ProcessLogger = new SysProcessLogger(logger = SimpleLogger()).get

  def apply(logger: Logger): ProcessLogger = new SysProcessLogger(logger).get

}