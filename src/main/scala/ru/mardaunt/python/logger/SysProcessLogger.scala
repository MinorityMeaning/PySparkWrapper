package ru.mardaunt.python.logger

import org.apache.log4j.Logger

import scala.sys.process.ProcessLogger

/** A class that creates an instance of ProcessLogger, for logging subprocesses started by the system library.
 */
class SysProcessLogger(logger: Logger) {

  /** Get instance ProcessLogger.
   */
  protected def get: ProcessLogger = ProcessLogger(out(_), err(_))

  protected def out(s: => String): Unit = logger.info(s)
  protected def err(s: => String): Unit = logger.error(s, null)
}

/** A companion object representing an instance of ProcessLogger for logging subprocesses run by the sys library.
 *
 * Example:
 * {{{
 * import ru.mardaunt.python.logger.SysProcessLogger
 * import scala.sys.process._
 *
 * class Example {
 *   "python hello.py".!(SysProcessLogger())
 * }
 * }}}
 */
object SysProcessLogger {

  /** Create instance ProcessLogger.
   * @return Instance ProcessLogger.
   */
  def apply(): ProcessLogger = new SysProcessLogger(logger = SimpleLogger()).get

  def apply(logger: Logger): ProcessLogger = new SysProcessLogger(logger).get

}