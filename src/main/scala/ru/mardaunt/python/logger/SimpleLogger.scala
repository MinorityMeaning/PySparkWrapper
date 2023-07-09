package ru.mardaunt.python.logger

import org.apache.log4j.Logger

object SimpleLogger extends Logger("SimpleLogger") {

  override def info(message: Any): Unit = println(message)

  override def warn(message: Any): Unit = println(message)

  override def error(message: Any, t: Throwable): Unit = println(message, t)

  def apply(): SimpleLogger.type = this
}
