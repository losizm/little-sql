/*
 * Copyright 2021 Carlos Conyers
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package little.sql

import java.io.PrintWriter
import java.net.{ URL, URLClassLoader }
import java.sql.{ Connection, Driver, SQLException, SQLFeatureNotSupportedException }
import java.time.Instant.now
import java.util.Properties
import java.util.logging.Logger

import javax.sql.DataSource


/**
 * Provides basic implementation of data source.
 *
 * @note If `driverClassPath` is empty, the driver is loaded using this
 * instance's class loader; otherwise, the driver is loaded using a class loader
 * constructed from supplied class path.
 *
 * @param url data source url
 * @param user data source user
 * @param password data source password
 * @param driverClassName fully qualified class name of JDBC driver
 * @param driverClassPath class path from which JDBC driver is loaded
 */
class Connector(url: String, user: String, password: String, driverClassName: String, driverClassPath: Seq[URL]) extends DataSource:
  /** Creates connector. */
  def this(url: String, user: String, password: String, driverClassName: String) =
    this(url, user, password, driverClassName, Nil)

  /** Creates connector. */
  def this(url: String, user: String, password: String, driverClassName: String, headDriverClassPath: URL, tailDriverClassPath: URL*) =
    this(url, user, password, driverClassName, headDriverClassPath +: tailDriverClassPath)

  private var logWriter: PrintWriter = null
  private var loginTimeout: Int = 0

  private lazy val driver: Driver = createDriver()

  /**
   * Gets parent logger.
   *
   * @throws SQLFeatureNotSupportedException &nbsp; unconditionally
   */
  def getParentLogger(): Logger =
    throw SQLFeatureNotSupportedException("Parent logger not supported")

  /** Gets log writer. */
  def getLogWriter(): PrintWriter =
    logWriter

  /** Sets log writer. */
  def setLogWriter(out: PrintWriter): Unit =
    logWriter = out

  /** Gets login timeout. */
  def getLoginTimeout(): Int =
    loginTimeout

  /** Sets login timeout. */
  def setLoginTimeout(seconds: Int): Unit =
    loginTimeout = seconds

  /** Gets connection. */
  def getConnection(): Connection =
    getConnection(user, password)

  /** Gets connection using supplied credentials. */
  def getConnection(user: String, password: String): Connection =
    val info = Properties()
    info.put("user", user)
    info.put("password", password)

    try
      val conn = driver.connect(url, info)
      log(s"Created connection for $user at $url")
      conn
    catch case err: Exception =>
      log(s"Failed to create connection for $user at $url => $err")
      throw err

  /** Tests for wrapper. */
  def isWrapperFor(kind: Class[?]): Boolean =
    kind.isInstance(this)

  /** Unwraps this to specified type. */
  def unwrap[T](kind: Class[T]): T =
    if ! isWrapperFor(kind) then
      throw SQLException(s"Cannot unwrap to instance of $kind")
    asInstanceOf

  /** Returns string representation. */
  override lazy val toString: String =
    StringBuilder("Connector(")
      .append("url=")
      .append(url)
      .append(",user=")
      .append(user)
      .append(",password=*")
      .append(",driverClassName=")
      .append(driverClassName)
      .append(",driverClassPath=")
      .append(driverClassPath.mkString("[", ",", "]"))
      .append(")")
      .toString

  private def createDriver(): Driver =
    classLoader()
      .loadClass(driverClassName)
      .getDeclaredConstructor()
      .newInstance()
      .asInstanceOf

  private def classLoader(): ClassLoader =
    driverClassPath.nonEmpty match
      case true  => URLClassLoader(driverClassPath.toArray, null : ClassLoader)
      case false => getClass.getClassLoader

  private def log(message: String): Unit =
    val writer = logWriter

    if writer != null then
      writer.printf("%n%s - %s%n", now(), message)
