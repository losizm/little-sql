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
import java.time.Instant.now as instant
import java.util.Properties
import java.util.logging.Logger

import javax.sql.DataSource

/**
 * Defines data source connector.
 *
 * @param url data source url
 * @param user data source user
 * @param password data source password
 * @param driverClassName JDBC driver class name
 * @param driverClassLoader JDBC driver class loader
 */
class Connector(url: String, user: String, password: String, driverClassName: String, driverClassLoader: ClassLoader) extends DataSource:
  /**
   * Creates connector.
   *
   * @param url data source url
   * @param user data source user
   * @param password data source password
   * @param driverClassName JDBC driver class name
   * @param driverClassPath JDBC driver class path
   *
   * @note If `driverClassPath` is empty, the driver is loaded using
   * `Connector`'s class loader
   */
  def this(url: String, user: String, password: String, driverClassName: String, driverClassPath: Seq[URL]) =
    this(
      url,
      user,
      password,
      driverClassName,
      driverClassPath.isEmpty match
        case true  => classOf[Connector].getClassLoader
        case false => URLClassLoader(driverClassPath.toArray, classOf[Connector].getClassLoader)
    )

  /**
   * Creates connector.
   *
   * @param url data source url
   * @param user data source user
   * @param password data source password
   * @param driverClassName JDBC driver class name
   */
  def this(url: String, user: String, password: String, driverClassName: String) =
    this(url, user, password, driverClassName, classOf[Connector].getClassLoader)

  if url == null || user == null || password == null || driverClassName == null || driverClassLoader == null then
    throw NullPointerException()

  private var logWriter: PrintWriter = null
  private var loginTimeout: Int = 0

  private lazy val driver: Driver = createDriver()

  /**
   * Gets parent logger.
   *
   * @throws java.sql.SQLFeatureNotSupportedException always
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

  /** Unwraps connector to specified type. */
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
      .append(")")
      .toString

  private def createDriver(): Driver =
    driverClassLoader
      .loadClass(driverClassName)
      .getConstructor()
      .newInstance()
      .asInstanceOf

  private def log(message: String): Unit =
    Option(logWriter)
      .foreach { _.printf("%n%s - %s%n", instant(), message) }
