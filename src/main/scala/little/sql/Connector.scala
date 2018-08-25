package little.sql

import java.sql.{ Connection, Driver }
import java.util.Properties

import scala.util.Try

/**
 * Creates database connections.
 *
 * @param url database url
 * @param user database user
 * @param password database password
 * @param driverClassName fully qualified class name of JDBC driver
 */
case class Connector(url: String, user: String, password: String, driverClassName: String) {
  /** Creates and returns Connection. */
  def getConnection(): Connection = {
    val driver =  Class.forName(driverClassName).newInstance().asInstanceOf[Driver]

    val info = new Properties
    info.put("user", user)
    info.put("password", password)

    driver.connect(url, info)
  }

  /**
   * Creates Connection and passes it to supplied function. Connection is closed
   * on function's return.
   *
   * @param f function
   *
   * @return value from supplied function
   */
  def withConnection[T](f: Connection => T): T = {
    val conn = getConnection()

    try f(conn)
    finally Try(conn.close())
  }
}
