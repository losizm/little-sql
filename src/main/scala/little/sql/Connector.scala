/*
 * Copyright 2019 Carlos Conyers
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

  /** Returns string representation of connector. */
  override def toString(): String = s"Connector(url=$url,driverClassName=$driverClassName)"
}
