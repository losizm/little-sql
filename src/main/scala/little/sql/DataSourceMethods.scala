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

import java.sql.Connection

import javax.sql.DataSource

import scala.util.Try

/** Provides extension methods for `javax.sql.DataSource`. */
implicit class DataSourceMethods(dataSource: DataSource) extends AnyVal:
  /**
   * Creates Connection and passes it to supplied function. Connection is
   * closed on function's return.
   *
   * @param f function
   *
   * @return value from supplied function
   */
  def withConnection[T](f: Connection => T): T =
    val conn = dataSource.getConnection()
    try f(conn)
    finally Try(conn.close())

  /**
   * Creates Connection and passes it to supplied function. Connection is
   * closed on function's return.
   *
   * @param user database user
   * @param password database password
   * @param f function
   *
   * @return value from supplied function
   */
  def withConnection[T](user: String, password: String)(f: Connection => T): T =
    val conn = dataSource.getConnection(user, password)
    try f(conn)
    finally Try(conn.close())
