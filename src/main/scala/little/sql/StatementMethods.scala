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

import java.sql.{ ResultSet, Statement }

import scala.collection.mutable.ListBuffer
import scala.util.Try

/**
 * Provides extension methods for `java.sql.Statement`.
 *
 * @see [[PreparedStatementMethods]]
 */
implicit class StatementMethods(statement: Statement) extends AnyVal:
  /**
   * Executes SQL and passes result to supplied function.
   *
   * @param sql SQL statement
   * @param f function
   */
  def execute[T](sql: String)(f: Execution => T): T =
    statement.execute(sql) match
      case true =>
        val rs = statement.getResultSet
        try f(Query(rs))
        finally Try(rs.close())
      case false =>
        f(Update(statement.getLargeUpdateCount))

  /**
   * Executes query and passes result set to supplied function.
   *
   * @param sql SQL query
   * @param f function
   */
  def query[T](sql: String)(f: ResultSet => T): T =
    val rs = statement.executeQuery(sql)
    try f(rs)
    finally Try(rs.close())

  /**
   * Executes update and passes update count to supplied function.
   *
   * @param sql SQL query
   * @param f function
   */
  def update[T](sql: String)(f: Long => T): T =
    f(statement.executeUpdate(sql))

  /**
   * Executes query and invokes supplied function for each row of result set.
   *
   * @param sql SQL query
   * @param f function
   */
  def foreach(sql: String)(f: ResultSet => Unit): Unit =
    query(sql) { _.foreach(f) }

  /**
   * Executes query and maps first row of result set using supplied function.
   *
   * If the result set is not empty, and if the supplied function's return
   * value is not null, then `Some` value is returned; otherwise, `None` is
   * returned.
   *
   * @param sql SQL query
   * @param f function
   */
  def first[T](sql: String)(f: ResultSet => T): Option[T] =
    Try(statement.setMaxRows(1))
    query(sql) { _.mapNext(f) }

  /**
   * Executes query and maps each row of result set using supplied function.
   *
   * @param sql SQL query
   * @param params parameters
   * @param f map function
   */
  def map[T](sql: String)(f: ResultSet => T): Seq[T] =
    fold(sql)(new ListBuffer[T]) { _ += f(_) }.toSeq

  /**
   * Executes query and builds a collection using the elements mapped from
   * each row of result set.
   *
   * @param sql SQL query
   * @param params parameters
   * @param f map function
   */
  def flatMap[T](sql: String)(f: ResultSet => Iterable[T]): Seq[T] =
    fold(sql)(new ListBuffer[T]) { (buf, rs) =>
      f(rs).foreach(buf.+=)
      buf
    }.toSeq

  private def fold[T](sql: String)(z: T)(op: (T, ResultSet) => T): T =
    val rs = statement.executeQuery(sql)
    try rs.fold(z)(op)
    finally Try(rs.close())
