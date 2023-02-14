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

import java.sql.{ Connection, PreparedStatement, ResultSet, Statement }

import scala.util.Try

/**
 * Provides extension methods for `java.sql.Connection`.
 *
 * {{{
 * import scala.language.implicitConversions
 *
 * import little.sql.{ *, given }
 *
 * val connector = Connector("jdbc:h2:~/test", "sa", "s3cr3t", "org.h2.Driver")
 *
 * connector.withConnection { conn =>
 *   val statements = Seq(
 *     "drop table prog_lang if exists",
 *     "create table prog_lang (id int, name text)",
 *     "insert into prog_lang (id, name) values (1, 'basic'), (2, 'pascal'), (3, 'c')",
 *     "select * from prog_lang"
 *   )
 *
 *   statements.foreach { sql =>
 *     // Execute SQL and handle execution result accordingly
 *     conn.execute(sql) {
 *       // If update is executed print update count
 *       case Update(count) ⇒ println(s"Update Count: \$count")
 *
 *       // If query is executed print values of each row in result set
 *       case Query(resultSet) =>
 *         while (resultSet.next())
 *           printf("id: %d, name: %s%n", resultSet.getInt("id"), resultSet.getString("name"))
 *     }
 *   }
 * }
 * }}}
 */
implicit class ConnectionMethods(connection: Connection) extends AnyVal:
  /**
   * Executes SQL and passes result to supplied function.
   *
   * @param sql SQL
   * @param params parameters
   * @param queryTimeout maximum number of seconds to wait for execution
   * @param maxRows maximum number of rows to return in result set
   * @param fetchSize number of result set rows to fetch on each retrieval from database
   * @param f function
   */
  def execute[T](sql: String, params: Seq[InParam] = Nil, queryTimeout: Int = 0, maxRows: Int = 0, fetchSize: Int = 0)(f: Execution => T): T =
    QueryBuilder(sql)
      .params(params)
      .queryTimeout(queryTimeout)
      .maxRows(maxRows)
      .fetchSize(fetchSize)
      .execute(f)(using connection)

  /**
   * Executes query and passes result set to supplied function.
   *
   * @param sql SQL query
   * @param params parameters
   * @param queryTimeout maximum number of seconds to wait for execution
   * @param maxRows maximum number of rows to return in result set
   * @param fetchSize number of result set rows to fetch on each retrieval from database
   * @param f function
   */
  def query[T](sql: String, params: Seq[InParam] = Nil, queryTimeout: Int = 0, maxRows: Int = 0, fetchSize: Int = 0)(f: ResultSet => T): T =
    QueryBuilder(sql)
      .params(params)
      .queryTimeout(queryTimeout)
      .maxRows(maxRows)
      .fetchSize(fetchSize)
      .query(f)(using connection)

  /**
   * Executes update and passes update count to supplied function.
   *
   * @param sql SQL update
   * @param params parameters
   * @param queryTimeout maximum number of seconds to wait for execution
   * @param f function
   */
  def update[T](sql: String, params: Seq[InParam] = Nil, queryTimeout: Int = 0)(f: Long => T): T =
    QueryBuilder(sql)
      .params(params)
      .queryTimeout(queryTimeout)
      .update(f)(using connection)

  /**
   * Executes update and returns update count.
   *
   * @param sql SQL update
   * @param params parameters
   * @param queryTimeout maximum number of seconds to wait for execution
   */
  def executeUpdate(sql: String, params: Seq[InParam] = Nil, queryTimeout: Int = 0): Long =
    update(sql, params, queryTimeout)(identity)

  /**
   * Executes batch of generated statements and returns results.
   *
   * @param generator SQL generator
   */
  def batch(generator: () => Iterable[String]): Array[Int] =
    val stmt = connection.createStatement()

    try
      generator().foreach(sql => stmt.addBatch(sql))
      stmt.executeBatch()
    finally
      Try(stmt.close())

  /**
   * Executes batch of statements with generated parameter values and returns
   * results.
   *
   * The generator must return sequence of parameter values that satisfy the
   * supplied SQL.
   *
   * @param sql SQL from which prepared statement is created
   * @param generator parameter value generator
   */
  def batch(sql: String)(generator: () => Iterable[Seq[InParam]]): Array[Int] =
    val stmt = connection.prepareStatement(sql)

    try
      generator().foreach(params => stmt.addBatch(params))
      stmt.executeBatch()
    finally
      Try(stmt.close())

  /**
   * Executes query and invokes supplied function for each row of result set.
   *
   * @param sql SQL query
   * @param params parameters
   * @param queryTimeout maximum number of seconds to wait for execution
   * @param maxRows maximum number of rows to return in result set
   * @param fetchSize number of result set rows to fetch on each retrieval from database
   * @param f function
   */
  def foreach(sql: String, params: Seq[InParam] = Nil, queryTimeout: Int = 0, maxRows: Int = 0, fetchSize: Int = 0)(f: ResultSet => Unit): Unit =
    QueryBuilder(sql)
      .params(params)
      .queryTimeout(queryTimeout)
      .maxRows(maxRows)
      .fetchSize(fetchSize)
      .foreach(f)(using connection)

  /**
   * Executes query and maps first row of result set using supplied function.
   *
   * If the result set is not empty, and if the supplied function's return
   * value is not null, then `Some` value is returned; otherwise, `None` is
   * returned.
   *
   * @param sql SQL query
   * @param params parameters
   * @param queryTimeout maximum number of seconds to wait for execution
   * @param f function
   *
   * @return value from supplied function
   */
  def first[T](sql: String, params: Seq[InParam] = Nil, queryTimeout: Int = 0)(f: ResultSet => T): Option[T] =
    QueryBuilder(sql)
      .params(params)
      .queryTimeout(queryTimeout)
      .first(f)(using connection)

  /**
   * Executes query and maps each row of result set using supplied function.
   *
   * @param sql SQL query
   * @param params parameters
   * @param queryTimeout maximum number of seconds to wait for execution
   * @param maxRows maximum number of rows to return in result set
   * @param fetchSize number of result set rows to fetch on each retrieval from database
   * @param f map function
   */
  def map[T](sql: String, params: Seq[InParam] = Nil, queryTimeout: Int = 0, maxRows: Int = 0, fetchSize: Int = 0)(f: ResultSet => T): Seq[T] =
    QueryBuilder(sql)
      .params(params)
      .queryTimeout(queryTimeout)
      .maxRows(maxRows)
      .fetchSize(fetchSize)
      .map(f)(using connection)

  /**
   * Executes query and builds a collection using the elements mapped from
   * each row of result set.
   *
   * @param sql SQL query
   * @param params parameters
   * @param queryTimeout maximum number of seconds to wait for execution
   * @param maxRows maximum number of rows to return in result set
   * @param fetchSize number of result set rows to fetch on each retrieval from database
   * @param f map function
   */
  def flatMap[T](sql: String, params: Seq[InParam] = Nil, queryTimeout: Int = 0, maxRows: Int = 0, fetchSize: Int = 0)(f: ResultSet => Iterable[T]): Seq[T] =
    QueryBuilder(sql)
      .params(params)
      .queryTimeout(queryTimeout)
      .maxRows(maxRows)
      .fetchSize(fetchSize)
      .flatMap(f)(using connection)

  /**
   * Creates Statement and passes it to supplied function. Statement is closed
   * on function's return.
   *
   * @param f function
   *
   * @return value from supplied function
   */
  def withStatement[T](f: Statement => T): T =
    val stmt = connection.createStatement()
    try f(stmt)
    finally Try(stmt.close())

  /**
   * Creates PreparedStatement and passes it to supplied function. Statement
   * is closed on function's return.
   *
   * @param sql SQL statement
   * @param f function
   *
   * @return value from supplied function
   */
  def withPreparedStatement[T](sql: String)(f: PreparedStatement => T): T =
    val stmt = connection.prepareStatement(sql)
    try f(stmt)
    finally Try(stmt.close())
