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

import java.sql.{ PreparedStatement, ResultSet, Date, Time, Timestamp, Types }
import java.time.{ Instant, LocalDate, LocalDateTime, LocalTime }

import scala.collection.mutable.ListBuffer
import scala.util.Try

/**
 * Provides extension methods for `java.sql.PreparedStatement`.
 *
 * @see [[StatementMethods]]
 */
implicit class PreparedStatementMethods(statement: PreparedStatement) extends AnyVal:
  /**
   * Sets parameters, executes statement, and passes result to supplied function.
   *
   * @param params parameters
   * @param f function
   */
  def execute[T](params: Seq[InParam])(f: Execution => T): T =
    set(params)

    statement.execute() match
      case true =>
        val rs = statement.getResultSet
        try f(Query(rs))
        finally Try(rs.close())
      case false =>
        f(Update(statement.getUpdateCount))

  /**
   * Sets parameters, executes query, and passes result set to supplied function.
   *
   * @param params parameters
   * @param f function
   */
  def query[T](params: Seq[InParam])(f: ResultSet => T): T =
    set(params)

    val rs = statement.executeQuery()
    try f(rs)
    finally Try(rs.close())

  /**
   * Sets parameters, executes update, and passes update count to supplied function.
   *
   * @param params parameters
   * @param f function
   */
  def update[T](params: Seq[InParam])(f: Long => T): T =
    set(params)
    f(statement.executeUpdate())

  /**
   * Sets parameter at index to given value.
   *
   * @param index parameter index
   * @param value parameter value
   */
  def set(index: Int, value: InParam): Unit =
    if value == null then
      statement.setNull(index, Types.NULL)
    else
      value.isNull match
        case true  => statement.setNull(index, value.sqlType)
        case false => statement.setObject(index, value.value, value.sqlType)

  /**
   * Sets parameters.
   *
   * @param index parameter index
   * @param value parameter value
   */
  def set(params: Seq[InParam]): Unit =
    params.zipWithIndex.foreach { (param, index) => set(index + 1, param) }

  /**
   * Adds parameters to batch of commands.
   *
   * @param params parameters
   */
  def addBatch(params: Seq[InParam]): Unit =
    set(params)
    statement.addBatch()

  /**
   * Executes query with parameters and invokes supplied function for each row
   * of result set.
   *
   * @param params parameters
   * @param f function
   */
  def foreach(params: Seq[InParam])(f: ResultSet => Unit): Unit =
    query(params) { _.foreach(f) }

  /**
   * Executes query with parameters and maps first row of result set using
   * supplied function.
   *
   * If the result set is not empty, and if the supplied function's return
   * value is not null, then `Some` value is returned; otherwise, `None` is
   * returned.
   *
   * @param params parameters
   * @param f map function
   */
  def first[T](params: Seq[InParam])(f: ResultSet => T): Option[T] =
    Try(statement.setMaxRows(1))
    query(params) { _.mapNext(f) }

  /**
   * Executes query with parameters and maps each row of result set using
   * supplied function.
   *
   * @param params parameters
   * @param f map function
   */
  def map[T](params: Seq[InParam])(f: ResultSet => T): Seq[T] =
    fold(params)(new ListBuffer[T]) {_ += f(_) }.toSeq

  /**
   * Executes query and builds a collection using the elements mapped from
   * each row of result set.
   *
   * @param params parameters
   * @param f map function
   */
  def flatMap[T](params: Seq[InParam])(f: ResultSet => Iterable[T]): Seq[T] =
    fold(params)(new ListBuffer[T]) { (buf, rs) =>
      f(rs).foreach(buf.+=)
      buf
    }.toSeq

  /**
   * Sets parameter to given `LocalDate`.
   *
   * @param index parameter index
   * @param value parameter value
   */
  def setLocalDate(index: Int, value: LocalDate): Unit =
    statement.setDate(index, Date.valueOf(value))

  /**
   * Sets parameter to given `LocalTime`.
   *
   * @param index parameter index
   * @param value parameter value
   */
  def setLocalTime(index: Int, value: LocalTime): Unit =
    statement.setTime(index, Time.valueOf(value))

  /**
   * Sets parameter to given `LocalDateTime`.
   *
   * @param index parameter index
   * @param value parameter value
   */
  def setLocalDateTime(index: Int, value: LocalDateTime): Unit =
    statement.setTimestamp(index, Timestamp.valueOf(value))

  /**
   * Sets parameter to given `Instant`.
   *
   * @param index parameter index
   * @param value parameter value
   */
  def setInstant(index: Int, value: Instant): Unit =
    statement.setTimestamp(index, Timestamp.from(value))

  private def fold[T](params: Seq[InParam])(z: T)(op: (T, ResultSet) => T): T =
    set(params)

    val rs = statement.executeQuery()
    try rs.fold(z)(op)
    finally Try(rs.close())
