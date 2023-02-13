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

import java.sql.ResultSet
import java.time.{ Instant, LocalDate, LocalDateTime, LocalTime }

import scala.collection.mutable.ListBuffer

/** Provides extension methods for `java.sql.ResultSet`. */
implicit class ResultSetMethods(resultSet: ResultSet) extends AnyVal:
  /** Gets column count. */
  def getColumnCount(): Int =
    resultSet.getMetaData.getColumnCount()

  /** Gets column labels. */
  def getColumnLabels(): Seq[String] =
    val metaData = resultSet.getMetaData()
    (1 to getColumnCount()).map(metaData.getColumnLabel).toSeq

  /**
   * Gets column value in current row.
   *
   * @tparam T type of value to return
   *
   * @param index column index
   */
  def get[T](index: Int)(using getValue: GetValueByIndex[T]): T =
    getValue(resultSet, index)

  /**
   * Gets column value in current row.
   *
   * @tparam T type of value to return
   *
   * @param label column label
   */
  def get[T](label: String)(using getValue: GetValueByLabel[T]): T =
    getValue(resultSet, label)

  /**
   * Gets column value in current row, or returns default if value is null.
   *
   * @tparam T type of value to return
   *
   * @param index column index
   * @param default default value
   */
  def getOrElse[T](index: Int, default: => T)(using getValue: GetValueByIndex[T]): T =
    getOption(index).getOrElse(default)

  /**
   * Gets column value in current row, or returns default if value is null.
   *
   * @tparam T type of value to return
   *
   * @param label column label
   * @param default default value
   */
  def getOrElse[T](label: String, default: => T)(using getValue: GetValueByLabel[T]): T =
    getOption(label).getOrElse(default)

  /**
   * Gets column value in current row if value is not null.
   *
   * @tparam T type of value to return
   *
   * @param index column index
   */
  def getOption[T](index: Int)(using getValue: GetValueByIndex[T]): Option[T] =
    val value = getValue(resultSet, index)

    resultSet.wasNull match
      case true  => None
      case false => Option(value)

  /**
   * Gets column value in current row if value is not null.
   *
   * @tparam T type of value to return
   *
   * @param label column label
   */
  def getOption[T](label: String)(using getValue: GetValueByLabel[T]): Option[T] =
    val value = getValue(resultSet, label)

    resultSet.wasNull match
      case true  => None
      case false => Option(value)

  /** Gets column value as LocalDate. */
  def getLocalDate(index: Int): LocalDate =
    GetLocalDate(resultSet, index)

  /** Gets column value as LocalTime. */
  def getLocalTime(index: Int): LocalTime =
    GetLocalTime(resultSet, index)

  /** Gets column value as LocalDateTime. */
  def getLocalDateTime(index: Int): LocalDateTime =
    GetLocalDateTime(resultSet, index)

  /** Gets column value as Instant. */
  def getInstant(index: Int): Instant =
    GetInstant(resultSet, index)

  /** Gets column value as LocalDate. */
  def getLocalDate(label: String): LocalDate =
    GetLocalDate(resultSet, label)

  /** Gets column value as LocalTime. */
  def getLocalTime(label: String): LocalTime =
    GetLocalTime(resultSet, label)

  /** Gets column value as LocalDateTime. */
  def getLocalDateTime(label: String): LocalDateTime =
    GetLocalDateTime(resultSet, label)

  /** Gets column value as Instant. */
  def getInstant(label: String): Instant =
    GetInstant(resultSet, label)

  /**
   * Invokes supplied function for each remaining row of ResultSet.
   *
   * @param f function
   */
  def foreach(f: ResultSet => Unit): Unit =
    while resultSet.next() do
      f(resultSet)

  /**
   * Maps next row of ResultSet using supplied function.
   *
   * If the result set has another row, and if the supplied function's return
   * value is not null, then `Some` value is returned; otherwise, `None` is
   * returned.
   *
   * @param f map function
   */
  def mapNext[T](f: ResultSet => T): Option[T] =
    resultSet.next() match
      case true  => Option(f(resultSet))
      case false => None

  /**
   * Maps remaining rows of ResultSet using supplied function.
   *
   * @param f map function
   */
  def map[T](f: ResultSet => T): Seq[T] =
    fold(new ListBuffer[T]) { _ += f(_) }.toSeq

  /**
   * Maps remaining rows of ResultSet building a collection using elements
   * returned from map function.
   *
   * @param f map function
   */
  def flatMap[T](f: ResultSet => Iterable[T]): Seq[T] =
    fold(new ListBuffer[T]) { (buf, rs) =>
      f(rs).foreach(buf.+=)
      buf
    }.toSeq

  /**
   * Folds remaining rows of ResultSet to single value using given initial
   * value and binary operator.
   *
   * @param init initial value
   * @param op binary operator
   */
  def fold[T](init: T)(op: (T, ResultSet) => T): T =
    var res = init
    while resultSet.next() do
      res = op(res, resultSet)
    res
