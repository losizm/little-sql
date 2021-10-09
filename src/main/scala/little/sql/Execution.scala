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

/**
 * Represents result of either update or query. If update, result can be
 * obtained via `count`; otherwise, if query, result can be obtained via
 * `resultSet`.
 *
 * @see [[ConnectionExt.execute]],
 *  [[StatementExt.execute]],
 *  [[PreparedStatementExt.execute]],
 *  [[QueryBuilder.execute]]
 */
sealed abstract class Execution:
  /**
   * Returns `true` if this execution represents result of update;
   * otherwise, returns `false`.
   */
  def isUpdate: Boolean

  /**
   * Returns `true` if this execution represents result of query;
   * otherwise, returns `false`.
   */
  def isQuery: Boolean

  /**
   * Gets update count.
   *
   * @throws NoSuchElementException if this execution is not result of update
   */
  def count: Int

  /**
   * Gets result set.
   *
   * @throws NoSuchElementException if this execution is not result of query
   */
  def resultSet: ResultSet

/** Provides factory methods for Execution. */
object Execution:
  /** Creates Update with specified count. */
  def apply(count: Int) = Update(count)

  /** Creates Query with supplied result set. */
  def apply(resultSet: ResultSet) = Query(resultSet)

/**
 * Represents result of update.
 *
 * @see [[Query]], [[ConnectionExt.execute]],
 *  [[StatementExt.execute]],
 *  [[PreparedStatementExt.execute]],
 *  [[QueryBuilder.execute]]
 *
 * @param count update count
 */
final case class Update(count: Int) extends Execution:
  /** Returns `true`. */
  val isUpdate = true

  /** Returns `false`. */
  val isQuery = false

  /** Throws `NoSuchElementException`. */
  def resultSet: ResultSet = throw new NoSuchElementException("resultSet")

/**
 * Represents result of query.
 *
 * @see [[Update]], [[ConnectionExt.execute]],
 *  [[StatementExt.execute]],
 *  [[PreparedStatementExt.execute]],
 *  [[QueryBuilder.execute]]
 *
 * @param resultSet result set
 */
final case class Query(resultSet: ResultSet) extends Execution:
  /** Returns `false`. */
  val isUpdate = false

  /** Returns `true`. */
  val isQuery = true

  /** Throws `NoSuchElementException`. */
  def count: Int = throw new NoSuchElementException("count")
