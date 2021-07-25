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
 * Gets value by index from ResultSet.
 *
 * @see [[GetValueByLabel]], [[Implicits.ResultSetType ResultSetType]]
 */
trait GetValueByIndex[T]:
  /** Gets value by index from ResultSet. */
  def apply(rs: ResultSet, index: Int): T

/**
 * Gets value by label from ResultSet.
 *
 * @see [[GetValueByIndex]], [[Implicits.ResultSetType ResultSetType]]
 */
trait GetValueByLabel[T]:
  /** Gets value by label from ResultSet. */
  def apply(rs: ResultSet, label: String): T

/**
 * Gets value from ResultSet.
 *
 * @see [[Implicits.ResultSetType ResultSetType]]
 */
trait GetValue[T] extends GetValueByIndex[T], GetValueByLabel[T]
