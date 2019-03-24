/*
 * Copyright 2018 Carlos Conyers
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

import java.sql.Types

/** Defines value for input parameter. */
trait InParam {
  /** Gets value. */
  def value: Any

  /** Tests whether value is null. */
  def isNull: Boolean

  /** Gets SQL type of value as defined in `java.sql.Types.` */
  def sqlType: Int
}

/** Provides factory methods for InParam. */
object InParam {
  /** General-purpose instance of null input parameter. */
  val NULL: InParam = InParamImpl(null, true, Types.VARCHAR)

  /**
   * Creates InParam with supplied properties.
   *
   * @param value parameter value
   * @param isNull indicates whether value is null
   * @param sqlType value type as defined in `java.sql.Types`
   */
  def apply[T](value: T, isNull: Boolean, sqlType: Int): InParam =
    InParamImpl(value, isNull, sqlType)

  /**
   * Creates InParam with supplied value and type. The `isNull` property is set
   * according to value.
   *
   * @param value parameter value
   * @param sqlType value type as defined in `java.sql.Types`
   */
  def apply[T](value: T, sqlType: Int): InParam =
    InParamImpl(value, value == null, sqlType)
}

private case class InParamImpl(value: Any, isNull: Boolean, sqlType: Int) extends InParam

