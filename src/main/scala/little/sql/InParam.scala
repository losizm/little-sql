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

import java.sql.{ Date, Time, Timestamp, Types }
import java.time.{ LocalDate, LocalDateTime, LocalTime }

import TimeConverters._

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

  /** Creates InParam from String. */
  def apply(value: String): InParam = apply(value, Types.VARCHAR)

  /** Creates InParam from Boolean. */
  def apply(value: Boolean): InParam = apply(value, Types.BOOLEAN)

  /** Creates InParam from Byte. */
  def apply(value: Byte): InParam = apply(value, Types.TINYINT)

  /** Creates InParam from Short. */
  def apply(value: Short): InParam = apply(value, Types.SMALLINT)

  /** Creates InParam from Int. */
  def apply(value: Int): InParam = apply(value, Types.INTEGER)

  /** Creates InParam from Long. */
  def apply(value: Long): InParam = apply(value, Types.BIGINT)

  /** Creates InParam from Float. */
  def apply(value: Float): InParam = apply(value, Types.FLOAT)

  /** Creates InParam from Double. */
  def apply(value: Double): InParam = apply(value, Types.DOUBLE)

  /** Creates InParam from BigDecimal. */
  def apply(value: BigDecimal): InParam =
    apply(if (value != null) value.bigDecimal else null, Types.DECIMAL)

  /** Creates InParam from Date. */
  def apply(value: Date): InParam = apply(value, Types.DATE)

  /** Creates InParam from Time. */
  def apply(value: Time): InParam = apply(value, Types.TIME)

  /** Creates InParam from Timestamp. */
  def apply(value: Timestamp): InParam = apply(value, Types.TIMESTAMP)

  /** Creates InParam from LocalDate. */
  def apply(value: LocalDate): InParam = apply(localDateToDate(value), Types.DATE)

  /** Creates InParam from LocalTime. */
  def apply(value: LocalTime): InParam = apply(localTimeToTime(value), Types.TIME)

  /** Creates InParam from LocalDateTime. */
  def apply(value: LocalDateTime): InParam = apply(localDateTimeToTimestamp(value), Types.TIMESTAMP)
}

private case class InParamImpl(value: Any, isNull: Boolean, sqlType: Int) extends InParam

