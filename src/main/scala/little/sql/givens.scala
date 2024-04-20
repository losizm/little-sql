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

import java.sql.{ ResultSet, Statement, Date, Time, Timestamp, Types }
import java.time.{ Instant, LocalDate, LocalDateTime, LocalTime }

import scala.language.{ higherKinds, implicitConversions }

import TimeConverters.*

/** Converts String to InParam. */
given stringToInParam: Conversion[String, InParam] = InParam(_)

/** Converts Boolean to InParam. */
given booleanToInParam: Conversion[Boolean, InParam] = InParam(_)

/** Converts Byte to InParam. */
given byteToInParam: Conversion[Byte, InParam] = InParam(_)

/** Converts Short to InParam. */
given shortToInParam: Conversion[Short, InParam] = InParam(_)

/** Converts Int to InParam. */
given intToInParam: Conversion[Int, InParam] = InParam(_)

/** Converts Long to InParam. */
given longToInParam: Conversion[Long, InParam] = InParam(_)

/** Converts Float to InParam. */
given floatToInParam: Conversion[Float, InParam] = InParam(_)

/** Converts Double to InParam. */
given doubleToInParam: Conversion[Double, InParam] = InParam(_)

/** Converts BigDecimal to InParam. */
given bigDecimalToInParam: Conversion[BigDecimal, InParam] = InParam(_)

/** Converts Date to InParam. */
given dateToInParam: Conversion[Date, InParam] = InParam(_)

/** Converts Time to InParam. */
given timeToInParam: Conversion[Time, InParam] = InParam(_)

/** Converts Timestamp to InParam. */
given timestampToInParam: Conversion[Timestamp, InParam] = InParam(_)

/** Converts LocalDate to InParam. */
given localDateToInParam: Conversion[LocalDate, InParam] = InParam(_)

/** Converts LocalTime to InParam. */
given localTimeToInParam: Conversion[LocalTime, InParam] = InParam(_)

/** Converts LocalDateTime to InParam. */
given localDateTimeToInParam: Conversion[LocalDateTime, InParam] = InParam(_)

/** Converts Instant to InParam. */
given instantToInParam: Conversion[Instant, InParam] = InParam(_)

/** Converts None to InParam. */
given noneToInParam: Conversion[None.type, InParam] = _ => InParam.Null

/** Converts Option[T] to InParam. */
given optionToInParam[T](using convert: Conversion[T, InParam]): Conversion[Option[T], InParam] with
  def apply(value: Option[T]) = value.map(convert).getOrElse(InParam.Null)

/** Converts Any to InParam. */
given anyToInParam: Conversion[Any, InParam] =
  case null             => InParam.Null
  case x: InParam       => x
  case x: String        => InParam(x)
  case x: Boolean       => InParam(x)
  case x: Byte          => InParam(x)
  case x: Short         => InParam(x)
  case x: Int           => InParam(x)
  case x: Long          => InParam(x)
  case x: Float         => InParam(x)
  case x: Double        => InParam(x)
  case x: BigDecimal    => InParam(x)
  case x: Date          => InParam(x)
  case x: Time          => InParam(x)
  case x: Timestamp     => InParam(x)
  case x: LocalDate     => InParam(x)
  case x: LocalTime     => InParam(x)
  case x: LocalDateTime => InParam(x)
  case x: Instant       => InParam(x)
  case x: Option[?]     => x.map(anyToInParam).getOrElse(InParam.Null)
  case x: Any           => throw IllegalArgumentException(s"Cannot convert instance of ${x.getClass.getName} to little.sql.InParam")

/** Converts Seq[T] to Seq[InParam]. */
given seqToSeqInParam[T](using convert: Conversion[T, InParam]): Conversion[Seq[T], Seq[InParam]] with
  def apply(values: Seq[T]) = values.map(convert)

/** Converts Map[String, T] to Seq[String, InParam]. */
given mapToMapInParam[T](using convert: Conversion[T, InParam]): Conversion[Map[String, T], Map[String, InParam]] with
  def apply(values: Map[String, T]) = values.map { (name, value) => name -> convert(value) }

/** Converts (String, T) to (String, InParam). */
given tupleToTupleInParam[T](using convert: Conversion[T, InParam]): Conversion[(String, T), (String, InParam)] with
  def apply(value: (String, T)) = value._1 -> convert(value._2)

/** Gets String from ResultSet using index. */
given getStringByIndex: GetValueByIndex[String] = _.getString(_)

/** Gets String from ResultSet using label. */
given getStringByLabel: GetValueByLabel[String] = _.getString(_)

/** Gets Boolean from ResultSet using index. */
given getBooleanByIndex: GetValueByIndex[Boolean] = _.getBoolean(_)

/** Gets Boolean from ResultSet using label. */
given getBooleanByLabel: GetValueByLabel[Boolean] = _.getBoolean(_)

/** Gets Byte from ResultSet using index. */
given getByteByIndex: GetValueByIndex[Byte] = _.getByte(_)

/** Gets Byte from ResultSet using label. */
given getByteByLabel: GetValueByLabel[Byte] = _.getByte(_)

/** Gets Int from ResultSet using index. */
given getIntByIndex: GetValueByIndex[Int] = _.getInt(_)

/** Gets Int from ResultSet using label. */
given getIntByLabel: GetValueByLabel[Int] = _.getInt(_)

/** Gets Short from ResultSet using index. */
given getShortByIndex: GetValueByIndex[Short] = _.getShort(_)

/** Gets Short from ResultSet using label. */
given getShortByLabel: GetValueByLabel[Short] = _.getShort(_)

/** Gets Long from ResultSet using index. */
given getLongByIndex: GetValueByIndex[Long] = _.getLong(_)

/** Gets Long from ResultSet using label. */
given getLongByLabel: GetValueByLabel[Long] = _.getLong(_)

/** Gets Float from ResultSet using index. */
given getFloatByIndex: GetValueByIndex[Float] = _.getFloat(_)

/** Gets Float from ResultSet using label. */
given getFloatByLabel: GetValueByLabel[Float] = _.getFloat(_)

/** Gets Double from ResultSet using index. */
given getDoubleByIndex: GetValueByIndex[Double] = _.getDouble(_)

/** Gets Double from ResultSet using label. */
given getDoubleByLabel: GetValueByLabel[Double] = _.getDouble(_)

/** Gets BigDecimal from ResultSet using index. */
given getBigDecimalByIndex: GetValueByIndex[BigDecimal] = _.getBigDecimal(_)

/** Gets BigDecimal from ResultSet using label. */
given getBigDecimalByLabel: GetValueByLabel[BigDecimal] = _.getBigDecimal(_)

/** Gets Date from ResultSet using index. */
given getDateByIndex: GetValueByIndex[Date] = _.getDate(_)

/** Gets Date from ResultSet using label. */
given getDateByLabel: GetValueByLabel[Date] = _.getDate(_)

/** Gets Time from ResultSet using index. */
given getTimeByIndex: GetValueByIndex[Time] = _.getTime(_)

/** Gets Time from ResultSet using label. */
given getTimeByLabel: GetValueByLabel[Time] = _.getTime(_)

/** Gets Timestamp from ResultSet using index. */
given getTimestampByIndex: GetValueByIndex[Timestamp] = _.getTimestamp(_)

/** Gets Timestamp from ResultSet using label. */
given getTimestampByLabel: GetValueByLabel[Timestamp] = _.getTimestamp(_)

/** Gets LocalDate from ResultSet using index. */
given getLocalDateByIndex: GetValueByIndex[LocalDate] = (rs, index) => dateToLocalDate(rs.getDate(index))

/** Gets LocalDate from ResultSet using label. */
given getLocalDateByLabel: GetValueByLabel[LocalDate] = (rs, label) => dateToLocalDate(rs.getDate(label))

/** Gets LocalTime from ResultSet using index. */
given getLocalTimeByIndex: GetValueByIndex[LocalTime] = (rs, index) => timeToLocalTime(rs.getTime(index))

/** Gets LocalTime from ResultSet using label. */
given getLocalTimeByLabel: GetValueByLabel[LocalTime] = (rs, label) => timeToLocalTime(rs.getTime(label))

/** Gets LocalDateTime from ResultSet using index. */
given getLocalDateTimeByIndex: GetValueByIndex[LocalDateTime] = (rs, index) => timestampToLocalDateTime(rs.getTimestamp(index))

/** Gets LocalDateTime from ResultSet using label. */
given getLocalDateTimeByLabel: GetValueByLabel[LocalDateTime] = (rs, label) => timestampToLocalDateTime(rs.getTimestamp(label))

/** Gets Instant from ResultSet using index. */
given getInstantByIndex: GetValueByIndex[Instant] = (rs, index) => timestampToInstant(rs.getTimestamp(index))

/** Gets Instant from ResultSet using label. */
given getInstantByLabel: GetValueByLabel[Instant] = (rs, label) => timestampToInstant(rs.getTimestamp(label))
