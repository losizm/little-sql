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

import java.sql.{ Connection, Date, Time, Timestamp }
import java.time.{ Instant, LocalDate, LocalDateTime, LocalTime }

import scala.language.implicitConversions

class AnotherSqlSpec extends org.scalatest.flatspec.AnyFlatSpec:
  private val connector = Connector(s"jdbc:h2:${sys.props("java.io.tmpdir")}/AnotherSqlSpec", "sa", "", "org.h2.Driver")

  it should "drop table if exists" in connector.withConnection { conn =>
    conn.update("drop table test_values if exists") { count =>
      assert(count == 0)
    }
  }

  it should "create table" in connector.withConnection { conn =>
    conn.update("""
      create table test_values (
        id               varchar(40) unique,
        varchar_value    varchar(8),
        boolean_value    boolean,
        decimal_value    decimal(12,3),
        date_value       date,
        time_value       time,
        timestamp_value  timestamp(3)
      )
    """) { count =>
      assert(count == 0)
    }
  }

  it should "insert and select String value" in connector.withConnection { implicit conn =>
    val value   = "Hello"
    val default = ""
    val select  = QueryBuilder("select varchar_value as test_value from test_values where id = ?")

    assert(insert("StringTest1", value, "varchar") == 1)
    assert(select.params("StringTest1").first(_.getString(1)).contains(value))
    assert(select.params("StringTest1").first(_.get[String](1)).contains(value))
    assert(select.params("StringTest1").first(_.getOption[String](1)).contains(Some(value)))
    assert(select.params("StringTest1").first(_.getOrElse(1, default)).contains(value))
    assert(select.params("StringTest1").first(_.get[String]("test_value")).contains(value))
    assert(select.params("StringTest1").first(_.getOption[String]("test_value")).contains(Some(value)))
    assert(select.params("StringTest1").first(_.getOrElse("test_value", default)).contains(value))

    assert(insert("StringTest2", InParam.Null, "varchar") == 1)
    assert(select.params("StringTest2").first(_.getString(1)).isEmpty)
    assert(select.params("StringTest2").first(_.get[String](1)).isEmpty)
    assert(select.params("StringTest2").first(_.getOption[String](1)).contains(None))
    assert(select.params("StringTest2").first(_.getOrElse(1, default)).contains(default))
    assert(select.params("StringTest2").first(_.get[String]("test_value")).isEmpty)
    assert(select.params("StringTest2").first(_.getOption[String]("test_value")).contains(None))
    assert(select.params("StringTest2").first(_.getOrElse("test_value", default)).contains(default))
  }

  it should "insert and select Boolean value" in connector.withConnection { implicit conn =>
    val value   = true
    val default = false
    val select  = QueryBuilder("select boolean_value as test_value from test_values where id = ?")

    assert(insert("BooleanTest1", value, "boolean") == 1)
    assert(select.params("BooleanTest1").first(_.getBoolean(1)).get)
    assert(select.params("BooleanTest1").first(_.get[Boolean](1)).get)
    assert(select.params("BooleanTest1").first(_.getOption[Boolean](1)).contains(Some(true)))
    assert(select.params("BooleanTest1").first(_.getOrElse(1, default)).get)
    assert(select.params("BooleanTest1").first(_.get[Boolean]("test_value")).get)
    assert(select.params("BooleanTest1").first(_.getOption[Boolean]("test_value")).contains(Some(true)))
    assert(select.params("BooleanTest1").first(_.getOrElse("test_value", default)).get)

    assert(insert("BooleanTest2", InParam.Null, "boolean") == 1)
    assert(!select.params("BooleanTest2").first(_.getBoolean(1)).get)
    assert(!select.params("BooleanTest2").first(_.get[Boolean](1)).get)
    assert(select.params("BooleanTest2").first(_.getOption[Boolean](1)).contains(None))
    assert(!select.params("BooleanTest2").first(_.getOrElse(1, default)).get)
    assert(!select.params("BooleanTest2").first(_.get[Boolean]("test_value")).get)
    assert(select.params("BooleanTest2").first(_.getOption[Boolean]("test_value")).contains(None))
    assert(!select.params("BooleanTest2").first(_.getOrElse("test_value", default)).get)
  }

  it should "insert and select Byte value" in connector.withConnection { implicit conn =>
    val value   = 123.toByte
    val default = -1.toByte
    val select  = QueryBuilder("select decimal_value as test_value from test_values where id = ?")

    assert(insert("ByteTest1", value, "decimal") == 1)
    assert(select.params("ByteTest1").first(_.getByte(1)).contains(value))
    assert(select.params("ByteTest1").first(_.get[Byte](1)).contains(value))
    assert(select.params("ByteTest1").first(_.getOption[Byte](1)).contains(Some(value)))
    assert(select.params("ByteTest1").first(_.getOrElse(1, default)).contains(value))
    assert(select.params("ByteTest1").first(_.get[Byte]("test_value")).contains(value))
    assert(select.params("ByteTest1").first(_.getOption[Byte]("test_value")).contains(Some(value)))
    assert(select.params("ByteTest1").first(_.getOrElse("test_value", default)).contains(value))

    assert(insert("ByteTest2", InParam.Null, "decimal") == 1)
    assert(select.params("ByteTest2").first(_.getByte(1)).contains(0))
    assert(select.params("ByteTest2").first(_.get[Byte](1)).contains(0))
    assert(select.params("ByteTest2").first(_.getOption[Byte](1)).contains(None))
    assert(select.params("ByteTest2").first(_.getOrElse(1, default)).contains(default))
    assert(select.params("ByteTest2").first(_.get[Byte]("test_value")).contains(0))
    assert(select.params("ByteTest2").first(_.getOption[Byte]("test_value")).contains(None))
    assert(select.params("ByteTest2").first(_.getOrElse("test_value", default)).contains(default))
  }

  it should "insert and select Short value" in connector.withConnection { implicit conn =>
    val value   = 123.toShort
    val default = -1.toShort
    val select  = QueryBuilder("select decimal_value as test_value from test_values where id = ?")

    assert(insert("ShortTest1", value, "decimal") == 1)
    assert(select.params("ShortTest1").first(_.getShort(1)).contains(value))
    assert(select.params("ShortTest1").first(_.get[Short](1)).contains(value))
    assert(select.params("ShortTest1").first(_.getOption[Short](1)).contains(Some(value)))
    assert(select.params("ShortTest1").first(_.getOrElse(1, default)).contains(value))
    assert(select.params("ShortTest1").first(_.get[Short]("test_value")).contains(value))
    assert(select.params("ShortTest1").first(_.getOption[Short]("test_value")).contains(Some(value)))
    assert(select.params("ShortTest1").first(_.getOrElse("test_value", default)).contains(value))

    assert(insert("ShortTest2", InParam.Null, "decimal") == 1)
    assert(select.params("ShortTest2").first(_.getShort(1)).contains(0))
    assert(select.params("ShortTest2").first(_.get[Short](1)).contains(0))
    assert(select.params("ShortTest2").first(_.getOption[Short](1)).contains(None))
    assert(select.params("ShortTest2").first(_.getOrElse(1, default)).contains(default))
    assert(select.params("ShortTest2").first(_.get[Short]("test_value")).contains(0))
    assert(select.params("ShortTest2").first(_.getOption[Short]("test_value")).contains(None))
    assert(select.params("ShortTest2").first(_.getOrElse("test_value", default)).contains(default))
  }

  it should "insert and select Int value" in connector.withConnection { implicit conn =>
    val value   = 123
    val default = -1
    val select  = QueryBuilder("select decimal_value as test_value from test_values where id = ?")

    assert(insert("IntTest1", value, "decimal") == 1)
    assert(select.params("IntTest1").first(_.getInt(1)).contains(value))
    assert(select.params("IntTest1").first(_.get[Int](1)).contains(value))
    assert(select.params("IntTest1").first(_.getOption[Int](1)).contains(Some(value)))
    assert(select.params("IntTest1").first(_.getOrElse(1, default)).contains(value))
    assert(select.params("IntTest1").first(_.get[Int]("test_value")).contains(value))
    assert(select.params("IntTest1").first(_.getOption[Int]("test_value")).contains(Some(value)))
    assert(select.params("IntTest1").first(_.getOrElse("test_value", default)).contains(value))

    assert(insert("IntTest2", InParam.Null, "decimal") == 1)
    assert(select.params("IntTest2").first(_.getInt(1)).contains(0))
    assert(select.params("IntTest2").first(_.get[Int](1)).contains(0))
    assert(select.params("IntTest2").first(_.getOption[Int](1)).contains(None))
    assert(select.params("IntTest2").first(_.getOrElse(1, default)).contains(default))
    assert(select.params("IntTest2").first(_.get[Int]("test_value")).contains(0))
    assert(select.params("IntTest2").first(_.getOption[Int]("test_value")).contains(None))
    assert(select.params("IntTest2").first(_.getOrElse("test_value", default)).contains(default))
  }

  it should "insert and select Long value" in connector.withConnection { implicit conn =>
    val value   = 123L
    val default = -1L
    val select  = QueryBuilder("select decimal_value as test_value from test_values where id = ?")

    assert(insert("LongTest1", value, "decimal") == 1)
    assert(select.params("LongTest1").first(_.getLong(1)).contains(value))
    assert(select.params("LongTest1").first(_.get[Long](1)).contains(value))
    assert(select.params("LongTest1").first(_.getOption[Long](1)).contains(Some(value)))
    assert(select.params("LongTest1").first(_.getOrElse(1, default)).contains(value))
    assert(select.params("LongTest1").first(_.get[Long]("test_value")).contains(value))
    assert(select.params("LongTest1").first(_.getOption[Long]("test_value")).contains(Some(value)))
    assert(select.params("LongTest1").first(_.getOrElse("test_value", default)).contains(value))

    assert(insert("LongTest2", InParam.Null, "decimal") == 1)
    assert(select.params("LongTest2").first(_.getLong(1)).contains(0))
    assert(select.params("LongTest2").first(_.get[Long](1)).contains(0))
    assert(select.params("LongTest2").first(_.getOption[Long](1)).contains(None))
    assert(select.params("LongTest2").first(_.getOrElse(1, default)).contains(default))
    assert(select.params("LongTest2").first(_.get[Long]("test_value")).contains(0))
    assert(select.params("LongTest2").first(_.getOption[Long]("test_value")).contains(None))
    assert(select.params("LongTest2").first(_.getOrElse("test_value", default)).contains(default))
  }

  it should "insert and select Float value" in connector.withConnection { implicit conn =>
    val value   = 123.456.toFloat
    val default = -1.toFloat
    val select  = QueryBuilder("select decimal_value as test_value from test_values where id = ?")

    assert(insert("FloatTest1", value, "decimal") == 1)
    assert(select.params("FloatTest1").first(_.getFloat(1)).contains(value))
    assert(select.params("FloatTest1").first(_.get[Float](1)).contains(value))
    assert(select.params("FloatTest1").first(_.getOption[Float](1)).contains(Some(value)))
    assert(select.params("FloatTest1").first(_.getOrElse(1, default)).contains(value))
    assert(select.params("FloatTest1").first(_.get[Float]("test_value")).contains(value))
    assert(select.params("FloatTest1").first(_.getOption[Float]("test_value")).contains(Some(value)))
    assert(select.params("FloatTest1").first(_.getOrElse("test_value", default)).contains(value))

    assert(insert("FloatTest2", InParam.Null, "decimal") == 1)
    assert(select.params("FloatTest2").first(_.getFloat(1)).contains(0))
    assert(select.params("FloatTest2").first(_.get[Float](1)).contains(0))
    assert(select.params("FloatTest2").first(_.getOption[Float](1)).contains(None))
    assert(select.params("FloatTest2").first(_.getOrElse(1, default)).contains(default))
    assert(select.params("FloatTest2").first(_.get[Float]("test_value")).contains(0))
    assert(select.params("FloatTest2").first(_.getOption[Float]("test_value")).contains(None))
    assert(select.params("FloatTest2").first(_.getOrElse("test_value", default)).contains(default))
  }

  it should "insert and select Double value" in connector.withConnection { implicit conn =>
    val value   = 123.456
    val default = -1.0
    val select  = QueryBuilder("select decimal_value as test_value from test_values where id = ?")

    assert(insert("DoubleTest1", value, "decimal") == 1)
    assert(select.params("DoubleTest1").first(_.getDouble(1)).contains(value))
    assert(select.params("DoubleTest1").first(_.get[Double](1)).contains(value))
    assert(select.params("DoubleTest1").first(_.getOption[Double](1)).contains(Some(value)))
    assert(select.params("DoubleTest1").first(_.getOrElse(1, default)).contains(value))
    assert(select.params("DoubleTest1").first(_.get[Double]("test_value")).contains(value))
    assert(select.params("DoubleTest1").first(_.getOption[Double]("test_value")).contains(Some(value)))
    assert(select.params("DoubleTest1").first(_.getOrElse("test_value", default)).contains(value))

    assert(insert("DoubleTest2", InParam.Null, "decimal") == 1)
    assert(select.params("DoubleTest2").first(_.getDouble(1)).contains(0))
    assert(select.params("DoubleTest2").first(_.get[Double](1)).contains(0))
    assert(select.params("DoubleTest2").first(_.getOption[Double](1)).contains(None))
    assert(select.params("DoubleTest2").first(_.getOrElse(1, default)).contains(default))
    assert(select.params("DoubleTest2").first(_.get[Double]("test_value")).contains(0))
    assert(select.params("DoubleTest2").first(_.getOption[Double]("test_value")).contains(None))
    assert(select.params("DoubleTest2").first(_.getOrElse("test_value", default)).contains(default))
  }

  it should "insert and select BigDecimal value" in connector.withConnection { implicit conn =>
    val value   = BigDecimal(123.456)
    val default = BigDecimal(-1)
    val select  = QueryBuilder("select decimal_value as test_value from test_values where id = ?")

    assert(insert("BigDecimalTest1", value, "decimal") == 1)
    assert(select.params("BigDecimalTest1").first(_.getBigDecimal(1)).contains(value.bigDecimal))
    assert(select.params("BigDecimalTest1").first(_.get[BigDecimal](1)).contains(value))
    assert(select.params("BigDecimalTest1").first(_.getOption[BigDecimal](1)).contains(Some(value)))
    assert(select.params("BigDecimalTest1").first(_.getOrElse(1, default)).contains(value))
    assert(select.params("BigDecimalTest1").first(_.get[BigDecimal]("test_value")).contains(value))
    assert(select.params("BigDecimalTest1").first(_.getOption[BigDecimal]("test_value")).contains(Some(value)))
    assert(select.params("BigDecimalTest1").first(_.getOrElse("test_value", default)).contains(value))

    assert(insert("BigDecimalTest2", InParam.Null, "decimal") == 1)
    assert(select.params("BigDecimalTest2").first(_.getBigDecimal(1)).isEmpty)
    assert(select.params("BigDecimalTest2").first(_.get[BigDecimal](1)).isEmpty)
    assert(select.params("BigDecimalTest2").first(_.getOption[BigDecimal](1)).contains(None))
    assert(select.params("BigDecimalTest2").first(_.getOrElse(1, default)).contains(default))
    assert(select.params("BigDecimalTest2").first(_.get[BigDecimal]("test_value")).isEmpty)
    assert(select.params("BigDecimalTest2").first(_.getOption[BigDecimal]("test_value")).contains(None))
    assert(select.params("BigDecimalTest2").first(_.getOrElse("test_value", default)).contains(default))
  }

  it should "insert and select Date value" in connector.withConnection { implicit conn =>
    val value   = Date.valueOf("2021-09-24")
    val default = Date.valueOf("2021-01-21")
    val select  = QueryBuilder("select date_value as test_value from test_values where id = ?")

    assert(insert("DateTest1", value, "date") == 1)
    assert(select.params("DateTest1").first(_.getDate(1)).contains(value))
    assert(select.params("DateTest1").first(_.get[Date](1)).contains(value))
    assert(select.params("DateTest1").first(_.getOption[Date](1)).contains(Some(value)))
    assert(select.params("DateTest1").first(_.getOrElse(1, default)).contains(value))
    assert(select.params("DateTest1").first(_.get[Date]("test_value")).contains(value))
    assert(select.params("DateTest1").first(_.getOption[Date]("test_value")).contains(Some(value)))
    assert(select.params("DateTest1").first(_.getOrElse("test_value", default)).contains(value))

    assert(insert("DateTest2", InParam.Null, "date") == 1)
    assert(select.params("DateTest2").first(_.getDate(1)).isEmpty)
    assert(select.params("DateTest2").first(_.get[Date](1)).isEmpty)
    assert(select.params("DateTest2").first(_.getOption[Date](1)).contains(None))
    assert(select.params("DateTest2").first(_.getOrElse(1, default)).contains(default))
    assert(select.params("DateTest2").first(_.get[Date]("test_value")).isEmpty)
    assert(select.params("DateTest2").first(_.getOption[Date]("test_value")).contains(None))
    assert(select.params("DateTest2").first(_.getOrElse("test_value", default)).contains(default))
  }

  it should "insert and select Time value" in connector.withConnection { implicit conn =>
    val value   = Time.valueOf("12:15:30")
    val default = Time.valueOf("00:00:00")
    val select  = QueryBuilder("select time_value as test_value from test_values where id = ?")

    assert(insert("TimeTest1", value, "time") == 1)
    assert(select.params("TimeTest1").first(_.getTime(1)).contains(value))
    assert(select.params("TimeTest1").first(_.get[Time](1)).contains(value))
    assert(select.params("TimeTest1").first(_.getOption[Time](1)).contains(Some(value)))
    assert(select.params("TimeTest1").first(_.getOrElse(1, default)).contains(value))
    assert(select.params("TimeTest1").first(_.get[Time]("test_value")).contains(value))
    assert(select.params("TimeTest1").first(_.getOption[Time]("test_value")).contains(Some(value)))
    assert(select.params("TimeTest1").first(_.getOrElse("test_value", default)).contains(value))

    assert(insert("TimeTest2", InParam.Null, "time") == 1)
    assert(select.params("TimeTest2").first(_.getTime(1)).isEmpty)
    assert(select.params("TimeTest2").first(_.get[Time](1)).isEmpty)
    assert(select.params("TimeTest2").first(_.getOption[Time](1)).contains(None))
    assert(select.params("TimeTest2").first(_.getOrElse(1, default)).contains(default))
    assert(select.params("TimeTest2").first(_.get[Time]("test_value")).isEmpty)
    assert(select.params("TimeTest2").first(_.getOption[Time]("test_value")).contains(None))
    assert(select.params("TimeTest2").first(_.getOrElse("test_value", default)).contains(default))
  }

  it should "insert and select Timestamp value" in connector.withConnection { implicit conn =>
    val value   = Timestamp.valueOf("2021-09-24 12:15:30.789")
    val default = Timestamp.valueOf("2021-01-01 00:00:00.000")
    val select  = QueryBuilder("select timestamp_value as test_value from test_values where id = ?")

    assert(insert("TimestampTest1", value, "timestamp") == 1)
    assert(select.params("TimestampTest1").first(_.getTimestamp(1)).contains(value))
    assert(select.params("TimestampTest1").first(_.get[Timestamp](1)).contains(value))
    assert(select.params("TimestampTest1").first(_.getOption[Timestamp](1)).contains(Some(value)))
    assert(select.params("TimestampTest1").first(_.getOrElse(1, default)).contains(value))
    assert(select.params("TimestampTest1").first(_.get[Timestamp]("test_value")).contains(value))
    assert(select.params("TimestampTest1").first(_.getOption[Timestamp]("test_value")).contains(Some(value)))
    assert(select.params("TimestampTest1").first(_.getOrElse("test_value", default)).contains(value))

    assert(insert("TimestampTest2", InParam.Null, "timestamp") == 1)
    assert(select.params("TimestampTest2").first(_.getTimestamp(1)).isEmpty)
    assert(select.params("TimestampTest2").first(_.get[Timestamp](1)).isEmpty)
    assert(select.params("TimestampTest2").first(_.getOption[Timestamp](1)).contains(None))
    assert(select.params("TimestampTest2").first(_.getOrElse(1, default)).contains(default))
    assert(select.params("TimestampTest2").first(_.get[Timestamp]("test_value")).isEmpty)
    assert(select.params("TimestampTest2").first(_.getOption[Timestamp]("test_value")).contains(None))
    assert(select.params("TimestampTest2").first(_.getOrElse("test_value", default)).contains(default))
  }

  it should "insert and select LocalDate value" in connector.withConnection { implicit conn =>
    val value   = LocalDate.parse("2021-09-24")
    val default = LocalDate.parse("2021-01-01")
    val select  = QueryBuilder("select date_value as test_value from test_values where id = ?")

    assert(insert("LocalDateTest1", value, "date") == 1)
    assert(select.params("LocalDateTest1").first(_.getLocalDate(1)).contains(value))
    assert(select.params("LocalDateTest1").first(_.get[LocalDate](1)).contains(value))
    assert(select.params("LocalDateTest1").first(_.getOption[LocalDate](1)).contains(Some(value)))
    assert(select.params("LocalDateTest1").first(_.getOrElse(1, default)).contains(value))
    assert(select.params("LocalDateTest1").first(_.get[LocalDate]("test_value")).contains(value))
    assert(select.params("LocalDateTest1").first(_.getOption[LocalDate]("test_value")).contains(Some(value)))
    assert(select.params("LocalDateTest1").first(_.getOrElse("test_value", default)).contains(value))

    assert(insert("LocalDateTest2", InParam.Null, "date") == 1)
    assert(select.params("LocalDateTest2").first(_.getLocalDate(1)).isEmpty)
    assert(select.params("LocalDateTest2").first(_.get[LocalDate](1)).isEmpty)
    assert(select.params("LocalDateTest2").first(_.getOption[LocalDate](1)).contains(None))
    assert(select.params("LocalDateTest2").first(_.getOrElse(1, default)).contains(default))
    assert(select.params("LocalDateTest2").first(_.get[LocalDate]("test_value")).isEmpty)
    assert(select.params("LocalDateTest2").first(_.getOption[LocalDate]("test_value")).contains(None))
    assert(select.params("LocalDateTest2").first(_.getOrElse("test_value", default)).contains(default))
  }

  it should "insert and select LocalTime value" in connector.withConnection { implicit conn =>
    val value   = LocalTime.parse("12:15:30")
    val default = LocalTime.parse("00:00:00")
    val select  = QueryBuilder("select time_value as test_value from test_values where id = ?")

    assert(insert("LocalTimeTest1", value, "time") == 1)
    assert(select.params("LocalTimeTest1").first(_.getLocalTime(1)).contains(value))
    assert(select.params("LocalTimeTest1").first(_.get[LocalTime](1)).contains(value))
    assert(select.params("LocalTimeTest1").first(_.getOption[LocalTime](1)).contains(Some(value)))
    assert(select.params("LocalTimeTest1").first(_.getOrElse(1, default)).contains(value))
    assert(select.params("LocalTimeTest1").first(_.get[LocalTime]("test_value")).contains(value))
    assert(select.params("LocalTimeTest1").first(_.getOption[LocalTime]("test_value")).contains(Some(value)))
    assert(select.params("LocalTimeTest1").first(_.getOrElse("test_value", default)).contains(value))

    assert(insert("LocalTimeTest2", InParam.Null, "time") == 1)
    assert(select.params("LocalTimeTest2").first(_.getLocalTime(1)).isEmpty)
    assert(select.params("LocalTimeTest2").first(_.get[LocalTime](1)).isEmpty)
    assert(select.params("LocalTimeTest2").first(_.getOption[LocalTime](1)).contains(None))
    assert(select.params("LocalTimeTest2").first(_.getOrElse(1, default)).contains(default))
    assert(select.params("LocalTimeTest2").first(_.get[LocalTime]("test_value")).isEmpty)
    assert(select.params("LocalTimeTest2").first(_.getOption[LocalTime]("test_value")).contains(None))
    assert(select.params("LocalTimeTest2").first(_.getOrElse("test_value", default)).contains(default))
  }

  it should "insert and select LocalDateTime value" in connector.withConnection { implicit conn =>
    val value   = LocalDateTime.parse("2021-09-24T12:15:30.789")
    val default = LocalDateTime.parse("2021-01-01T00:00:00.000")
    val select  = QueryBuilder("select timestamp_value as test_value from test_values where id = ?")

    assert(insert("LocalDateTimeTest1", value, "timestamp") == 1)
    assert(select.params("LocalDateTimeTest1").first(_.getLocalDateTime(1)).contains(value))
    assert(select.params("LocalDateTimeTest1").first(_.get[LocalDateTime](1)).contains(value))
    assert(select.params("LocalDateTimeTest1").first(_.getOption[LocalDateTime](1)).contains(Some(value)))
    assert(select.params("LocalDateTimeTest1").first(_.getOrElse(1, default)).contains(value))
    assert(select.params("LocalDateTimeTest1").first(_.get[LocalDateTime]("test_value")).contains(value))
    assert(select.params("LocalDateTimeTest1").first(_.getOption[LocalDateTime]("test_value")).contains(Some(value)))
    assert(select.params("LocalDateTimeTest1").first(_.getOrElse("test_value", default)).contains(value))

    assert(insert("LocalDateTimeTest2", InParam.Null, "timestamp") == 1)
    assert(select.params("LocalDateTimeTest2").first(_.getLocalDateTime(1)).isEmpty)
    assert(select.params("LocalDateTimeTest2").first(_.get[LocalDateTime](1)).isEmpty)
    assert(select.params("LocalDateTimeTest2").first(_.getOption[LocalDateTime](1)).contains(None))
    assert(select.params("LocalDateTimeTest2").first(_.getOrElse(1, default)).contains(default))
    assert(select.params("LocalDateTimeTest2").first(_.get[LocalDateTime]("test_value")).isEmpty)
    assert(select.params("LocalDateTimeTest2").first(_.getOption[LocalDateTime]("test_value")).contains(None))
    assert(select.params("LocalDateTimeTest2").first(_.getOrElse("test_value", default)).contains(default))
  }

  it should "insert and select Instant value" in connector.withConnection { implicit conn =>
    val value   = Instant.parse("2021-09-24T12:15:30.789Z")
    val default = Instant.parse("2021-01-01T00:00:00.000Z")
    val select  = QueryBuilder("select timestamp_value as test_value from test_values where id = ?")

    assert(insert("InstantTest1", value, "timestamp") == 1)
    assert(select.params("InstantTest1").first(_.getInstant(1)).contains(value))
    assert(select.params("InstantTest1").first(_.get[Instant](1)).contains(value))
    assert(select.params("InstantTest1").first(_.getOption[Instant](1)).contains(Some(value)))
    assert(select.params("InstantTest1").first(_.getOrElse(1, default)).contains(value))
    assert(select.params("InstantTest1").first(_.get[Instant]("test_value")).contains(value))
    assert(select.params("InstantTest1").first(_.getOption[Instant]("test_value")).contains(Some(value)))
    assert(select.params("InstantTest1").first(_.getOrElse("test_value", default)).contains(value))

    assert(insert("InstantTest2", InParam.Null, "timestamp") == 1)
    assert(select.params("InstantTest2").first(_.getInstant(1)).isEmpty)
    assert(select.params("InstantTest2").first(_.get[Instant](1)).isEmpty)
    assert(select.params("InstantTest2").first(_.getOption[Instant](1)).contains(None))
    assert(select.params("InstantTest2").first(_.getOrElse(1, default)).contains(default))
    assert(select.params("InstantTest2").first(_.get[Instant]("test_value")).isEmpty)
    assert(select.params("InstantTest2").first(_.getOption[Instant]("test_value")).contains(None))
    assert(select.params("InstantTest2").first(_.getOrElse("test_value", default)).contains(default))
  }

  private def insert(id: String, value: InParam, typeName: String)(using conn: Connection): Int =
    val sql = s"insert into test_values (id, ${typeName}_value) values (?, ?)"

    conn.withPreparedStatement(sql) { stmt =>
      stmt.setString(1, id)
      stmt.set(2, value)
      stmt.executeUpdate()
    }
