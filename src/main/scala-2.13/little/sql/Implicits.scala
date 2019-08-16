/*
 * Copyright 2019 Carlos Conyers
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

import java.sql.{ Connection, PreparedStatement, ResultSet, Statement, Date, Time, Timestamp, Types }
import java.time.{ LocalDate, LocalDateTime, LocalTime }

import javax.sql.DataSource

import scala.collection.mutable.ListBuffer
import scala.language.{ higherKinds, implicitConversions }
import scala.util.Try

import TimeConverters._

/** Provides implicits values and types. */
object Implicits {
  /** Converts String to InParam. */
  implicit def stringToInParam(value: String) = InParam(value)

  /** Converts Boolean to InParam. */
  implicit def booleanToInParam(value: Boolean) = InParam(value)

  /** Converts Byte to InParam. */
  implicit def byteToInParam(value: Byte) = InParam(value)

  /** Converts Short to InParam. */
  implicit def shortToInParam(value: Short) = InParam(value)

  /** Converts Int to InParam. */
  implicit def intToInParam(value: Int) = InParam(value)

  /** Converts Long to InParam. */
  implicit def longToInParam(value: Long) = InParam(value)

  /** Converts Float to InParam. */
  implicit def floatToInParam(value: Float) = InParam(value)

  /** Converts Double to InParam. */
  implicit def doubleToInParam(value: Double) = InParam(value)

  /** Converts BigDecimal to InParam. */
  implicit def bigDecimalToInParam(value: BigDecimal) = InParam(value)

  /** Converts Date to InParam. */
  implicit def dateToInParam(value: Date) = InParam(value)

  /** Converts Time to InParam. */
  implicit def timeToInParam(value: Time) = InParam(value)

  /** Converts Timestamp to InParam. */
  implicit def timestampToInParam(value: Timestamp) = InParam(value)

  /** Converts LocalDate to InParam. */
  implicit def localDateToInParam(value: LocalDate) = InParam(value)

  /** Converts LocalTime to InParam. */
  implicit def localTimeToInParam(value: LocalTime) = InParam(value)

  /** Converts LocalDateTime to InParam. */
  implicit def localDateTimeToInParam(value: LocalDateTime) = InParam(value)

  /** Converts Option[T] to InParam. */
  implicit def optionToInParam[T](value: Option[T])(implicit toInParam: T => InParam) =
    value.map(toInParam).getOrElse(InParam.NULL)

  /** Converts Any to InParam. */
  implicit def anyToInParam(value: Any): InParam =
    value match {
      case null             => InParam.NULL
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
      case x: Option[_]     => anyToInParam(x.getOrElse(null))
      case x: InParam       => x
      case x: Any           => throw new IllegalArgumentException(s"Cannot convert instance of ${x.getClass.getName} to little.sql.InParam")
    }

  /** Converts Seq[T] to Seq[InParam]. */
  implicit def seqToInParams[T](values: Seq[T])(implicit toInParam: T => InParam): Seq[InParam] =
    values.map(toInParam)

  /** Gets String from ResultSet. */
  implicit object GetString extends GetValue[String] {
    def apply(rs: ResultSet, index: Int): String = rs.getString(index)
    def apply(rs: ResultSet, label: String): String = rs.getString(label)
  }

  /** Gets Boolean from ResultSet. */
  implicit object GetBoolean extends GetValue[Boolean] {
    def apply(rs: ResultSet, index: Int): Boolean = rs.getBoolean(index)
    def apply(rs: ResultSet, label: String): Boolean = rs.getBoolean(label)
  }

  /** Gets Byte from ResultSet. */
  implicit object GetByte extends GetValue[Byte] {
    def apply(rs: ResultSet, index: Int): Byte = rs.getByte(index)
    def apply(rs: ResultSet, label: String): Byte = rs.getByte(label)
  }

  /** Gets Int from ResultSet. */
  implicit object GetInt extends GetValue[Int] {
    def apply(rs: ResultSet, index: Int): Int = rs.getInt(index)
    def apply(rs: ResultSet, label: String): Int = rs.getInt(label)
  }

  /** Gets Short from ResultSet. */
  implicit object GetShort extends GetValue[Short] {
    def apply(rs: ResultSet, index: Int): Short = rs.getShort(index)
    def apply(rs: ResultSet, label: String): Short = rs.getShort(label)
  }

  /** Gets Long from ResultSet. */
  implicit object GetLong extends GetValue[Long] {
    def apply(rs: ResultSet, index: Int): Long = rs.getLong(index)
    def apply(rs: ResultSet, label: String): Long = rs.getLong(label)
  }

  /** Gets Float from ResultSet. */
  implicit object GetFloat extends GetValue[Float] {
    def apply(rs: ResultSet, index: Int): Float = rs.getFloat(index)
    def apply(rs: ResultSet, label: String): Float = rs.getFloat(label)
  }

  /** Gets Double from ResultSet. */
  implicit object GetDouble extends GetValue[Double] {
    def apply(rs: ResultSet, index: Int): Double = rs.getDouble(index)
    def apply(rs: ResultSet, label: String): Double = rs.getDouble(label)
  }

  /** Gets BigDecimal from ResultSet. */
  implicit object GetBigDecimal extends GetValue[BigDecimal] {
    def apply(rs: ResultSet, index: Int): BigDecimal = rs.getBigDecimal(index)
    def apply(rs: ResultSet, label: String): BigDecimal = rs.getBigDecimal(label)
  }

  /** Gets Date from ResultSet. */
  implicit object GetDate extends GetValue[Date] {
    def apply(rs: ResultSet, index: Int): Date = rs.getDate(index)
    def apply(rs: ResultSet, label: String): Date = rs.getDate(label)
  }

  /** Gets Time from ResultSet. */
  implicit object GetTime extends GetValue[Time] {
    def apply(rs: ResultSet, index: Int): Time = rs.getTime(index)
    def apply(rs: ResultSet, label: String): Time = rs.getTime(label)
  }

  /** Gets Timestamp from ResultSet. */
  implicit object GetTimestamp extends GetValue[Timestamp] {
    def apply(rs: ResultSet, index: Int): Timestamp = rs.getTimestamp(index)
    def apply(rs: ResultSet, label: String): Timestamp = rs.getTimestamp(label)
  }

  /** Gets LocalDate from ResultSet. */
  implicit object GetLocalDate extends GetValue[LocalDate] {
    def apply(rs: ResultSet, index: Int): LocalDate = dateToLocalDate(rs.getDate(index))
    def apply(rs: ResultSet, label: String): LocalDate = dateToLocalDate(rs.getDate(label))
  }

  /** Gets LocalTime from ResultSet. */
  implicit object GetLocalTime extends GetValue[LocalTime] {
    def apply(rs: ResultSet, index: Int): LocalTime = timeToLocalTime(rs.getTime(index))
    def apply(rs: ResultSet, label: String): LocalTime = timeToLocalTime(rs.getTime(label))
  }

  /** Gets LocalDateTime from ResultSet. */
  implicit object GetLocalDateTime extends GetValue[LocalDateTime] {
    def apply(rs: ResultSet, index: Int): LocalDateTime = timestampToLocalDateTime(rs.getTimestamp(index))
    def apply(rs: ResultSet, label: String): LocalDateTime = timestampToLocalDateTime(rs.getTimestamp(label))
  }

  /** Provides extension methods to `javax.sql.DataSource`. */
  implicit class DataSourceType(private val dataSource: DataSource) extends AnyVal {
    /**
     * Creates Connection and passes it to supplied function. Connection is
     * closed on function's return.
     *
     * @param f function
     *
     * @return value from supplied function
     */
    def withConnection[T](f: Connection => T): T = {
      val conn = dataSource.getConnection()
      try f(conn)
      finally Try(conn.close())
    }

    /**
     * Creates Connection and passes it to supplied function. Connection is
     * closed on function's return.
     *
     * @param user database user
     * @param password database password
     * @param f function
     *
     * @return value from supplied function
     */
    def withConnection[T](user: String, password: String)(f: Connection => T): T = {
      val conn = dataSource.getConnection(user, password)
      try f(conn)
      finally Try(conn.close())
    }
  }

  /**
   * Provides extension methods to `java.sql.Connection`.
   *
   * {{{
   * import little.sql._
   * import Implicits._
   *
   * val connector = Connector("jdbc:h2:~/test", "sa", "s3cr3t", "org.h2.Driver")
   *
   * connector.withConnection { conn ⇒
   *   val statements = Seq(
   *     "drop table prog_lang if exists",
   *     "create table prog_lang (id int, name text)",
   *     "insert into prog_lang (id, name) values (1, 'basic'), (2, 'pascal'), (3, 'c')",
   *     "select * from prog_lang"
   *   )
   *
   *   statements.foreach { sql ⇒
   *     // Execute SQL and handle execution result accordingly
   *     conn.execute(sql) {
   *       // If update is executed print update count
   *       case Update(count) ⇒ println(s"Update Count: \$count")
   *       // If query is executed print values of each row in ResultSet
   *       case Query(resultSet) ⇒
   *         while (resultSet.next())
   *           printf("id: %d, name: %s%n", resultSet.getInt("id"), resultSet.getString("name"))
   *     }
   *   }
   * }
   * }}}
   */
  implicit class ConnectionType(private val connection: Connection) extends AnyVal {
    /**
     * Executes SQL and passes Execution to supplied function.
     *
     * @param sql SQL
     * @param params parameters
     * @param queryTimeout maximum number of seconds to wait for execution
     * @param maxRows maximum number of rows to return in result set
     * @param fetchSize number of result set rows to fetch on each retrieval
     *   from database
     * @param f function
     */
    def execute[T](sql: String, params: Seq[InParam] = Nil, queryTimeout: Int = 0, maxRows: Int = 0, fetchSize: Int = 0)(f: Execution => T): T =
      QueryBuilder(sql).params(params : _*)
        .queryTimeout(queryTimeout)
        .maxRows(maxRows)
        .fetchSize(fetchSize)
        .execute(f)(connection)

    /**
     * Executes query and passes ResultSet to supplied function.
     *
     * @param sql SQL query
     * @param params parameters
     * @param queryTimeout maximum number of seconds to wait for execution
     * @param maxRows maximum number of rows to return in result set
     * @param fetchSize number of result set rows to fetch on each retrieval
     *   from database
     * @param f function
     */
    def query[T](sql: String, params: Seq[InParam] = Nil, queryTimeout: Int = 0, maxRows: Int = 0, fetchSize: Int = 0)(f: ResultSet => T): T =
      QueryBuilder(sql)
        .params(params : _*)
        .queryTimeout(queryTimeout)
        .maxRows(maxRows)
        .fetchSize(fetchSize)
        .withResultSet(f)(connection)

    /**
     * Executes update and returns update count.
     *
     * @param sql SQL update
     * @param params parameters
     * @param queryTimeout maximum number of seconds to wait for execution
     */
    def update(sql: String, params: Seq[InParam] = Nil, queryTimeout: Int = 0): Long =
      QueryBuilder(sql)
        .params(params : _*)
        .queryTimeout(queryTimeout)
        .getUpdateCount(connection)

    /**
     * Executes batch of generated statements and returns results.
     *
     * @param generator SQL generator
     */
    def batch(generator: () => Iterable[String]): Array[Int] = {
      val stmt = connection.createStatement()

      try {
        generator().foreach(sql => stmt.addBatch(sql))
        stmt.executeBatch()
      } finally {
        Try(stmt.close())
      }
    }

    /**
     * Executes batch of statements with generated parameter values and returns
     * results.
     *
     * The generator must return sets of parameter values that satisfy the
     * supplied SQL.
     *
     * @param sql SQL from which prepared statement is created
     * @param generator parameter value generator
     */
    def batch(sql: String)(generator: () => Iterable[Seq[InParam]]): Array[Int] = {
      val stmt = connection.prepareStatement(sql)

      try {
        generator().foreach(params => stmt.addBatch(params))
        stmt.executeBatch()
      } finally {
        Try(stmt.close())
      }
    }

    /**
     * Executes query and invokes supplied function for each row of ResultSet.
     *
     * @param sql SQL query
     * @param params parameters
     * @param queryTimeout maximum number of seconds to wait for execution
     * @param maxRows maximum number of rows to return in result set
     * @param fetchSize number of result set rows to fetch on each retrieval
     *   from database
     * @param f function
     */
    def foreach(sql: String, params: Seq[InParam] = Nil, queryTimeout: Int = 0, maxRows: Int = 0, fetchSize: Int = 0)(f: ResultSet => Unit): Unit =
      QueryBuilder(sql)
        .params(params : _*)
        .queryTimeout(queryTimeout)
        .maxRows(maxRows)
        .fetchSize(fetchSize)
        .foreach(f)(connection)

    /**
     * Executes query and maps first row of ResultSet using supplied function.
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
        .params(params : _*)
        .queryTimeout(queryTimeout)
        .first(f)(connection)

    /**
     * Executes query and maps each row of ResultSet using supplied function.
     *
     * @param sql SQL query
     * @param params parameters
     * @param queryTimeout maximum number of seconds to wait for execution
     * @param maxRows maximum number of rows to return in result set
     * @param fetchSize number of result set rows to fetch on each retrieval
     *   from database
     * @param f map function
     */
    def map[T](sql: String, params: Seq[InParam] = Nil, queryTimeout: Int = 0, maxRows: Int = 0, fetchSize: Int = 0)(f: ResultSet => T): Seq[T] =
      QueryBuilder(sql)
        .params(params : _*)
        .queryTimeout(queryTimeout)
        .maxRows(maxRows)
        .fetchSize(fetchSize)
        .map(f)(connection)

    /**
     * Executes query and builds a collection using the elements mapped from
     * each row of ResultSet.
     *
     * @param sql SQL query
     * @param params parameters
     * @param queryTimeout maximum number of seconds to wait for execution
     * @param maxRows maximum number of rows to return in result set
     * @param fetchSize number of result set rows to fetch on each retrieval
     *   from database
     * @param f map function
     */
    def flatMap[T](sql: String, params: Seq[InParam] = Nil, queryTimeout: Int = 0, maxRows: Int = 0, fetchSize: Int = 0)(f: ResultSet => Iterable[T]): Seq[T] =
      QueryBuilder(sql)
        .params(params : _*)
        .queryTimeout(queryTimeout)
        .maxRows(maxRows)
        .fetchSize(fetchSize)
        .flatMap(f)(connection)

    /**
     * Creates Statement and passes it to supplied function. Statement is closed
     * on function's return.
     *
     * @param f function
     *
     * @return value from supplied function
     */
    def withStatement[T](f: Statement => T): T = {
      val stmt = connection.createStatement()
      try f(stmt)
      finally Try(stmt.close())
    }

    /**
     * Creates PreparedStatement and passes it to supplied function. Statement
     * is closed on function's return.
     *
     * @param sql SQL statement
     * @param f function
     *
     * @return value from supplied function
     */
    def withPreparedStatement[T](sql: String)(f: PreparedStatement => T): T = {
      val stmt = connection.prepareStatement(sql)
      try f(stmt)
      finally Try(stmt.close())
    }
  }

  /**
   * Provides extension methods to `java.sql.Statement`.
   *
   * @see [[PreparedStatementType]]
   */
  implicit class StatementType(private val statement: Statement) extends AnyVal {
    /**
     * Executes SQL and passes Execution to supplied function.
     *
     * @param sql SQL statement
     * @param f function
     */
    def execute[T](sql: String)(f: Execution => T): T =
      statement.execute(sql) match {
        case true =>
          val rs = statement.getResultSet
          try f(Query(rs))
          finally Try(rs.close())
        case false =>
          f(Update(statement.getUpdateCount))
      }

    /**
     * Executes query and passes ResultSet to supplied function.
     *
     * @param sql SQL query
     * @param f function
     */
    def query[T](sql: String)(f: ResultSet => T): T = {
      val rs = statement.executeQuery(sql)
      try f(rs)
      finally Try(rs.close())
    }

    /**
     * Executes query and invokes supplied function for each row of ResultSet.
     *
     * @param sql SQL query
     * @param f function
     */
    def foreach(sql: String)(f: ResultSet => Unit): Unit =
      query(sql) { _.foreach(f) }

    /**
     * Executes query and maps first row of ResultSet using supplied function.
     *
     * If the result set is not empty, and if the supplied function's return
     * value is not null, then `Some` value is returned; otherwise, `None` is
     * returned.
     *
     * @param sql SQL query
     * @param f function
     */
    def first[T](sql: String)(f: ResultSet => T): Option[T] = {
      Try(statement.setMaxRows(1))
      query(sql) { _.next(f) }
    }

    /**
     * Executes query and maps each row of ResultSet using supplied function.
     *
     * @param sql SQL query
     * @param params parameters
     * @param f map function
     */
    def map[T](sql: String)(f: ResultSet => T): Seq[T] =
      fold(sql)(new ListBuffer[T]) { _ += f(_) }.toSeq

    /**
     * Executes query and builds a collection using the elements mapped from
     * each row of ResultSet.
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

    private def fold[T](sql: String)(z: T)(op: (T, ResultSet) => T): T = {
      val rs = statement.executeQuery(sql)
      try rs.fold(z)(op)
      finally Try(rs.close())
    }
  }

  /**
   * Provides extension methods to `java.sql.PreparedStatement`.
   *
   * @see [[StatementType]]
   */
  implicit class PreparedStatementType(private val statement: PreparedStatement) extends AnyVal {
    /**
     * Executes statement with parameters and passes Execution to supplied
     * function.
     *
     * @param params parameters
     * @param f function
     */
    def execute[T](params: Seq[InParam])(f: Execution => T): T = {
      set(params)

      statement.execute() match {
        case true =>
          val rs = statement.getResultSet
          try f(Query(rs))
          finally Try(rs.close())
        case false =>
          f(Update(statement.getUpdateCount))
      }
    }

    /**
     * Executes query with parameters and passes ResultSet to supplied function.
     *
     * @param params parameters
     * @param f function
     */
    def query[T](params: Seq[InParam])(f: ResultSet => T): T = {
      set(params)

      val rs = statement.executeQuery()
      try f(rs)
      finally Try(rs.close())
    }

    /**
     * Executes update with parameters and returns update count.
     *
     * @param params parameters
     */
    def update(params: Seq[InParam]): Int = {
      set(params)
      statement.executeUpdate()
    }

    /**
     * Sets parameter at index to given value.
     *
     * @param index parameter index
     * @param value parameter value
     */
    def set(index: Int, value: InParam): Unit =
      if (value == null)
        statement.setNull(index, Types.VARCHAR)
      else
        value.isNull match {
          case true  => statement.setNull(index, value.sqlType)
          case false => statement.setObject(index, value.value, value.sqlType)
        }

    /**
     * Sets parameters.
     *
     * @param index parameter index
     * @param value parameter value
     */
    def set(params: Seq[InParam]): Unit =
      params.zipWithIndex.foreach {
        case (param, index) => set(index + 1, param)
      }

    /**
     * Adds parameters to batch of commands.
     *
     * @param params parameters
     */
    def addBatch(params: Seq[InParam]): Unit = {
      set(params)
      statement.addBatch()
    }

    /**
     * Executes query with parameters and invokes supplied function for each row
     * of ResultSet.
     *
     * @param params parameters
     * @param f function
     */
    def foreach(params: Seq[InParam])(f: ResultSet => Unit): Unit =
      query(params) { _.foreach(f) }

    /**
     * Executes query with parameters and maps first row of ResultSet using
     * supplied function.
     *
     * If the result set is not empty, and if the supplied function's return
     * value is not null, then `Some` value is returned; otherwise, `None` is
     * returned.
     *
     * @param params parameters
     * @param f map function
     */
    def first[T](params: Seq[InParam])(f: ResultSet => T): Option[T] = {
      Try(statement.setMaxRows(1))
      query(params) { _.next(f) }
    }

    /**
     * Executes query with parameters and maps each row of ResultSet using
     * supplied function.
     *
     * @param params parameters
     * @param f map function
     */
    def map[T](params: Seq[InParam])(f: ResultSet => T): Seq[T] =
      fold(params)(new ListBuffer[T]) {_ += f(_) }.toSeq

    /**
     * Executes query and builds a collection using the elements mapped from
     * each row of ResultSet.
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

    private def fold[T](params: Seq[InParam])(z: T)(op: (T, ResultSet) => T): T = {
      set(params)

      val rs = statement.executeQuery()
      try rs.fold(z)(op)
      finally Try(rs.close())
    }
  }

  /** Provides extension methods to `java.sql.ResultSet`. */
  implicit class ResultSetType(private val resultSet: ResultSet) extends AnyVal {
    /** Gets column count. */
    def getColumnCount(): Int = resultSet.getMetaData.getColumnCount

    /** Gets column labels. */
    def getColumnLabels(): Seq[String] = {
      val columnCount = getColumnCount
      val metaData = resultSet.getMetaData
      ((1 to columnCount) map { metaData getColumnLabel _ }).toSeq
    }

    /**
     * Gets column value in current row.
     *
     * @tparam T type of value to return
     *
     * @param index column index
     */
    def get[T](index: Int)(implicit getValue: GetValueByIndex[T]): T = getValue(resultSet, index)

    /**
     * Gets column value in current row.
     *
     * @tparam T type of value to return
     *
     * @param label column label
     */
    def get[T](label: String)(implicit getValue: GetValueByLabel[T]): T = getValue(resultSet, label)

    /**
     * Gets column value in current row, or returns default if value is null.
     *
     * @tparam T type of value to return
     *
     * @param index column index
     * @param default default value
     */
    def getOrElse[T](index: Int, default: => T)(implicit getValue: GetValueByIndex[T]): T =
      getOption(index)(getValue).getOrElse(default)

    /**
     * Gets column value in current row, or returns default if value is null.
     *
     * @tparam T type of value to return
     *
     * @param label column label
     * @param default default value
     */
    def getOrElse[T](label: String, default: => T)(implicit getValue: GetValueByLabel[T]): T =
      getOption(label)(getValue).getOrElse(default)

    /**
     * Gets column value in current row if value is not null.
     *
     * @tparam T type of value to return
     *
     * @param index column index
     */
    def getOption[T](index: Int)(implicit getValue: GetValueByIndex[T]): Option[T] = {
      val value = getValue(resultSet, index)

      resultSet.wasNull match {
        case true  => None
        case false => Option(value)
      }
    }

    /**
     * Gets column value in current row if value is not null.
     *
     * @tparam T type of value to return
     *
     * @param label column label
     */
    def getOption[T](label: String)(implicit getValue: GetValueByLabel[T]): Option[T] = {
      val value = getValue(resultSet, label)

      resultSet.wasNull match {
        case true  => None
        case false => Option(value)
      }
    }

    /** Gets column value as LocalDate. */
    def getLocalDate(index: Int): LocalDate = GetLocalDate(resultSet, index)

    /** Gets column value as LocalTime. */
    def getLocalTime(index: Int): LocalTime = GetLocalTime(resultSet, index)

    /** Gets column value as LocalDateTime. */
    def getLocalDateTime(index: Int): LocalDateTime = GetLocalDateTime(resultSet, index)

    /** Gets column value as LocalDate. */
    def getLocalDate(label: String): LocalDate = GetLocalDate(resultSet, label)

    /** Gets column value as LocalTime. */
    def getLocalTime(label: String): LocalTime = GetLocalTime(resultSet, label)

    /** Gets column value as LocalDateTime. */
    def getLocalDateTime(label: String): LocalDateTime = GetLocalDateTime(resultSet, label)

    /**
     * Invokes supplied function for each remaining row of ResultSet.
     *
     * @param f function
     */
    def foreach(f: ResultSet => Unit): Unit =
      while (resultSet.next())
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
    def next[T](f: ResultSet => T): Option[T] =
      if (resultSet.next())
        Option(f(resultSet))
      else None

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
    def fold[T](init: T)(op: (T, ResultSet) => T): T = {
      var res = init
      while (resultSet.next())
        res = op(res, resultSet)
      res
    }
  }
}
