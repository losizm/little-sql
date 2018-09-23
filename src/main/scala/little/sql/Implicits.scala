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

import java.sql.{ Connection, PreparedStatement, ResultSet, Statement, Date, Time, Timestamp, Types }
import java.time.{ LocalDate, LocalDateTime, LocalTime }

import javax.sql.DataSource

import scala.language.higherKinds
import scala.util.Try

/** Provides implicits values and types. */
object Implicits {
  // java.sql.Date <=> java.time.LocalDate
  private def dateToLocalDate(value: Date): LocalDate = if (value != null) value.toLocalDate else null
  private def localDateToDate(value: LocalDate): Date = if (value != null) Date.valueOf(value) else null

  // java.sql.Time <=> java.time.LocalTime
  private def timeToLocalTime(value: Time): LocalTime = if (value != null) value.toLocalTime else null
  private def localTimeToTime(value: LocalTime): Time = if (value != null) Time.valueOf(value) else null

  // java.sql.Timestamp <=> java.time.LocalDateTime
  private def timestampToLocalDateTime(value: Timestamp): LocalDateTime = if (value != null) value.toLocalDateTime else null
  private def localDateTimeToTimestamp(value: LocalDateTime): Timestamp = if (value != null) Timestamp.valueOf(value) else null

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

  /** Gets Option[T] by index from ResultSet. */
  implicit def getOptionByIndex[T](implicit getValue: GetValueByIndex[T]): GetValueByIndex[Option[T]] =
    (rs, index) => {
      val value = getValue(rs, index)
      if (rs.wasNull) Option.empty[T] else Some(value)
    }

  /** Gets Option[T] by label from ResultSet. */
  implicit def getOptionByLabel[T](implicit getValue: GetValueByLabel[T]): GetValueByLabel[Option[T]] =
    (rs, label) => {
      val value = getValue(rs, label)
      if (rs.wasNull) Option.empty[T] else Some(value)
    }

  /** Sets String in PreparedStatement. */
  implicit val setString: SetValue[String] = (stmt, index, value) =>
    if (value != null) stmt.setString(index, value)
    else  stmt.setNull(index, Types.VARCHAR)

  /** Sets Boolean in PreparedStatement. */
  implicit val setBoolean: SetValue[Boolean] = (stmt, index, value) => stmt.setBoolean(index, value)

  /** Sets Byte in PreparedStatement. */
  implicit val setByte: SetValue[Byte] = (stmt, index, value) => stmt.setByte(index, value)

  /** Sets Int in PreparedStatement. */
  implicit val setInt: SetValue[Int] = (stmt, index, value) => stmt.setInt(index, value)

  /** Sets Short in PreparedStatement. */
  implicit val setShort: SetValue[Short] = (stmt, index, value) => stmt.setShort(index, value)

  /** Sets Long in PreparedStatement. */
  implicit val setLong: SetValue[Long] = (stmt, index, value) => stmt.setLong(index, value)

  /** Sets Float in PreparedStatement. */
  implicit val setFloat: SetValue[Float] = (stmt, index, value) => stmt.setFloat(index, value)

  /** Sets Double in PreparedStatement. */
  implicit val setDouble: SetValue[Double] = (stmt, index, value) => stmt.setDouble(index, value)

  /** Sets BigDecimal in PreparedStatement. */
  implicit val setBigDecimal: SetValue[BigDecimal] = (stmt, index, value) =>
    if (value != null) stmt.setBigDecimal(index, value.bigDecimal)
    else stmt.setNull(index, Types.DECIMAL)

  /** Sets Date in PreparedStatement. */
  implicit val setDate: SetValue[Date] = (stmt, index, value) =>
    if (value != null) stmt.setDate(index, value)
    else stmt.setNull(index, Types.DATE)

  /** Sets Time in PreparedStatement. */
  implicit val setTime: SetValue[Time] = (stmt, index, value) =>
    if (value != null) stmt.setTime(index, value)
    else stmt.setNull(index, Types.TIME)

  /** Sets Timestamp in PreparedStatement. */
  implicit val setTimestamp: SetValue[Timestamp] = (stmt, index, value) =>
    if (value != null) stmt.setTimestamp(index, value)
    else stmt.setNull(index, Types.TIMESTAMP)

  /** Sets LocalDate in PreparedStatement. */
  implicit val setLocalDate: SetValue[LocalDate] = (stmt, index, value) =>
    if (value != null) stmt.setDate(index, localDateToDate(value))
    else stmt.setNull(index, Types.DATE)

  /** Sets LocalTime in PreparedStatement. */
  implicit val setLocalTime: SetValue[LocalTime] = (stmt, index, value) =>
    if (value != null) stmt.setTime(index, localTimeToTime(value))
    else stmt.setNull(index, Types.TIME)

  /** Sets LocalDateTime in PreparedStatement. */
  implicit val setLocalDateTime: SetValue[LocalDateTime] = (stmt, index, value) =>
    if (value != null) stmt.setTimestamp(index, localDateTimeToTimestamp(value))
    else stmt.setNull(index, Types.TIMESTAMP)

  /** Sets Option[T] in PreparedStatement. */
  implicit def setOption[T](implicit setValue: SetValue[T]): SetValue[Option[T]] =
    (stmt, index, value) => value.fold(stmt.setNull(index, Types.VARCHAR)) { x => setValue(stmt, index, x) }

  /** Provides extension methods to {@code javax.sql.DataSource}. */
  implicit class DataSourceType(val dataSource: DataSource) extends AnyVal {
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
   * Provides extension methods to {@code java.sql.Connection}.
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
  implicit class ConnectionType(val connection: Connection) extends AnyVal {
    /**
     * Executes SQL and passes ResultSet to supplied function.
     *
     * @param sql SQL statement
     * @param params SQL parameters
     * @param maxRows maximum number of rows to return in result set
     * @param f function
     */
    def query(sql: String, params: Seq[Any] = Nil, maxRows: Int = 0)(f: ResultSet => Unit): Unit = {
      val stmt = connection.prepareStatement(sql)

      try {
        stmt.setMaxRows(maxRows)
        stmt.query(params)(f)
      } finally {
        Try(stmt.close())
      }
    }

    /**
     * Executes SQL and returns update count.
     *
     * @param sql SQL statement
     * @param params SQL parameters
     */
    def update(sql: String, params: Seq[Any] = Nil): Int = {
      val stmt = connection.prepareStatement(sql)

      try stmt.update(params)
      finally Try(stmt.close())
    }

    /**
     * Executes SQL and passes Execution to supplied function.
     *
     * @param sql SQL statement
     * @param params SQL parameters
     * @param maxRows maximum number of rows to return in result set
     * @param f function
     */
    def execute(sql: String, params: Seq[Any] = Nil, maxRows: Int = 0)(f: Execution => Unit): Unit = {
      val stmt = connection.prepareStatement(sql)

      try {
        stmt.setMaxRows(maxRows)
        stmt.execute(params)(f)
      } finally {
        Try(stmt.close())
      }
    }

    /**
     * Executes batch of commands and returns results.
     *
     * The supplied generator must return an iterable set of SQL statements.
     *
     * @param generator SQL generating function
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
     * Executes batch of commands and returns results.
     *
     * The supplied generator must return an iterable set of parameter values,
     * with each set satisfying the prepared statement as defined by
     * {@code sql}.
     *
     * @param sql SQL statement from which prepared statement is created
     * @param generator parameter generating function
     */
    def batch(sql: String)(generator: () => Iterable[Seq[Any]]): Array[Int] = {
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
     * @param sql SQL statement
     * @param params SQL parameters
     * @param maxRows maximum number of rows to return in result set
     * @param fetchSize number of rows to fetch on each retrieval from result set
     * @param f function
     */
    def forEachRow(sql: String, params: Seq[Any] = Nil, maxRows: Int = 0, fetchSize: Int = 0)(f: ResultSet => Unit): Unit =
      query(sql, params, maxRows) { rs =>
        rs.setFetchSize(fetchSize)
        rs.forEachRow(f)
      }

    /**
     * Executes query, invokes supplied function for first row of ResultSet, and
     * returns value from function. If query produced empty result set, then
     * supplied is not invoked, and {@code None} is returned.
     *
     * @param sql SQL statement
     * @param params SQL parameters
     * @param f function
     *
     * @return value from supplied function
     */
    def mapFirstRow[T](sql: String, params: Seq[Any] = Nil)(f: ResultSet => T): Option[T] =
      withPreparedStatement(sql) { _.mapFirstRow(params)(f) }

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
   * Provides extension methods to {@code java.sql.Statement}.
   *
   * @see [[PreparedStatementType]]
   */
  implicit class StatementType(val statement: Statement) extends AnyVal {
    /**
     * Executes SQL and passes ResultSet to supplied function.
     *
     * @param sql SQL statement
     * @param f function
     */
    def query(sql: String)(f: ResultSet => Unit): Unit = {
      val rs = statement.executeQuery(sql)

      try f(rs)
      finally Try(rs.close())
    }

    /**
     * Executes SQL and passes Execution to supplied function.
     *
     * @param sql SQL statement
     * @param f function
     */
    def execute(sql: String)(f: Execution => Unit): Unit =
      if (statement.execute(sql)) {
        val rs = statement.getResultSet

        try f(Query(rs))
        finally Try(rs.close())
      } else {
        f(Update(statement.getUpdateCount))
      }

    /**
     * Executes query and invokes supplied function for each row of ResultSet.
     *
     * @param sql SQL statement
     * @param f function
     */
    def forEachRow(sql: String)(f: ResultSet => Unit): Unit =
      query(sql) { _.forEachRow(f) }

    /**
     * Executes query and invokes supplied function for first row of
     * ResultSet.
     *
     * The function's return value is wrapped in {@code Some}. Or, if result set
     * is empty, the function is not invoked and {@code None} is returned.
     *
     * @param sql SQL statement
     * @param f function
     */
    def mapFirstRow[T](sql: String)(f: ResultSet => T): Option[T] = {
      var result: Option[T] = None
      query(sql) { rs => result = rs.mapNextRow(f) }
      result
    }
  }

  /**
   * Provides extension methods to {@code java.sql.PreparedStatement}.
   *
   * @see [[StatementType]]
   */
  implicit class PreparedStatementType(val statement: PreparedStatement) extends AnyVal {
    /**
     * Sets parameter at index to given value.
     *
     * This method can be used with type inference (i.e., without specifying
     * type parameter) as shown in example below.
     * {{{
     * import little.sql._
     * import Implicits._
     *
     * val id = 1
     * val email = "jane.doe@xyz.com"
     * val name: Option[String] = None
     *
     * statement.set(1, id)
     * statement.set(2, email)
     * statement.set(3, name) // Sets parameter value to null
     * }}}
     *
     * @tparam T type of value to set
     *
     * @param index parameter index
     * @param value parameter value
     */
    def set[T](index: Int, value: T)(implicit setValue: SetValue[T]): Unit =
      setValue(statement, index, value)

    /**
     * Executes statement with parameters and passes ResultSet to supplied
     * function.
     *
     * @param params statment parameters
     * @param f function
     */
    def query(params: Seq[Any])(f: ResultSet => Unit): Unit = {
      setParameters(params)

      val rs = statement.executeQuery()

      try f(rs)
      finally Try(rs.close())
    }

    /**
     * Executes statement with parameters and returns update count.
     *
     * @param params statement parameters
     */
    def update(params: Seq[Any]): Int = {
      setParameters(params)
      statement.executeUpdate()
    }

    /**
     * Executes statement with parameters and passes Execution to supplied
     * function.
     *
     * @param params statement parameters
     * @param f function
     */
    def execute(params: Seq[Any])(f: Execution => Unit): Unit = {
      setParameters(params)

      if (statement.execute()) {
        val rs = statement.getResultSet

        try f(Query(rs))
        finally Try(rs.close())
      } else {
        f(Update(statement.getUpdateCount))
      }
    }

    /**
     * Adds parameters to batch of commands.
     *
     * @param params statement parameters
     */
    def addBatch(params: Seq[Any]): Unit = {
      setParameters(params)
      statement.addBatch()
    }

    /**
     * Executes query with parameters and invokes supplied function for each row
     * of ResultSet.
     *
     * @param f function
     */
    def forEachRow(params: Seq[Any])(f: ResultSet => Unit): Unit =
      query(params) { _.forEachRow(f) }

    /**
     * Executes query with parameters and invokes supplied function for first
     * row of ResultSet.
     *
     * The function's return value is wrapped in {@code Some}. Or, if result set
     * is empty, the function is not invoked and {@code None} is returned.
     *
     * @param f function
     */
    def mapFirstRow[T](params: Seq[Any])(f: ResultSet => T): Option[T] = {
      var result: Option[T] = None

      query(params) { rs => result = rs.mapNextRow(f) }

      result
    }

    /**
     * Sets parameter to given {@code LocalDate}.
     *
     * @param index parameter index
     * @param value parameter value
     */
    def setLocalDate(index: Int, value: LocalDate): Unit =
      statement.setDate(index, Date.valueOf(value))

    /**
     * Sets parameter to given {@code LocalTime}.
     *
     * @param index parameter index
     * @param value parameter value
     */
    def setLocalTime(index: Int, value: LocalTime): Unit =
      statement.setTime(index, Time.valueOf(value))

    /**
     * Sets parameter to given {@code LocalDateTime}.
     *
     * @param index parameter index
     * @param value parameter value
     */
    def setLocalDateTime(index: Int, value: LocalDateTime): Unit =
      statement.setTimestamp(index, Timestamp.valueOf(value))

    private def setParameter(index: Int, value: Any): Unit =
      if (value == null) {
        statement.setNull(index, Types.VARCHAR)
      } else {
        value match {
          case x: String        => statement.setString(index, x)
          case x: Boolean       => statement.setBoolean(index, x)
          case x: Int           => statement.setInt(index, x)
          case x: Byte          => statement.setByte(index, x)
          case x: Short         => statement.setShort(index, x)
          case x: Long          => statement.setLong(index, x)
          case x: Float         => statement.setFloat(index, x)
          case x: Double        => statement.setDouble(index, x)
          case x: BigDecimal    => statement.setBigDecimal(index, x.bigDecimal)
          case x: Date          => statement.setDate(index, x)
          case x: Time          => statement.setTime(index, x)
          case x: Timestamp     => statement.setTimestamp(index, x)
          case x: LocalDate     => setLocalDate(index, x)
          case x: LocalTime     => setLocalTime(index, x)
          case x: LocalDateTime => setLocalDateTime(index, x)
          case x: Option[_]     => setParameter(index, x.getOrElse(null))
          case x: Any           => statement.setObject(index, x)
        }
      }

    private def setParameters(params: Seq[Any]): Unit =
      params.zipWithIndex.foreach {
        case (value, index) => setParameter(index + 1, value)
      }

  }

  /** Provides extension methods to {@code java.sql.ResultSet}. */
  implicit class ResultSetType(val resultSet: ResultSet) extends AnyVal {
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
     * Invokes supplied function for each row of ResultSet after first advancing
     * to next row.
     *
     * @param f function
     */
    def forEachRow(f: ResultSet => Unit): Unit =
      while (resultSet.next())
        f(resultSet)

    /**
     * Invokes supplied function for next row of ResultSet.
     *
     * The function's return value is wrapped in {@code Some}. Or, if there are
     * no more rows in result set, the function is not invoked and {@code None}
     * is returned.
     *
     * @param f function
     */
    def mapNextRow[T](f: ResultSet => T): Option[T] =
      if (resultSet.next())
        Some(f(resultSet))
      else None
  }
}
