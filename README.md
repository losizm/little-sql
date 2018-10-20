# little-sql

The Scala library that provides extension methods to _java.sql_.

[![Maven Central](https://img.shields.io/maven-central/v/com.github.losizm/little-sql_2.12.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22com.github.losizm%22%20AND%20a:%22little-sql_2.12%22)

## Getting Started

To use **little-sql**, add it as a dependency to your project:

* sbt
```scala
libraryDependencies += "com.github.losizm" %% "little-sql" % "0.4.2"
```
* Gradle
```groovy
compile group: 'com.github.losizm', name: 'little-sql_2.12', version: '0.4.2'
```
* Maven
```xml
<dependency>
  <groupId>com.github.losizm</groupId>
  <artifactId>little-sql_2.12</artifactId>
  <version>0.4.2</version>
</dependency>
```

## A Taste of little-sql

Here's a taste of what **little-sql** has to offer.

### Getting Connection and Executing Statements

The example below demonstrates obtaining a database connection using `Connector`
and executing a sequence of arbitrary SQL statements. After executing each
statement, a subclass of `Execution` is passed to the supplied handler. The
handler will receive either an `Update` providing an update count or a `Query`
holding a `ResultSet`.

```scala
import java.sql.{ PreparedStatement, ResultSet }
import little.sql.{ Connector, Query, Update }
import little.sql.Implicits._ // Unleash the power

case class User(id: Int, name: String)

def getUser(rs: ResultSet): User = {
  User(rs.getInt("id"), rs.getString("name"))
}

// Define database connector
val connector = Connector("jdbc:h2:~/test", "gza", "1iquid5w0rd5", "org.h2.Driver")

// Create connection, pass it to function, and close connection when done
connector.withConnection { conn =>
  val statements = Seq(
    "drop table if exists users",
    "create table users (id int, name varchar(32))",
    "insert into users (id, name) values (0, 'root'), (500, 'guest')",
    "select id, name from users",
    "drop table if exists passwords",
    "create table passwords (id int, password varchar(32))",
    "insert into passwords (id, password) values (0, 'repus'), (500, 'esuom')"
  )

  // Loop thru statements executing each one
  statements.foreach { sql =>
    println(s"Executing $sql ...")

    // Execute SQL, handle result, and close statement and result set (if any)
    conn.execute(sql) {
      // If update was executed, print number of rows affected
      case Update(count) => println(s"Rows affected: $count")

      // If query was executed, print first row of result set
      case Query(resultSet) => if (resultSet.next()) println(getUser(resultSet))
    }
  }
}
```

### Setting Parameters in Prepared Statement

If you're executing a statement with input parameters, you can pass the SQL
along with the parameters and allow the parameters to be set based on their
value types.

```scala
// Get connection, run update with parameters, and print number of rows inserted
connector.withConnection { conn =>
  val sql = "insert into users (id, name) values (?, ?)"
  val params = Seq(501, "ghostface")

  val count = conn.update(sql, params)
  println(s"Rows inserted: $count")
}
```

### Looping thru Result Set

**little-sql** adds a `forEachRow` method to `Connection`, `Statement`, and
`PreparedStatement`, which cuts down the boilerplate of executing a query and
looping through the `ResultSet`.

```scala
// Get connection, run select, and print each row in result set
connector.withConnection { conn =>
  conn.forEachRow("select * from users") { resultSet =>
    println(getUser(resultSet))
  }
}
```

### Mapping First Row of Result Set

At times, you may want only the first row in a result set. Perhaps you're running
a query knowing it will return at most one row. With pure Java, you use a
`Connection` to create a `Statement`, you execute the statement which returns
a `ResultSet`, and then you check the result set to see whether it has a row. If
so, you proceed to get values from the result set. When you're done, you close
the result set and statement.

With **little-sql**, ditch the ceremony. Get straight to the point.

```scala
val user: Option[User] = connector.withConnection { conn =>
  conn.mapFirstRow("select * from users where id = 501")(getUser)
}
```

### Getting Custom Values from Result Set

You can define an implementation of `GetValue` to retrieve custom values from a
`ResultSet`.

```scala
import little.sql.GetValue

case class Secret(text: String)

// Get Secret from ResultSet
implicit object GetSecret extends GetValue[Secret] {
  // Get value by index
  def apply(rs: ResultSet, index: Int): Secret =
    decrypt(rs.getString(index))

  // Get value by label
  def apply(rs: ResultSet, label: String): Secret =
    decrypt(rs.getString(label))

  private def decrypt(text: String): Secret =
    if (text == null) Secret("")
    else Secret(text.reverse)
}

// Get connection, run select, and print each user's password
connector.withConnection { conn =>
  val sql = """
    select u.name, p.password
    from passwords p join users u
    on p.id = u.id
  """

  conn.forEachRow(sql) { rs =>
    val name = rs.getString("name")
    val password = rs.get[Secret]("password")

    printf("%s's password is %s%n", name, password.text)
  }
}
```

### Setting Custom Values in Prepared Statement

You can define an implementation of `SetValue` to set custom values in a
`PreparedStatement`.

```scala
import little.sql.SetValue

// Set Secret in PreparedStatement
implicit object SetSecret extends SetValue[Secret] {
  def apply(stmt: PreparedStatement, index: Int, value: Secret): Unit =
    stmt.setString(index, encrypt(value))

  private def encrypt(value: Secret): String =
    value.text.reverse
}

// Get connection, run update with parameters, and print number of rows inserted
connector.withConnection { conn =>
  conn.withPreparedStatement("insert into passwords (id, password) values (?, ?)") { stmt =>
    stmt.setInt(1, 501)
    stmt.set(2, Secret("ir0nm@n"))

    val count = stmt.executeUpdate()
    println(s"Rows inserted: $count")
  }
}
```

### Working with Data Source

If you have access to an instance of `javax.sql.DataSource`, you can use its
extension methods for automatic resource management, similar to all `Connector`
examples above.

```scala
import javax.naming.InitialContext
// Adds methods to javax.sql.DataSource
import little.sql.Implicits.DataSourceType

val ctx = new InitialContext()
val dataSource = ctx.lookup("java:module/jdbc/UserDB")

// Get connection, run update with parameters, and print number of rows inserted
dataSource.withConnection { conn =>
  val sql = "insert into users (id, name) values (?, ?)"
  val params = Seq(502, "raekwon")

  val count = conn.update(sql, params)
  println(s"Rows inserted: $count")
}

// Or if you need to provide user and password
dataSource.withConnection("gza", "1iquid5w0rd5") { conn =>
  conn.forEachRow("select name from users") { resultSet =>
    println(resultSet.getString("name"))
  }
}
```

## License
**little-sql** is licensed under the Apache License, Version 2. See LICENSE file
for more information.
