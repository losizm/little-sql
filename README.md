# little-sql

The Scala library that provides extension methods to _java.sql_.

[![Maven Central](https://img.shields.io/maven-central/v/com.github.losizm/little-sql_2.12.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22com.github.losizm%22%20AND%20a:%22little-sql_2.12%22)

## Getting Started

To use **little-sql**, add it as a dependency to your project:

* sbt
```scala
libraryDependencies += "com.github.losizm" %% "little-sql" % "0.8.0"
```
* Gradle
```groovy
compile group: 'com.github.losizm', name: 'little-sql_2.12', version: '0.8.0'
```
* Maven
```xml
<dependency>
  <groupId>com.github.losizm</groupId>
  <artifactId>little-sql_2.12</artifactId>
  <version>0.8.0</version>
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

  val count = conn.update(sql, Seq(501, "ghostface"))
  println(s"Rows inserted: $count")
}
```

### Looping thru Result Set

**little-sql** adds a `foreach` method to `Connection`, `Statement`, and
`PreparedStatement`, which cuts down the boilerplate of executing a query and
looping through the `ResultSet`.

```scala
// Get connection, run select, and print each row in result set
connector.withConnection { conn =>
  conn.foreach("select * from users") { rs =>
    println(getUser(rs))
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
  conn.first("select * from users where id = 501")(getUser)
}
```

### Mapping All Rows of Result Set

You can also map over an entire result set.

```scala
val users: Seq[User] = connector.withConnection { conn =>
  conn.map("select * from users")(getUser)
}
```

Or, if you're particular about which rows to map, you can `flatMap` the result
set instead.

```scala
val regUsers: Seq[User] = connector.withConnection { conn =>
  conn.flatMap("select * from users") {
    getUser(_) match {
      case User(_, "root") => None
      case user            => Some(user)
    }
  }
}
```

The above example is not the best of use cases. You could've instead written the
query to exclude the root user &ndash; but you get the point.

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

  conn.foreach(sql) { rs =>
    val name = rs.getString("name")
    val password = rs.get[Secret]("password")

    printf("%s's password is %s%n", name, password.text)
  }
}
```

### Setting Custom Values in Prepared Statement

To set a parameter to a custom value, you can define an implicit conversion to
convert the value to an `InParam`.

```scala
import scala.language.implicitConversions

import little.sql.InParam

// Convert Secret to InParam
implicit def secretToInParam(value: Secret) =
  if (value == null) InParam.NULL
  else InParam(value.text.reverse)

// Get connection, run update with parameters, and print number of rows inserted
connector.withConnection { conn =>
  val sql = "insert into passwords (id, password) values (?, ?)"

  val count = conn.update(sql, Seq(501, Secret("ironm@n")))
  println(s"Rows inserted: $count")
}
```

### Using QueryBuilder to Build and Execute Statements

`QueryBuilder` is an immutable structure that provides an interface for
incrementally building SQL statements. And, for executing them, it has versions
of the comprehension methods demonstrated thus far, such as `foreach`, `map`,
and `flatMap`, and adds `fold` to top it off.

```scala
import little.sql.QueryBuilder

connector.withConnection { implicit conn =>
  val sum = QueryBuilder("select * from users where id != ? and name != ?")
    .params(0, "root") // Set input parameters
    .queryTimeout(5)   // Set query timeout to 5 seconds
    .maxRows(10)       // Limit result set to 10 rows
    .fetchSize(10)     // Fetch 10 rows at a time
    // Fold over all rows summing the user IDs
    // Executes using implicit connection
    .fold(0) { (sum, rs) => sum + rs.getInt("id") }

  println(s"Sum: $sum")
}
```

Again, the example isn't the best use case, but never mind that.

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
  conn.foreach("select name from users") { rs =>
    println(rs.getString("name"))
  }
}
```

## API Documentation

See [scaladoc](https://losizm.github.io/little-sql/latest/api/little/sql/index.html)
for additional details.

## License
**little-sql** is licensed under the Apache License, Version 2. See LICENSE file
for more information.

