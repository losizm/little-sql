# little-sql &ndash; Scala library for java.sql and javax.sql

**little-sql** is a Scala library that provides extension methods to _java.sql_
and _javax.sql_.

## Getting Started

To use **little-sql**, add it as a dependency to your project:

* sbt
```scala
libraryDependencies += "losizm" %% "little-sql" % "0.2.0"
```
* Gradle
```groovy
compile group: 'losizm', name: 'little-sql_2.12', version: '0.2.0'
```
* Maven
```xml
<dependency>
  <groupId>losizm</groupId>
  <artifactId>little-sql_2.12</artifactId>
  <version>0.2.0</version>
</dependency>
```

## A Taste of little-sql

Here's a taste of what **little-sql** has to offer.

```scala
import java.sql.ResultSet
import little.sql.{ Connector, Query, Update }
import little.sql.Implicits.ConnectionType

case class User(id: Int, name: String)

def getUser(rs: ResultSet): User = {
  User(rs.getInt("id"), rs.getString("name"))
}

// Define database connector
val connector = Connector("jdbc:h2:~/test", "sa", "s3cret", "org.h2.Driver")

// Create connection, pass it to function, and close connection when done
connector.withConnection { conn =>
  val statements = Seq(
    "drop table if exists users",
    "create table users (id int, name varchar(32))",
    "insert into users (id, name) values (0, 'root'), (500, 'guest')",
    "select id, name from users"
  )

  statements.foreach { sql =>
    println(s"Executing $sql ...")

    // Execute SQL, handle result, and close resources when done
    conn.execute(sql) {
      // If update statement was executed, print number of rows affected
      case Update(count) =>
        println(s"Rows affected: $count")

      // If query statement was executed, print each row of result set
      case Query(resultSet) =>
        while (resultSet.next())
          println(getUser(resultSet))
    }
  }
}

connector.withConnection { conn =>
  // Execute SQL using supplied parameters and return number of rows inserted
  val count = conn.update("insert into users (id, name) values (?, ?)", Seq(-1, "nobody"))
  println(s"Rows inserted: $count")

  // Execute SQL, print each user in result set, and close resources when done
  conn.forEachRow("select id, name from users") { resultSet =>
    println(getUser(resultSet))
  }
}

val user: Option[User] = connector.withConnection { conn =>
  val sql = "select id, name from users where id = ?"
  val params = Seq(500)

  // Return Some(User) if exists or None otherwise
  conn.mapFirstRow(sql, params)(getUser)
}
```

## License
**little-sql** is licensed under the Apache License, Version 2. See LICENSE file
for more information.
