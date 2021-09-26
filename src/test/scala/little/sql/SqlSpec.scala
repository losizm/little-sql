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

import java.sql.{ Connection, ResultSet }

import scala.language.implicitConversions

import Implicits.{ *, given }

class SqlSpec extends org.scalatest.flatspec.AnyFlatSpec:
  private val connector = Connector(s"jdbc:h2:${sys.props("java.io.tmpdir")}/test", "sa", "", "org.h2.Driver")

  it should "drop table if it exists" in connector.withConnection { conn =>
    conn.update("drop table prog_lang if exists")
  }

  it should "create table" in connector.withConnection { conn =>
    conn.update("create table prog_lang (id int, name text, comments text)")
  }

  it should "insert records into table" in connector.withConnection { conn =>
    val count = conn.update("insert into prog_lang(id, name) values (1, 'basic'), (2, 'pascal'), (3, 'c')")
    assert(count == 3)
  }

  it should "select records from table" in connector.withConnection { conn =>
    conn.foreach("select * from prog_lang") { rs =>
      val id = rs.get[Int]("id")
      val name = rs.get[String]("name")
      val comments = rs.get[String]("comments")
    }
  }

  it should "select record having one column with null value" in connector.withConnection { conn =>
    conn.foreach("select * from prog_lang") { rs =>
      val id = rs.getOrElse("id", 0)
      val name = rs.getOrElse("name", "no name")
      val comments = rs.getOrElse("comments", "no comments")

      assert(id != 0)
      assert(name != "no name")
      assert(comments == "no comments")
    }
  }

  it should "insert records into table with null value" in connector.withConnection { conn =>
    conn.update("insert into prog_lang (id, name) values (?, ?)", Seq(None, "cobol"))
    val count: Option[Int] = conn.first("select count(*) from prog_lang where id is null") { rs =>
      rs.get[Int](1)
    }
    assert(count.getOrElse(0) == 1)
  }

  it should "execute batch of commands (with multiple SQL statements)" in connector.withConnection { conn =>
    conn.batch {
      () => List(
        "insert into prog_lang (id, name) values (11, 'java')",
        "insert into prog_lang (id, name) values (12, 'groovy')",
        "insert into prog_lang (id, name) values (13, 'scala')"
      )
    }

    val query = "select name from prog_lang where id = ?"
    val namer = (rs: ResultSet) => rs.getString("name")

    assert(conn.first(query, Seq(11))(namer).contains("java"))
    assert(conn.first(query, Seq(12))(namer).contains("groovy"))
    assert(conn.first(query, Seq(13))(namer).contains("scala"))
  }

  it should "execute batch of commands (with multiple sets of parameters)" in connector.withConnection { conn =>
    conn.batch("insert into prog_lang (id, name) values (?, ?)") {
      () => List(Seq(21, "java"), Seq(22, "groovy"), Seq(23, "scala"))
    }

    val query = "select name from prog_lang where id = ?"
    val namer = (rs: ResultSet) => rs.getString("name")

    assert(conn.first(query, Seq(21))(namer).contains("java"))
    assert(conn.first(query, Seq(22))(namer).contains("groovy"))
    assert(conn.first(query, Seq(23))(namer).contains("scala"))
  }

  it should "map rows" in connector.withConnection { conn =>
    val entries = conn.map("select id, name from prog_lang where id in (21, 22, 23) order by id") { rs =>
      rs.getInt("id") -> rs.getString("name")
    }

    assert(entries == Seq(21 -> "java", 22 -> "groovy", 23 -> "scala"))
  }

  it should "map and flatten rows" in connector.withConnection { conn =>
    val entries = conn.flatMap("select id, name from prog_lang where id in (21, 22, 23) order by id") { rs =>
      Seq(rs.getInt("id") -> rs.getString("name"))
    }

    assert(entries == Seq(21 -> "java", 22 -> "groovy", 23 -> "scala"))
  }

  it should "fold rows" in connector.withConnection { implicit conn =>
    val sum = QueryBuilder("select id, name from prog_lang where id in (21, 22, 23) order by id").fold(0) { (sum, rs) =>
      sum + rs.getInt("id")
    }

    assert(sum == 21 + 22 + 23)
  }
