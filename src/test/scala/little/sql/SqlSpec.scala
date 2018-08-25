package little.sql

import org.scalatest.FlatSpec
import java.sql.Connection

import Implicits._

class SqlSpec extends FlatSpec {
  var conn: Connection = null
   
  "Connection" should "be created" in {
    conn = Connector(s"""jdbc:h2:${sys.props("java.io.tmpdir")}/test""", "sa", "", "org.h2.Driver").getConnection()
  }

  it should "drop table if it exists" in {
    conn.update("drop table prog_lang if exists")
  }

  it should "create table" in {
    conn.update("create table prog_lang (id int, name text, comments text)")
  }

  it should "insert records into table" in {
    val count = conn.update("insert into prog_lang(id, name) values (1, 'basic'), (2, 'pascal'), (3, 'c')")
    assert(count == 3)
  }

  it should "select records from table" in {
    conn.forEachRow("select * from prog_lang") { rs =>
      val id = rs.get[Int]("id")
      val name = rs.get[String]("name")
      val comments = rs.get[String]("comments")
    }
  }

  it should "select record having one column with null value" in {
    conn.forEachRow("select * from prog_lang") { rs =>
      val id = rs.get[Option[Int]]("id")
      val name = rs.get[Option[String]]("name")
      val comments = rs.get[Option[String]]("comments")

      assert(id.isDefined)
      assert(name.isDefined)
      assert(!comments.isDefined)
    }
  }

  it should "insert records into table with null value" in {
    conn.update("insert into prog_lang (id, name) values (?, ?)", Seq(None, "cobol"))
    val count: Option[Int] = conn.mapFirstRow("select count(*) from prog_lang where id is null") { rs =>
      rs.get[Int](1)
    }
    assert(count.getOrElse(0) == 1)
  }

  it should "be closed" in {
    conn.close()
  }
}
