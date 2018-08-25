package little.sql

import java.sql.PreparedStatement

/**
 * Sets value in PreparedStatement.
 *
 * @see [[Implicits.PreparedStatementType]]
 */
trait SetValue[T] extends Any {
  /** Sets value in PreparedStatement. */
  def apply(stmt: PreparedStatement, index: Int, value: T): Unit
}
