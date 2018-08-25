package little.sql

import java.sql.ResultSet

/**
 * Gets value by index from ResultSet.
 *
 * @see [[GetValueByLabel]], [[Implicits.ResultSetType]]
 */
trait GetValueByIndex[T] extends Any {
  /** Gets value by index from ResultSet. */
  def apply(rs: ResultSet, index: Int): T
}

/**
 * Gets value by label from ResultSet.
 *
 * @see [[GetValueByIndex]], [[Implicits.ResultSetType]]
 */
trait GetValueByLabel[T] extends Any {
  /** Gets value by label from ResultSet. */
  def apply(rs: ResultSet, label: String): T
}

/**
 * Gets value from ResultSet.
 *
 * @see [[Implicits.ResultSetType]]
 */
trait GetValue[T] extends GetValueByIndex[T] with GetValueByLabel[T]
