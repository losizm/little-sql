/*
 * Copyright 2020 Carlos Conyers
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

import java.sql.{ Date, Time, Timestamp }
import java.time.{ LocalDate, LocalDateTime, LocalTime }

private object TimeConverters {
  // Date => LocalDate
  def dateToLocalDate(value: Date): LocalDate =
    if (value != null) value.toLocalDate else null

  // LocalDate => Date
  def localDateToDate(value: LocalDate): Date =
    if (value != null) Date.valueOf(value) else null

  // Time => LocalTime
  def timeToLocalTime(value: Time): LocalTime =
    if (value != null) value.toLocalTime else null

  // LocalTime => Time
  def localTimeToTime(value: LocalTime): Time =
    if (value != null) Time.valueOf(value) else null

  // Timestamp => LocalDateTime
  def timestampToLocalDateTime(value: Timestamp): LocalDateTime =
    if (value != null) value.toLocalDateTime else null

  // LocalDateTime => Timestamp
  def localDateTimeToTimestamp(value: LocalDateTime): Timestamp =
    if (value != null) Timestamp.valueOf(value) else null
}
