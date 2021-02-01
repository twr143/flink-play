package org.iv.integrations.postgres

import java.sql.PreparedStatement

import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}

/**
 * Created by twr143 on 31.01.2021 at 15:31.
 */
object Sinks {
  val connection = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
    .withDriverName("org.postgresql.Driver")
    .withPassword("123")
    .withUrl("jdbc:postgresql://localhost:5432/flink-d")
    .withUsername("fl_user")
    .build()
  val jdbcOptions = JdbcExecutionOptions.builder().build()

  def logtableSink[T]= JdbcSink.sink(
    "insert into lognote (message) values (?)",
    new JdbcStatementBuilder[T] {
      def accept(a: PreparedStatement, u: T): Unit =
        a.setString(1, s"$u")
    }, jdbcOptions, connection)

  val cleanLogTableSink = JdbcSink.sink(
        "delete from lognote",
        new JdbcStatementBuilder[Int] {
          def accept(a: PreparedStatement, u: Int): Unit = {}
        }, connection)
}
