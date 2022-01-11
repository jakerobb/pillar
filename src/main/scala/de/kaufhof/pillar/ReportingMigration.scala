package de.kaufhof.pillar

import com.datastax.oss.driver.api.core.CqlSession

import java.time.Instant

class ReportingMigration(reporter: Reporter, wrapped: Migration) extends Migration {
  val description: String = wrapped.description
  val authoredAt: Instant = wrapped.authoredAt
  val up: Seq[String] = wrapped.up

  override def executeUpStatement(session: CqlSession, statementRegistry: StatementRegistry) {
    reporter.applying(wrapped)
    wrapped.executeUpStatement(session, statementRegistry)
  }

  def executeDownStatement(session: CqlSession, statementRegistry: StatementRegistry) {
    reporter.reversing(wrapped)
    wrapped.executeDownStatement(session, statementRegistry)
  }
}
