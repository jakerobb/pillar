package de.kaufhof.pillar

import com.datastax.oss.driver.api.core.CqlSession

import java.time.Instant

object Migration {
  def apply(description: String, authoredAt: Instant, up: Seq[String]): Migration = {
    new IrreversibleMigration(description, authoredAt, up)
  }

  def apply(description: String, authoredAt: Instant, up: Seq[String], down: Option[Seq[String]]): Migration = {
    down match {
      case Some(downStatement) =>
        new ReversibleMigration(description, authoredAt, up, downStatement)
      case None =>
        new ReversibleMigrationWithNoOpDown(description, authoredAt, up)
    }
  }
}

trait Migration {
  val description: String
  val authoredAt: Instant
  val up: Seq[String]

  def key: MigrationKey = MigrationKey(authoredAt, description)

  def authoredAfter(date: Instant): Boolean = {
    authoredAt.isAfter(date)
  }

  def authoredBefore(date: Instant): Boolean = {
    authoredAt.compareTo(date) <= 0
  }

  def executeUpStatement(session: CqlSession, statementRegistry: StatementRegistry, debug: Boolean) {
    applyStatements(session, statementRegistry, up, debug)
    insertIntoAppliedMigrations(session, statementRegistry)
  }

  protected def deleteFromAppliedMigrations(session: CqlSession, statementRegistry: StatementRegistry) {
    session.execute(statementRegistry.bindDeleteFromAppliedMigrations(authoredAt, description))
  }

  protected def applyStatements(session: CqlSession, statementRegistry: StatementRegistry, statements: Seq[String], debug: Boolean) {
    statements.foreach(s => {
      if (debug) {
        System.out.printf("Executing: %s\n", s)
      }
      try {
        val ss = statementRegistry.buildStatement(s)
        session.execute(ss)
      } catch {
        case e: Exception =>
          e.printStackTrace()
          throw e
      }
    })
  }

  def executeDownStatement(session: CqlSession, statementRegistry: StatementRegistry, debug: Boolean): Unit

  private def insertIntoAppliedMigrations(session: CqlSession, statementRegistry: StatementRegistry) {
    session.execute(statementRegistry.bindInsertIntoAppliedMigrations(authoredAt, description, Instant.now()))
  }
}

class IrreversibleMigration(val description: String, val authoredAt: Instant, val up: Seq[String]) extends Migration {
  def executeDownStatement(session: CqlSession, statementRegistry: StatementRegistry, debug: Boolean) {
    throw new IrreversibleMigrationException(this)
  }
}

class ReversibleMigrationWithNoOpDown(val description: String, val authoredAt: Instant, val up: Seq[String]) extends Migration {
  def executeDownStatement(session: CqlSession, statementRegistry: StatementRegistry, debug: Boolean) {
    deleteFromAppliedMigrations(session, statementRegistry)
  }
}

class ReversibleMigration(val description: String, val authoredAt: Instant, val up: Seq[String], val down: Seq[String]) extends Migration {
  def executeDownStatement(session: CqlSession, statementRegistry: StatementRegistry, debug: Boolean) {
    applyStatements(session, statementRegistry, down, debug)
    deleteFromAppliedMigrations(session, statementRegistry)
  }
}
