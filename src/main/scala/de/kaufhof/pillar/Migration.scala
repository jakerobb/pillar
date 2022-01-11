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

  def executeUpStatement(session: CqlSession, statementRegistry: StatementRegistry) {
    applyStatements(session, up)
    insertIntoAppliedMigrations(session, statementRegistry)
  }

  private def insertIntoAppliedMigrations(session: CqlSession, statementRegistry: StatementRegistry) {
    session.execute(statementRegistry.insertIntoAppliedMigrations().bind(authoredAt, description, Instant.now()).setConsistencyLevel(statementRegistry.consistencyLevel))
  }

  protected def applyStatements(session: CqlSession, statements: Seq[String]) {
    statements.foreach(s => {
      System.out.println(s)
      try {
        session.execute(s)
      } catch {
        case e: Exception => e.printStackTrace()
      }
    })
  }

  def executeDownStatement(session: CqlSession, statementRegistry: StatementRegistry): Unit

  protected def deleteFromAppliedMigrations(session: CqlSession, statementRegistry: StatementRegistry) {
    session.execute(statementRegistry.deleteFromAppliedMigrations().bind(authoredAt, description).setConsistencyLevel(statementRegistry.consistencyLevel))
  }
}

class IrreversibleMigration(val description: String, val authoredAt: Instant, val up: Seq[String]) extends Migration {
  def executeDownStatement(session: CqlSession, statementRegistry: StatementRegistry) {
    throw new IrreversibleMigrationException(this)
  }
}

class ReversibleMigrationWithNoOpDown(val description: String, val authoredAt: Instant, val up: Seq[String]) extends Migration {
  def executeDownStatement(session: CqlSession, statementRegistry: StatementRegistry) {
    deleteFromAppliedMigrations(session, statementRegistry)
  }
}

class ReversibleMigration(val description: String, val authoredAt: Instant, val up: Seq[String], val down: Seq[String]) extends Migration {
  def executeDownStatement(session: CqlSession, statementRegistry: StatementRegistry) {
    applyStatements(session, down)
    deleteFromAppliedMigrations(session, statementRegistry)
  }
}
