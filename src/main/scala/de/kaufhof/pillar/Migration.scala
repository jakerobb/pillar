package de.kaufhof.pillar

import java.util.Date
import com.datastax.driver.core.Session
import com.datastax.driver.core.querybuilder.QueryBuilder

object Migration {
  def apply(description: String, authoredAt: Date, up: Seq[String]): Migration = {
    new IrreversibleMigration(description, authoredAt, up)
  }

  def apply(description: String, authoredAt: Date, up: Seq[String], down: Option[Seq[String]]): Migration = {
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
  val authoredAt: Date
  val up: Seq[String]

  def key: MigrationKey = MigrationKey(authoredAt, description)

  def authoredAfter(date: Date): Boolean = {
    authoredAt.after(date)
  }

  def authoredBefore(date: Date): Boolean = {
    authoredAt.compareTo(date) <= 0
  }

  def executeUpStatement(session: Session, appliedMigrationsTableName: String) {
    applyStatements(session, up)
    insertIntoAppliedMigrations(session, appliedMigrationsTableName)
  }

  def executeDownStatement(session: Session, appliedMigrationsTableName: String)

  protected def deleteFromAppliedMigrations(session: Session, appliedMigrationsTableName: String) {
    session.execute(QueryBuilder.
      delete().
      from(appliedMigrationsTableName).
      where(QueryBuilder.eq("authored_at", authoredAt)).
      and(QueryBuilder.eq("description", description))
    )
  }

  private def insertIntoAppliedMigrations(session: Session,appliedMigrationsTableName: String) {
    session.execute(QueryBuilder.
      insertInto(appliedMigrationsTableName).
      value("authored_at", authoredAt).
      value("description", description).
      value("applied_at", System.currentTimeMillis())
    )
  }

  protected def applyStatements(session: Session, statements: Seq[String]) {
    statements.foreach(s => {
      System.out.println(s)
      try {
        session.execute(s)
      } catch {
        case e: Exception => e.printStackTrace()
      }
    })
  }
}

class IrreversibleMigration(val description: String, val authoredAt: Date, val up: Seq[String]) extends Migration {
  def executeDownStatement(session: Session, appliedMigrationsTableName: String) {
    throw new IrreversibleMigrationException(this)
  }
}

class ReversibleMigrationWithNoOpDown(val description: String, val authoredAt: Date, val up: Seq[String]) extends Migration {
  def executeDownStatement(session: Session, appliedMigrationsTableName: String) {
    deleteFromAppliedMigrations(session, appliedMigrationsTableName)
  }
}

class ReversibleMigration(val description: String, val authoredAt: Date, val up: Seq[String], val down: Seq[String]) extends Migration {
  def executeDownStatement(session: Session, appliedMigrationsTableName: String) {
    applyStatements(session, down)
    deleteFromAppliedMigrations(session, appliedMigrationsTableName)
  }
}
