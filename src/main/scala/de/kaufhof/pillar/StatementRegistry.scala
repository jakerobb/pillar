package de.kaufhof.pillar

import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement}
import com.datastax.oss.driver.api.core.{ConsistencyLevel, CqlSession}
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker

import java.time.Duration

sealed trait StatementRegistry {
  def consistencyLevel: ConsistencyLevel

  def bindSelectFromAppliedMigrations(): BoundStatement

  def bindInsertIntoAppliedMigrations(values: Object*): BoundStatement

  def bindDeleteFromAppliedMigrations(values: Object*): BoundStatement
}

class StatementPreparer(session: CqlSession, keyspace: String, appliedMigrationsTableName: String, consistency: ConsistencyLevel, timeoutDuration: Duration) extends StatementRegistry {
  lazy val consistencyLevel: ConsistencyLevel = consistency

  lazy val selectFromAppliedMigrations: PreparedStatement = {
    val selectFromAppliedMigrationsQuery = QueryBuilder.selectFrom(keyspace, appliedMigrationsTableName)
      .columns("authored_at", "description")

    session.prepare(selectFromAppliedMigrationsQuery.build)
  }

  lazy val insertIntoAppliedMigrations: PreparedStatement = {
    val insertIntoAppliedMigrationsQuery = QueryBuilder.insertInto(keyspace, appliedMigrationsTableName)
      .value("authored_at", bindMarker())
      .value("description", bindMarker())
      .value("applied_at", bindMarker())

    session.prepare(insertIntoAppliedMigrationsQuery.build)
  }

  lazy val deleteFromAppliedMigrations: PreparedStatement = {
    val deleteFromAppliedMigrationsQuery = QueryBuilder.deleteFrom(keyspace, appliedMigrationsTableName)
      .whereColumn("authored_at").isEqualTo(bindMarker())
      .whereColumn("description").isEqualTo(bindMarker())

    session.prepare(deleteFromAppliedMigrationsQuery.build)
  }

  override def bindSelectFromAppliedMigrations(): BoundStatement = {
    bind(selectFromAppliedMigrations)
  }

  override def bindInsertIntoAppliedMigrations(values: Object*): BoundStatement = {
    bind(insertIntoAppliedMigrations, values)
  }

  override def bindDeleteFromAppliedMigrations(values: Object*): BoundStatement = {
    bind(deleteFromAppliedMigrations, values)
  }

  def bind(preparedStatement: PreparedStatement, values: Object*): BoundStatement = {
    preparedStatement.bind(values: _*)
      .setConsistencyLevel(consistencyLevel)
      .setTimeout(timeoutDuration)
  }
}
