package de.kaufhof.pillar

import com.datastax.oss.driver.api.core.cql.PreparedStatement
import com.datastax.oss.driver.api.core.{ConsistencyLevel, CqlSession}
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker

sealed trait StatementRegistry {
  def consistencyLevel: ConsistencyLevel

  def selectFromAppliedMigrations(): PreparedStatement

  def insertIntoAppliedMigrations(): PreparedStatement

  def deleteFromAppliedMigrations(): PreparedStatement
}

class StatementPreparer(session: CqlSession, keyspace: String, appliedMigrationsTableName: String, consistency: ConsistencyLevel) extends StatementRegistry {
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
}
