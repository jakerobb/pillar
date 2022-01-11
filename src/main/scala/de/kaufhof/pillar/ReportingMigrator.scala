package de.kaufhof.pillar

import com.datastax.oss.driver.api.core.CqlSession

import java.time.Instant

class ReportingMigrator(reporter: Reporter, wrapped: Migrator, appliedMigrationsTableName: String) extends Migrator {
  override def initialize(session: CqlSession, keyspace: String, replicationStrategy: ReplicationStrategy) {
    createKeyspace(session, keyspace, replicationStrategy)
    createMigrationsTable(session, keyspace)
  }

  override def createKeyspace(session: CqlSession, keyspace: String, replicationStrategy: ReplicationStrategy): Unit = {
    reporter.creatingKeyspace(session, keyspace, replicationStrategy)
    wrapped.createKeyspace(session, keyspace, replicationStrategy)
  }

  override def createMigrationsTable(session: CqlSession, keyspace: String): Unit = {
    reporter.creatingMigrationsTable(session, keyspace, appliedMigrationsTableName)
    wrapped.createMigrationsTable(session, keyspace)
  }

  override def migrate(session: CqlSession, keyspace: String, dateRestriction: Option[Instant] = None) {
    reporter.migrating(session, keyspace, dateRestriction)
    wrapped.migrate(session, keyspace, dateRestriction)
  }

  override def destroy(session: CqlSession, keyspace: String) {
    reporter.destroying(session, keyspace)
    wrapped.destroy(session, keyspace)
  }

  override def useKeyspace(session: CqlSession, keyspace: String): Unit = {
    reporter.usingKeyspace(session, keyspace)
    wrapped.useKeyspace(session, keyspace)
  }
}
