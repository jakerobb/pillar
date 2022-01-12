package de.kaufhof.pillar

import com.datastax.oss.driver.api.core.CqlSession

import java.time.Instant

object Migrator {
  def apply(registry: Registry, statementRegistry: StatementRegistry, reporter: Reporter, appliedMigrationsTableName: String): Migrator = {
    new ReportingMigrator(reporter, apply(registry, statementRegistry), appliedMigrationsTableName)
  }

  def apply(registry: Registry, statementRegistry: StatementRegistry): Migrator = {
    new CassandraMigrator(registry, statementRegistry)
  }
}

trait Migrator {
  def migrate(session: CqlSession, keyspace: String, dateRestriction: Option[Instant] = None): Unit

  def initialize(session: CqlSession, keyspace: String, replicationStrategy: ReplicationStrategy): Unit

  def createKeyspace(session: CqlSession, keyspace: String, replicationStrategy: ReplicationStrategy): Unit

  def useKeyspace(session: CqlSession, keyspace: String): Unit

  def createMigrationsTable(session: CqlSession, keyspace: String): Unit

  def destroy(session: CqlSession, keyspace: String): Unit
}
