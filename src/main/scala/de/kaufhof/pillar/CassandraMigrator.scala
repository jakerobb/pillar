package de.kaufhof.pillar

import com.datastax.oss.driver.api.core.CqlSession

import java.time.Instant

object CassandraMigrator {
  val appliedMigrationsTableNameDefault = "applied_migrations"
}

class CassandraMigrator(registry: Registry, statementRegistry: StatementRegistry, debug: Boolean) extends Migrator {
  override def migrate(session: CqlSession, keyspace: String, dateRestriction: Option[Instant] = None) {
    val appliedMigrations = AppliedMigrations(session, registry, statementRegistry)
    useKeyspace(session, keyspace)
    selectMigrationsToReverse(dateRestriction, appliedMigrations).foreach(_.executeDownStatement(session, statementRegistry, debug))
    selectMigrationsToApply(dateRestriction, appliedMigrations).foreach(_.executeUpStatement(session, statementRegistry, debug))
  }

  private def selectMigrationsToApply(dateRestriction: Option[Instant], appliedMigrations: AppliedMigrations): Seq[Migration] = {
    (dateRestriction match {
      case None => registry.all
      case Some(cutOff) => registry.authoredBefore(cutOff)
    }).filter(!appliedMigrations.contains(_))
  }

  private def selectMigrationsToReverse(dateRestriction: Option[Instant], appliedMigrations: AppliedMigrations): Seq[Migration] = {
    (dateRestriction match {
      case None => List.empty[Migration]
      case Some(cutOff) => appliedMigrations.authoredAfter(cutOff)
    }).sortBy(_.authoredAt).reverse
  }

  override def useKeyspace(session: CqlSession, keyspace: String): Unit = {
    val s = statementRegistry.useKeyspace(keyspace)
    session.execute(s)
  }

  override def initialize(session: CqlSession, keyspace: String,
                          replicationStrategy: ReplicationStrategy = SimpleStrategy()) {
    createKeyspace(session, keyspace, replicationStrategy)
    useKeyspace(session, keyspace)
    createMigrationsTable(session, keyspace)
  }

  override def createKeyspace(session: CqlSession, keyspace: String, replicationStrategy: ReplicationStrategy = SimpleStrategy()): Unit = {
    val s = statementRegistry.createKeyspace(keyspace, replicationStrategy)
    session.execute(s)
  }

  override def createMigrationsTable(session: CqlSession, keyspace: String): Unit = {
    val s = statementRegistry.createMigrationsTable()
    session.execute(s)
  }

  override def destroy(session: CqlSession, keyspace: String) {
    val s = statementRegistry.dropKeyspace(keyspace)
    session.execute(s)
  }
}
