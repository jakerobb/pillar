package de.kaufhof.pillar

import com.datastax.oss.driver.api.core.{CqlIdentifier, CqlSession}

import java.time.Instant

object CassandraMigrator {
  val appliedMigrationsTableNameDefault = "applied_migrations"
}

class CassandraMigrator(registry: Registry, statementRegistry: StatementRegistry, appliedMigrationsTableName: String) extends Migrator {
  override def migrate(session: CqlSession, keyspace: String, dateRestriction: Option[Instant] = None) {
    val appliedMigrations = AppliedMigrations(session, registry, statementRegistry)
    useKeyspace(session, keyspace)
    selectMigrationsToReverse(dateRestriction, appliedMigrations).foreach(_.executeDownStatement(session, statementRegistry))
    selectMigrationsToApply(dateRestriction, appliedMigrations).foreach(_.executeUpStatement(session, statementRegistry))
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
    session.execute(s"USE ${CqlIdentifier.fromCql(keyspace)}")
  }

  override def initialize(session: CqlSession, keyspace: String,
                          replicationStrategy: ReplicationStrategy = SimpleStrategy()) {
    createKeyspace(session, keyspace, replicationStrategy)
    useKeyspace(session, keyspace)
    createMigrationsTable(session, keyspace)
  }

  override def createKeyspace(session: CqlSession, keyspace: String, replicationStrategy: ReplicationStrategy = SimpleStrategy()): Unit = {
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication = ${replicationStrategy.cql}")
  }

  override def createMigrationsTable(session: CqlSession, keyspace: String): Unit = {
    session.execute(
      """
        | CREATE TABLE IF NOT EXISTS %s.%s (
        |   authored_at timestamp,
        |   description text,
        |   applied_at timestamp,
        |   PRIMARY KEY (authored_at, description)
        |  )
      """.stripMargin.format(keyspace, appliedMigrationsTableName)
    )
  }

  override def destroy(session: CqlSession, keyspace: String) {
    session.execute("DROP KEYSPACE %s".format(keyspace))
  }
}
