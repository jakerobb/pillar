package de.kaufhof.pillar

import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.driver.api.core.{CqlIdentifier, CqlSession}

import java.time.{Duration, Instant}

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
    val s = SimpleStatement.builder(s"USE ${CqlIdentifier.fromCql(keyspace)}")
      .setTimeout(Duration.ofMinutes(1)).build
    session.execute(s)
  }

  override def initialize(session: CqlSession, keyspace: String,
                          replicationStrategy: ReplicationStrategy = SimpleStrategy()) {
    createKeyspace(session, keyspace, replicationStrategy)
    useKeyspace(session, keyspace)
    createMigrationsTable(session, keyspace)
  }

  override def createKeyspace(session: CqlSession, keyspace: String, replicationStrategy: ReplicationStrategy = SimpleStrategy()): Unit = {
    val s = SimpleStatement.builder(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication = ${replicationStrategy.cql}")
      .setTimeout(Duration.ofMinutes(1)).build
    session.execute(s)
  }

  override def createMigrationsTable(session: CqlSession, keyspace: String): Unit = {
    val s = SimpleStatement.builder(
      """
        | CREATE TABLE IF NOT EXISTS %s.%s (
        |   authored_at timestamp,
        |   description text,
        |   applied_at timestamp,
        |   PRIMARY KEY (authored_at, description)
        |  )
      """.stripMargin.format(keyspace, appliedMigrationsTableName))
      .setTimeout(Duration.ofMinutes(1)).build
    session.execute(s)
  }

  override def destroy(session: CqlSession, keyspace: String) {
    val s = SimpleStatement.builder("DROP KEYSPACE %s".format(keyspace))
      .setTimeout(Duration.ofMinutes(1)).build
    session.execute(s)
  }
}
