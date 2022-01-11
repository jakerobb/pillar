package de.kaufhof.pillar.cli

import com.datastax.oss.driver.api.core.CqlSession
import de.kaufhof.pillar.{Registry, ReplicationStrategy, StatementRegistry}

case class Command(action: MigratorAction, session: CqlSession, keyspace: String, timeStampOption: Option[Long],
                   registry: Registry, replicationStrategy: ReplicationStrategy, preparedStatements: StatementRegistry, appliedMigrationsTableName: String)
