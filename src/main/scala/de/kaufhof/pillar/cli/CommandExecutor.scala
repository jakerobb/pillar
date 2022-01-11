package de.kaufhof.pillar.cli

import de.kaufhof.pillar.{Migrator, Registry, Reporter, StatementRegistry}

import java.time.Instant

object CommandExecutor {
  implicit private val migratorConstructor: (Registry, StatementRegistry, Reporter, String) => Migrator = Migrator.apply

  def apply(): CommandExecutor = new CommandExecutor()
}

class CommandExecutor(implicit val migratorConstructor: (Registry, StatementRegistry, Reporter, String) => Migrator) {
  def execute(command: Command, reporter: Reporter) {
    val migrator = migratorConstructor(command.registry, command.preparedStatements, reporter, command.appliedMigrationsTableName)
    command.action match {
      case Initialize => migrator.initialize(command.session, command.keyspace, command.replicationStrategy)
      case Migrate => migrator.migrate(command.session, command.keyspace, command.timeStampOption.map(Instant.ofEpochMilli))
    }
  }
}
