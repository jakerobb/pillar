package de.kaufhof.pillar.cli

import com.datastax.oss.driver.api.core.{ConsistencyLevel, CqlSession}
import com.typesafe.config.{Config, ConfigFactory}
import de.kaufhof.pillar._
import de.kaufhof.pillar.config.ConnectionConfiguration

import java.io.File
import java.net.InetSocketAddress
import javax.net.ssl.SSLContext

object App {
  def apply(reporter: Reporter = new PrintStreamReporter(System.out),
            configuration: Config = ConfigFactory.load()): App = {
    new App(reporter, configuration)
  }

  def main(arguments: Array[String]) {
    try {
      App().run(arguments)
    } catch {
      case exception: Exception =>
        System.err.println(exception.getMessage)
        System.err.flush()
        System.exit(1)
    }

    System.exit(0)
  }
}

class App(reporter: Reporter, configuration: Config) {

  def run(arguments: Array[String]) {
    val commandLineConfiguration = CommandLineConfiguration.buildFromArguments(arguments)
    val registry = Registry.fromDirectory(new File(commandLineConfiguration.migrationsDirectory, commandLineConfiguration.dataStore), reporter)
    val dataStoreName = commandLineConfiguration.dataStore
    val environment = commandLineConfiguration.environment

    val cassandraConfiguration = new ConnectionConfiguration(dataStoreName, environment, configuration)

    val session = createSession(commandLineConfiguration, cassandraConfiguration)

    val replicationOptions = try {
      ReplicationStrategyBuilder.getReplicationStrategy(configuration, dataStoreName, environment)
    } catch {
      case e: Exception => throw e
    }

    val statementRegistry: StatementRegistry = new StatementPreparer(session, cassandraConfiguration.keyspace, cassandraConfiguration.appliedMigrationsTableName, ConsistencyLevel.QUORUM)
    val command = Command(
      commandLineConfiguration.command,
      session,
      cassandraConfiguration.keyspace,
      commandLineConfiguration.timeStampOption,
      registry,
      replicationOptions,
      statementRegistry,
      cassandraConfiguration.appliedMigrationsTableName
    )

    try {
      CommandExecutor().execute(command, reporter)
    } finally {
      session.close()
    }
  }

  private def createSession(commandLineConfiguration: CommandLineConfiguration, connectionConfiguration: ConnectionConfiguration): CqlSession = {
    val sessionBuilder = CqlSession.builder

    sessionBuilder.withLocalDatacenter(connectionConfiguration.datacenter)
    connectionConfiguration.seedAddress
      .map(a => new InetSocketAddress(a, connectionConfiguration.port))
      .foreach(sessionBuilder.addContactPoint)

    connectionConfiguration.auth.foreach(sessionBuilder.withAuthProvider)

    connectionConfiguration.sslConfig.foreach(_.setAsSystemProperties())
    if (connectionConfiguration.useSsl) {
      sessionBuilder.withSslContext(SSLContext.getDefault)
    }

    sessionBuilder.build
  }
}
