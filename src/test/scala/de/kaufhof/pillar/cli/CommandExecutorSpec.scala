package de.kaufhof.pillar.cli

import com.datastax.oss.driver.api.core.{ConsistencyLevel, CqlSession}
import de.kaufhof.pillar._
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSpec}

import java.time.{Duration, Instant}

class CommandExecutorSpec extends FunSpec with BeforeAndAfter with MockitoSugar {
  describe("#execute") {
    val session = mock[CqlSession]
    val keyspace = "myks"
    val registry = mock[Registry]
    val reporter = mock[Reporter]
    val migrator = mock[Migrator]
    val appliedMigrationsTableName = "applied_migrations"
    val statementRegistry: StatementRegistry = new StatementPreparer(session, keyspace, appliedMigrationsTableName, ConsistencyLevel.ONE, Duration.ofMinutes(1))
    val simpleStrategy = SimpleStrategy()
    val networkTopologyStrategy = NetworkTopologyStrategyTestData.networkTopologyStrategy
    val migratorConstructor = mock[(Registry, StatementRegistry, Reporter, String) => Migrator]
    when(migratorConstructor.apply(registry, statementRegistry, reporter, appliedMigrationsTableName)).thenReturn(migrator)
    val executor = new CommandExecutor()(migratorConstructor)

    describe("an initialize action") {
      val commandSimple = Command(Initialize, session, keyspace, None, registry, simpleStrategy, statementRegistry, appliedMigrationsTableName)

      executor.execute(commandSimple, reporter)

      it("initializes a simple strategy") {
        verify(migrator).initialize(session, keyspace, simpleStrategy)
      }

      val commandNetwork = Command(Initialize, session, keyspace, None, registry, networkTopologyStrategy, statementRegistry, appliedMigrationsTableName)

      executor.execute(commandNetwork, reporter)

      it("initializes a network topology strategy") {
        verify(migrator).initialize(session, keyspace, networkTopologyStrategy)
      }
    }

    describe("a migrate action without date restriction") {
      val command = Command(Migrate, session, keyspace, None, registry, simpleStrategy, statementRegistry, appliedMigrationsTableName)

      executor.execute(command, reporter)

      it("migrates") {
        verify(migrator).migrate(session, keyspace, None)
      }
    }

    describe("a migrate action with date restriction") {
      val date = Instant.now()
      val command = Command(Migrate, session, keyspace, Some(date.toEpochMilli), registry, simpleStrategy, statementRegistry, appliedMigrationsTableName)

      executor.execute(command, reporter)

      it("migrates") {
        verify(migrator).migrate(session, keyspace, Some(date))
      }
    }
  }
}
