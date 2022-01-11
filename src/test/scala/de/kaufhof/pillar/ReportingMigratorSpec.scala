package de.kaufhof.pillar

import com.datastax.oss.driver.api.core.CqlSession
import org.mockito.Mockito._
import org.scalatest.FunSpec
import org.scalatest.mockito.MockitoSugar

class ReportingMigratorSpec extends FunSpec with MockitoSugar {
  val reporter: Reporter = mock[Reporter]
  val wrapped: Migrator = mock[Migrator]
  val appliedMigrationsTableName = "applied_migrations"
  val migrator = new ReportingMigrator(reporter, wrapped, appliedMigrationsTableName)
  val session: CqlSession = mock[CqlSession]
  val keyspace = "myks"

  describe("#initialize") {
    val replicationStrategy = SimpleStrategy()
    migrator.initialize(session, keyspace, replicationStrategy)

    it("delegates to both createKeyspace and createMigrationsTable of the wrapped migrator") {
      verify(wrapped).createKeyspace(session, keyspace, replicationStrategy)
      verify(wrapped).createMigrationsTable(session, keyspace)
    }
  }

  describe("#migrate") {
    migrator.migrate(session, keyspace)

    it("reports the migrate action") {
      verify(reporter).migrating(session, keyspace, None)
    }

    it("delegates to the wrapped migrator") {
      verify(wrapped).migrate(session, keyspace, None)
    }
  }

  describe("#destroy") {
    migrator.destroy(session, keyspace)

    it("reports the destroy action") {
      verify(reporter).destroying(session, keyspace)
    }

    it("delegates to the wrapped migrator") {
      verify(wrapped).destroy(session, keyspace)
    }
  }
}
