package de.kaufhof.pillar

import com.datastax.oss.driver.api.core.{ConsistencyLevel, CqlSession}
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FunSpec, Matchers}

import java.time.Duration

class ReportingMigrationSpec extends FunSpec with Matchers with MockitoSugar {
  val reporter: Reporter = mock[Reporter]
  val wrapped: Migration = mock[Migration]
  val migration = new ReportingMigration(reporter, wrapped)
  val session: CqlSession = mock[CqlSession]
  val appliedMigrationsTableName = "applied_migrations"
  val statementRegistry = new StatementPreparer(session, "keyspace", appliedMigrationsTableName, ConsistencyLevel.ONE, Duration.ofMinutes(1))

  describe("#executeUpStatement") {
    migration.executeUpStatement(session, statementRegistry)

    it("reports the applying action") {
      verify(reporter).applying(wrapped)
    }

    it("delegates to the wrapped migration") {
      verify(wrapped).executeUpStatement(session, statementRegistry)
    }
  }

  describe("#executeDownStatement") {
    migration.executeDownStatement(session, statementRegistry)

    it("reports the reversing action") {
      verify(reporter).reversing(wrapped)
    }

    it("delegates to the wrapped migration") {
      verify(wrapped).executeDownStatement(session, statementRegistry)
    }
  }
}
