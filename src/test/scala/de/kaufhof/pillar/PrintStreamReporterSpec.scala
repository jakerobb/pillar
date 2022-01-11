package de.kaufhof.pillar

import com.datastax.oss.driver.api.core.CqlSession
import org.scalatest._
import org.scalatest.mockito.MockitoSugar

import java.io.{ByteArrayOutputStream, PrintStream}
import java.time.Instant

class PrintStreamReporterSpec extends FunSpec with MockitoSugar with Matchers with OneInstancePerTest {
  val session: CqlSession = mock[CqlSession]
  val migration: Migration = Migration("creates things table", Instant.ofEpochMilli(1370489972546L), Seq("up"), Some(Seq("down")))
  val output = new ByteArrayOutputStream()
  val stream = new PrintStream(output)
  val reporter = new PrintStreamReporter(stream)
  val keyspace = "myks"
  val replicationStrategy: SimpleStrategy = SimpleStrategy()
  val nl: String = System.lineSeparator()
  val appliedMigrationsTableName = "applied_migrations"

  describe("#creatingKeyspace") {
    it("should print to the stream") {
      reporter.creatingKeyspace(session, keyspace, replicationStrategy)
      output.toString should equal(s"Creating keyspace myks$nl")
    }
  }

  describe("#creatingMigrationsTable") {
    it("should print to the stream") {
      reporter.creatingMigrationsTable(session, keyspace, appliedMigrationsTableName)
      output.toString should equal(s"Creating migrations-table [$appliedMigrationsTableName] in keyspace myks$nl")
    }
  }

  describe("#migrating") {
    describe("without date restriction") {
      it("should print to the stream") {
        reporter.migrating(session, keyspace, None)
        output.toString should equal(s"Migrating keyspace $keyspace with date restriction None$nl")
      }
    }
  }

  describe("#applying") {
    it("should print to the stream") {
      reporter.applying(migration)
      output.toString should equal(s"Applying 1370489972546: creates things table$nl")
    }
  }

  describe("#reversing") {
    it("should print to the stream") {
      reporter.reversing(migration)
      output.toString should equal(s"Reversing 1370489972546: creates things table$nl")
    }
  }

  describe("#destroying") {
    it("should print to the stream") {
      reporter.destroying(session, keyspace)
      output.toString should equal(s"Destroying myks$nl")
    }
  }
}
