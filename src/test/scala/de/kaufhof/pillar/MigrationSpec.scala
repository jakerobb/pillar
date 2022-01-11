package de.kaufhof.pillar

import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FunSpec, Matchers}

import java.time.Instant

class MigrationSpec extends FunSpec with Matchers with MockitoSugar {
  describe(".apply") {
    describe("without a down parameter") {
      it("returns an irreversible migration") {
        Migration.apply("description", Instant.now(), Seq("up")).getClass should be(classOf[IrreversibleMigration])
      }
    }

    describe("with a down parameter") {
      describe("when the down is None") {
        it("returns a reversible migration with no-op down") {
          Migration.apply("description", Instant.now(), Seq("up"), None).getClass should be(classOf[ReversibleMigrationWithNoOpDown])
        }
      }

      describe("when the down is Some") {
        it("returns a reversible migration with no-op down") {
          Migration.apply("description", Instant.now(), Seq("up"), Some(Seq("down"))).getClass should be(classOf[ReversibleMigration])
        }
      }
    }
  }
}
