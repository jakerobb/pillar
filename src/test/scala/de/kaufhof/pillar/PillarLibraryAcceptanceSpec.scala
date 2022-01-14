package de.kaufhof.pillar

import com.datastax.oss.driver.api.core.servererrors.{InvalidQueryException, QueryValidationException}
import com.datastax.oss.driver.api.core.{ConsistencyLevel, CqlIdentifier}
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import org.scalatest.{BeforeAndAfter, FeatureSpec, GivenWhenThen, Matchers}

import java.time.{Duration, Instant}

class PillarLibraryAcceptanceSpec extends FeatureSpec
  with CassandraSpec
  with GivenWhenThen
  with BeforeAndAfter
  with Matchers
  with AcceptanceAssertions {

  val keyspaceName: String = "test_%d".format(Instant.now().toEpochMilli)
  val simpleStrategy: SimpleStrategy = SimpleStrategy()
  val appliedMigrationsTableName = "applied_migrations"
  val nonDefaultAppliedMigrationsTableName = "applied_migrations_non_default"
  val statementRegistry = new StatementPreparer(session, keyspaceName, appliedMigrationsTableName, ConsistencyLevel.ONE, Duration.ofMinutes(1))
  val nonDefaultStatementRegistry = new StatementPreparer(session, keyspaceName, nonDefaultAppliedMigrationsTableName, ConsistencyLevel.ONE, Duration.ofMinutes(1))

  val now: Instant = Instant.now()
  val migrations = Seq(
    Migration("creates events table", now.minusMillis(5000),
      Seq(
        """
          |CREATE TABLE events (
          |  batch_id text,
          |  occurred_at uuid,
          |  event_type text,
          |  payload blob,
          |  PRIMARY KEY (batch_id, occurred_at, event_type)
          |)
      """.stripMargin)),
    Migration("creates views table", now.minusMillis(3000),
      Seq(
        """
          |CREATE TABLE views (
          |  id uuid PRIMARY KEY,
          |  url text,
          |  person_id int,
          |  viewed_at timestamp
          |)
      """.stripMargin),
      Some(Seq(
        """
          |DROP TABLE views
        """.stripMargin))),
    Migration("adds user_agent to views table", now.minusMillis(1000),
      Seq(
        """
          |ALTER TABLE views
          |ADD user_agent text
          """.stripMargin), None), // Dropping a column is coming in Cassandra 2.0
    Migration("adds index on views.user_agent", now,
      Seq(
        """
          |CREATE INDEX views_user_agent ON views(user_agent)
      """.stripMargin),
      Some(Seq(
        """
          |DROP INDEX views_user_agent
        """.stripMargin)))
  )
  val registry: Registry = Registry(migrations)
  val migrator: Migrator = Migrator(registry, statementRegistry, debug = true)

  after {
    try {
      val s = statementRegistry.buildStatement("DROP KEYSPACE %s".format(keyspaceName))
      session.execute(s)
    } catch {
      case _: InvalidQueryException =>
      case _: QueryValidationException =>
    }
  }

  feature("The operator can initialize a keyspace") {
    info("As an application operator")
    info("I want to initialize a Cassandra keyspace")
    info("So that I can manage the keyspace schema")

    scenario("initialize a non-existent keyspace") {
      Given("a non-existent keyspace")

      When("the migrator initializes the keyspace")
      migrator.initialize(session, keyspaceName, simpleStrategy)

      Then("the keyspace contains a applied_migrations column family")
      assertEmptyAppliedMigrationsTable()
    }

    scenario("initialize an existing keyspace without a applied_migrations column family") {
      Given("an existing keyspace")
      session.execute(s"CREATE KEYSPACE $keyspaceName WITH replication = ${simpleStrategy.cql}")

      When("the migrator initializes the keyspace")
      migrator.initialize(session, keyspaceName, simpleStrategy)

      Then("the keyspace contains a applied_migrations column family")
      assertEmptyAppliedMigrationsTable()
    }

    scenario("initialize an existing keyspace with a applied_migrations column family") {
      Given("an existing keyspace")
      migrator.initialize(session, keyspaceName, simpleStrategy)

      When("the migrator initializes the keyspace")
      migrator.initialize(session, keyspaceName, simpleStrategy)

      Then("the migration completes successfully")
    }
  }

  feature("The operator can destroy a keyspace") {
    info("As an application operator")
    info("I want to destroy a Cassandra keyspace")
    info("So that I can clean up automated tasks")

    scenario("destroy a keyspace") {
      Given("an existing keyspace")
      migrator.initialize(session, keyspaceName, simpleStrategy)

      When("the migrator destroys the keyspace")
      migrator.destroy(session, keyspaceName)

      Then("the keyspace no longer exists")
      assertKeyspaceDoesNotExist()
    }

    scenario("destroy a bad keyspace") {
      Given("a datastore with a non-existing keyspace")

      When("the migrator destroys the keyspace")

      Then("the migrator throws an exception")
      assertThrows[Throwable] {
        migrator.destroy(session, keyspaceName)
      }
    }
  }

  feature("The operator can apply migrations") {
    info("As an application operator")
    info("I want to migrate a Cassandra keyspace from an older version of the schema to a newer version")
    info("So that I can run an application using the schema")

    scenario("all migrations") {
      Given("an initialized, empty, keyspace")
      migrator.initialize(session, keyspaceName, simpleStrategy)

      Given("a migration that creates an events table")
      Given("a migration that creates a views table")

      When("the migrator migrates the schema")
      session.execute("USE " + CqlIdentifier.fromCql(keyspaceName))
      migrator.migrate(session, keyspaceName)

      Then("the keyspace contains the events table")
      session.execute(QueryBuilder.selectFrom(keyspaceName, "events").all().build()).all().size() should equal(0)

      And("the keyspace contains the views table")
      session.execute(QueryBuilder.selectFrom(keyspaceName, "views").all().build()).all().size() should equal(0)

      And("the applied_migrations table records the migrations")
      session.execute(QueryBuilder.selectFrom(keyspaceName, "applied_migrations").all().build()).all().size() should equal(4)
    }

    scenario("all migrations for a non default applied migrations table name") {
      Given("an initialized, empty, keyspace")
      val migrator = Migrator(registry, nonDefaultStatementRegistry, debug = true)
      migrator.initialize(session, keyspaceName, simpleStrategy)

      Given("a migration that creates an events table")
      Given("a migration that creates a views table")

      When("the migrator migrates the schema")
      migrator.migrate(session, keyspaceName)

      Then("the keyspace contains the events table")
      session.execute(QueryBuilder.selectFrom(keyspaceName, "events").all().build()).all().size() should equal(0)

      And("the keyspace contains the views table")
      session.execute(QueryBuilder.selectFrom(keyspaceName, "views").all().build()).all().size() should equal(0)

      And("the applied_migrations table records the migrations")
      session.execute(QueryBuilder.selectFrom(keyspaceName, nonDefaultAppliedMigrationsTableName).all().build()).all().size() should equal(4)
    }

    scenario("some migrations") {
      Given("an initialized, empty, keyspace")
      migrator.initialize(session, keyspaceName, simpleStrategy)

      Given("a migration that creates an events table")
      Given("a migration that creates a views table")

      When("the migrator migrates with a cut off date")
      migrator.migrate(session, keyspaceName, Some(migrations.head.authoredAt))

      Then("the keyspace contains the events table")
      session.execute(QueryBuilder.selectFrom(keyspaceName, "events").all().build()).all().size() should equal(0)

      And("the applied_migrations table records the migration")
      session.execute(QueryBuilder.selectFrom(keyspaceName, "applied_migrations").all().build()).all().size() should equal(1)
    }

    scenario("skip previously applied migration") {
      Given("an initialized keyspace")
      migrator.initialize(session, keyspaceName, simpleStrategy)

      Given("a set of migrations applied in the past")
      migrator.migrate(session, keyspaceName)

      When("the migrator applies migrations")
      migrator.migrate(session, keyspaceName)

      Then("the migration completes successfully")
    }
  }

  feature("The operator can reverse migrations") {
    info("As an application operator")
    info("I want to migrate a Cassandra keyspace from a newer version of the schema to an older version")
    info("So that I can run an application using the schema")

    scenario("reversible previously applied migration") {
      Given("an initialized keyspace")
      migrator.initialize(session, keyspaceName, simpleStrategy)

      Given("a set of migrations applied in the past")
      migrator.migrate(session, keyspaceName)

      When("the migrator migrates with a cut off date")
      migrator.migrate(session, keyspaceName, Some(migrations.head.authoredAt))

      Then("the migrator reverses the reversible migration")
      val thrown = intercept[InvalidQueryException] {
        session.execute(QueryBuilder.selectFrom(keyspaceName, "views").all().build()).all()
      }
      thrown.getMessage should startWith("unconfigured")

      And("the migrator removes the reversed migration from the applied migrations table")
      val reversedMigration = migrations(1)
      val query = QueryBuilder.
        selectFrom(keyspaceName, "applied_migrations").all()
        .whereColumn("authored_at").isEqualTo(QueryBuilder.literal(reversedMigration.authoredAt))
        .whereColumn("description").isEqualTo(QueryBuilder.literal(reversedMigration.description))
        .build()
      session.execute(query).all().size() should equal(0)
    }

    scenario("irreversible previously applied migration") {
      Given("an initialized keyspace")
      migrator.initialize(session, keyspaceName, simpleStrategy)

      Given("a set of migrations applied in the past")
      migrator.migrate(session, keyspaceName)

      When("the migrator migrates with a cut off date")
      val thrown = intercept[IrreversibleMigrationException] {
        migrator.migrate(session, keyspaceName, Some(Instant.ofEpochMilli(0)))
      }

      Then("the migrator reverses the reversible migrations")
      session.execute(QueryBuilder.selectFrom(keyspaceName, "applied_migrations").all().build()).all().size() should equal(1)

      And("the migrator throws an IrreversibleMigrationException")
      thrown should not be null
    }
  }
}
