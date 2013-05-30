package streamsend.pillar

import org.scalatest.{BeforeAndAfter, GivenWhenThen, FeatureSpec}
import org.scalatest.matchers.ShouldMatchers
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.exceptions.InvalidQueryException
import java.util.Date

class MigratorFeatureSpec extends FeatureSpec with GivenWhenThen with BeforeAndAfter with ShouldMatchers {
  val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
  val session = cluster.connect()
  val keyspaceName = "test_%d".format(System.currentTimeMillis())
  val migrations = Seq(
    Migration("creates events table", new Date(System.currentTimeMillis() - 5000),
      """
        |CREATE TABLE events (
        |  batch_id text,
        |  occurred_at timestamp,
        |  event_type text,
        |  payload blob,
        |  PRIMARY KEY (batch_id, occurred_at, event_type)
        |)
      """.stripMargin),
    Migration("creates views table", new Date(System.currentTimeMillis() - 3000),
      """
        |CREATE TABLE views (
        |  id uuid PRIMARY KEY,
        |  url text,
        |  person_id int,
        |  viewed_at timestamp
        |)
      """.stripMargin,
      Some("""
        |DROP TABLE views
      """.stripMargin)),
    Migration("adds user_agent to views table", new Date(System.currentTimeMillis() - 1000),
      """
        |ALTER TABLE views
        |ADD user_agent text
      """.stripMargin, None), // Dropping a column is coming in Cassandra 2.0
    Migration("adds index on views.user_agent", new Date(),
      """
        |CREATE INDEX views_user_agent ON views(user_agent)
      """.stripMargin,
      Some("""
        |DROP INDEX views_user_agent
      """.stripMargin))
  )
  val registry = MigrationRegistry(migrations)
  val migrator = Migrator(keyspaceName, registry)

  after {
    try {
      session.execute("DROP KEYSPACE %s".format(keyspaceName))
    } catch {
      case ok: InvalidQueryException =>
    }
  }

  feature("The operator can initialize a keyspace") {
    info("As an application operator")
    info("I want to initialize a Cassandra keyspace")
    info("So that I can manage the keyspace schema")

    scenario("initialize a non-existent keyspace") {
      Given("a non-existent keyspace")

      When("the migrator initializes the keyspace")
      migrator.initialize(keyspaceName)

      Then("the keyspace contains a applied_migrations column family")
      assertEmptyAppliedMigrationsTable()
    }

    scenario("initialize an existing keyspace without a applied_migrations column family") {
      Given("an existing keyspace")
      session.execute("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}".format(keyspaceName))

      When("the migrator initializes the keyspace")
      migrator.initialize(keyspaceName)

      Then("the keyspace contains a applied_migrations column family")
      assertEmptyAppliedMigrationsTable()
    }

    scenario("initialize an existing keyspace with a applied_migrations column family") {
      Given("an existing keyspace")
      migrator.initialize(keyspaceName)

      When("the migrator initializes the keyspace")
      migrator.initialize(keyspaceName)

      Then("the migration completes successfully")
    }
  }

  feature("The operator can generate an empty migration") {}

  feature("The operator can apply migrations") {
    info("As an application operator")
    info("I want to migrate a Cassandra keyspace from an older version of the schema to a newer version")
    info("So that I can run an application using the schema")

    scenario("all migrations") {
      Given("an initialized, empty, keyspace")
      migrator.initialize(keyspaceName)

      Given("a migration that creates an events table")
      Given("a migration that creates a views table")

      When("the migrator migrates the schema")
      migrator.migrate()

      Then("the keyspace contains the events table")
      session.execute(QueryBuilder.select().from(keyspaceName, "events")).all().size() should equal(0)

      And("the keyspace contains the views table")
      session.execute(QueryBuilder.select().from(keyspaceName, "views")).all().size() should equal(0)

      And("the applied_migrations table records the migrations")
      session.execute(QueryBuilder.select().from(keyspaceName, "applied_migrations")).all().size() should equal(4)
    }

    scenario("some migrations") {
      Given("an initialized, empty, keyspace")
      migrator.initialize(keyspaceName)

      Given("a migration that creates an events table")
      Given("a migration that creates a views table")

      When("the migrator migrates with a cut off date")
      migrator.migrate(Some(migrations(0).authoredAt))

      Then("the keyspace contains the events table")
      session.execute(QueryBuilder.select().from(keyspaceName, "events")).all().size() should equal(0)

      And("the applied_migrations table records the migration")
      session.execute(QueryBuilder.select().from(keyspaceName, "applied_migrations")).all().size() should equal(1)
    }

    scenario("skip previously applied migration") {
      Given("an initialized keyspace")
      migrator.initialize(keyspaceName)

      Given("a set of migrations applied in the past")
      migrator.migrate()

      When("the migrator applies migrations")
      migrator.migrate()

      Then("the migration completes successfully")
    }
  }

  feature("The operator can reverse migrations") {
    info("As an application operator")
    info("I want to migrate a Cassandra keyspace from a newer version of the schema to an older version")
    info("So that I can run an application using the schema")

    scenario("reverse previously applied migration") {
      Given("an initialized keyspace")
      migrator.initialize(keyspaceName)

      Given("a set of migrations applied in the past")
      migrator.migrate()

      When("the migrator migrates with a cut off date")
      migrator.migrate(Some(migrations(0).authoredAt))

      Then("the migrator reverses the missing migration")
      val thrown = intercept[InvalidQueryException] {
        session.execute(QueryBuilder.select().from(keyspaceName, "views")).all()
      }
      thrown.getMessage should equal("unconfigured columnfamily views")

      And("the migrator removes the reversed migration from the applied migrations table")
      val reversedMigration = migrations(1)
      val query = QueryBuilder.
        select().
        from(keyspaceName, "applied_migrations").
        where(QueryBuilder.eq("authored_at", reversedMigration.authoredAt)).
        and(QueryBuilder.eq("description", reversedMigration.description))
      session.execute(query).all().size() should equal(0)
    }
  }

  feature("The operator can list applied migrations") {}

  private def assertEmptyAppliedMigrationsTable() {
    session.execute(QueryBuilder.select().from(keyspaceName, "applied_migrations")).all().size() should equal(0)
  }
}