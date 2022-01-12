package de.kaufhof.pillar

import com.datastax.oss.driver.api.core.servererrors.{InvalidQueryException, QueryValidationException}
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.typesafe.config.{Config, ConfigFactory}
import de.kaufhof.pillar.cli.App
import org.scalatest.{BeforeAndAfter, FeatureSpec, GivenWhenThen, Matchers}

// todo tests we need (not necessarily in this file; just wanted to put this note somewhere)
// parsing failure logs something useful
// execution failure aborts the run
// timeout is honored

class PillarCommandLineAcceptanceSpec extends FeatureSpec
  with CassandraSpec
  with GivenWhenThen
  with BeforeAndAfter
  with Matchers
  with AcceptanceAssertions {

  val keyspaceName = "pillar_acceptance_test"
  val environment = "acceptance_test"
  val dataStoreName = "faker"

  //create a configuration pointing to the embedded cassandra, falling back to the default configuration
  lazy val config: Config = ConfigFactory.parseString(
    s"""
       |pillar.$dataStoreName {
       |    $environment {
       |        cassandra-datacenter-name: datacenter1
       |        cassandra-port: $port
       |    }
       |}
     """.stripMargin).withFallback(ConfigFactory.load())

  before {
    try {
      session.execute("DROP KEYSPACE %s".format(keyspaceName))
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
      App(configuration = config).run(Array("-e", environment, "initialize", dataStoreName))

      Then("the keyspace contains a applied_migrations column family")
      assertEmptyAppliedMigrationsTable()
    }
  }

  feature("The operator can apply migrations") {
    info("As an application operator")
    info("I want to migrate a Cassandra keyspace from an older version of the schema to a newer version")
    info("So that I can run an application using the schema")

    scenario("all migrations") {
      Given("an initialized, empty, keyspace")
      App(configuration = config).run(Array("-e", environment, "initialize", dataStoreName))

      Given("a migration that creates an events table")
      Given("a migration that creates a views table")

      When("the migrator migrates the schema")
      App(configuration = config).run(Array("-e", environment, "-d", "src/test/resources/pillar/migrations", "migrate", dataStoreName))

      Then("the keyspace contains the events table")
      session.execute(QueryBuilder.selectFrom(keyspaceName, "events").all().build()).all().size() should equal(0)

      And("the keyspace contains the views table")
      session.execute(QueryBuilder.selectFrom(keyspaceName, "views").all().build()).all().size() should equal(0)

      And("the applied_migrations table records the migrations")
      session.execute(QueryBuilder.selectFrom(keyspaceName, "applied_migrations").all().build()).all().size() should equal(4)
    }
  }
}
