package de.kaufhof.pillar

import com.datastax.oss.driver.api.core.`type`.DataTypes.{TEXT, TIMESTAMP}
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement, SimpleStatement, Statement}
import com.datastax.oss.driver.api.core.{ConsistencyLevel, CqlSession}
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker
import com.datastax.oss.driver.api.querybuilder.{QueryBuilder, SchemaBuilder}

import java.time.Duration
import scala.collection.JavaConverters.mapAsJavaMap

sealed trait StatementRegistry {
  def bindSelectFromAppliedMigrations(): BoundStatement

  def bindInsertIntoAppliedMigrations(values: Object*): BoundStatement

  def bindDeleteFromAppliedMigrations(values: Object*): BoundStatement

  def buildStatement(query: String): SimpleStatement

  def useKeyspace(keyspace: String): SimpleStatement

  def createKeyspace(keyspaceName: String, replicationStrategy: ReplicationStrategy): SimpleStatement

  def createMigrationsTable(): SimpleStatement

  def dropKeyspace(keyspace: String): SimpleStatement
}

class StatementPreparer(session: CqlSession, keyspace: String, appliedMigrationsTableName: String, consistency: ConsistencyLevel, timeoutDuration: Duration) extends StatementRegistry {
  lazy val consistencyLevel: ConsistencyLevel = consistency

  lazy val selectFromAppliedMigrations: PreparedStatement = {
    val selectFromAppliedMigrationsQuery = QueryBuilder.selectFrom(keyspace, appliedMigrationsTableName)
      .columns("authored_at", "description")

    session.prepare(selectFromAppliedMigrationsQuery.build)
  }

  lazy val insertIntoAppliedMigrations: PreparedStatement = {
    val insertIntoAppliedMigrationsQuery = QueryBuilder.insertInto(keyspace, appliedMigrationsTableName)
      .value("authored_at", bindMarker())
      .value("description", bindMarker())
      .value("applied_at", bindMarker())

    session.prepare(insertIntoAppliedMigrationsQuery.build)
  }

  lazy val deleteFromAppliedMigrations: PreparedStatement = {
    val deleteFromAppliedMigrationsQuery = QueryBuilder.deleteFrom(keyspace, appliedMigrationsTableName)
      .whereColumn("authored_at").isEqualTo(bindMarker())
      .whereColumn("description").isEqualTo(bindMarker())

    session.prepare(deleteFromAppliedMigrationsQuery.build)
  }

  override def useKeyspace(keyspace: String): SimpleStatement = {
    buildStatement(s"USE $keyspace")
  }

  override def buildStatement(query: String): SimpleStatement = {
    applyStandardOptions(SimpleStatement.builder(query).build)
  }

  override def createKeyspace(keyspaceName: String, replicationStrategy: ReplicationStrategy): SimpleStatement = {
    val builder = SchemaBuilder.createKeyspace(keyspaceName).ifNotExists()

    def withReplicationStrategy = {
      replicationStrategy match {
        case NetworkTopologyStrategy(dataCenters: Seq[CassandraDataCenter]) =>
          builder.withNetworkTopologyStrategy(mapAsJavaMap(dataCenters.map(dc => dc.name -> Integer.valueOf(dc.replicationFactor)).toMap))
        case SimpleStrategy(replicationFactor: Int) =>
          builder.withSimpleStrategy(replicationFactor)
      }
    }

    val s = withReplicationStrategy.build()
    applyStandardOptions(s)
  }

  override def createMigrationsTable(): SimpleStatement = {
    val s = SchemaBuilder.createTable(appliedMigrationsTableName).ifNotExists()
      .withPartitionKey("authored_at", TIMESTAMP)
      .withClusteringColumn("description", TEXT)
      .withColumn("applied_at", TIMESTAMP)
      .build()
    applyStandardOptions(s)
  }

  override def dropKeyspace(keyspace: String): SimpleStatement = {
    val s = SchemaBuilder.dropKeyspace(keyspace).build()
    applyStandardOptions(s)
  }

  override def bindSelectFromAppliedMigrations(): BoundStatement = {
    bindWithStandardOptions(selectFromAppliedMigrations)
  }

  def bindWithStandardOptions(preparedStatement: PreparedStatement, values: Object*): BoundStatement = {
    applyStandardOptions(preparedStatement.bind(values.toIndexedSeq: _*))
  }

  def applyStandardOptions[T <: Statement[T]](statement: Statement[T]): T = {
    statement
      .setConsistencyLevel(consistencyLevel)
      .setTimeout(timeoutDuration)
  }

  override def bindInsertIntoAppliedMigrations(values: Object*): BoundStatement = {
    bindWithStandardOptions(insertIntoAppliedMigrations, values.toIndexedSeq: _*)
  }

  override def bindDeleteFromAppliedMigrations(values: Object*): BoundStatement = {
    bindWithStandardOptions(deleteFromAppliedMigrations, values.toIndexedSeq: _*)
  }

}
