package de.kaufhof.pillar

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.metadata.Metadata
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import org.scalatest.Matchers

import java.util.Optional

trait AcceptanceAssertions extends Matchers {
  val session: CqlSession
  val keyspaceName: String

  protected def assertEmptyAppliedMigrationsTable(appliedMigrationsTableName: String = "applied_migrations") {
    val query = QueryBuilder.selectFrom(keyspaceName, appliedMigrationsTableName).all().build()
    session.execute(query).all().size() should equal(0)
  }

  protected def assertKeyspaceDoesNotExist() {
    val metadata: Metadata = session.getMetadata
    metadata.getKeyspace(keyspaceName) should be(Optional.empty)
  }
}
