package de.kaufhof.pillar

import com.datastax.oss.driver.api.core.CqlSession

import java.time.Instant
import scala.collection.JavaConverters

object AppliedMigrations {
  def apply(session: CqlSession, registry: Registry, statementRegistry: StatementRegistry): AppliedMigrations = {
    val results = session.execute(statementRegistry.bindSelectFromAppliedMigrations())
    new AppliedMigrations(JavaConverters.asScalaBuffer(results.all()).map {
      row => registry(MigrationKey(row.getInstant("authored_at"), row.getString("description")))
    })
  }
}

class AppliedMigrations(applied: Seq[Migration]) {
  def length: Int = applied.length

  def apply(index: Int): Migration = applied.apply(index)

  def iterator: Iterator[Migration] = applied.iterator

  def authoredAfter(date: Instant): Seq[Migration] = applied.filter(migration => migration.authoredAfter(date))

  def contains(other: Migration): Boolean = applied.contains(other)
}
