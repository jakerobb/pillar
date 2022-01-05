package de.kaufhof.pillar

import com.datastax.driver.core.Session

import java.io.{File, PrintStream}
import java.util.Date

class PrintStreamReporter(stream: PrintStream) extends Reporter {

  override def parsing(file: File) {
    stream.println(s"Parsing file ${file.getAbsolutePath}")
  }

  override def parsed(file: File) {
    stream.println(s"Parsed file ${file.getAbsolutePath}")
  }

  override def parseFail(file: File, exception: Exception) {
    stream.println(s"Failed to parse file ${file.getAbsolutePath}")
    exception.printStackTrace(stream)
  }

  override def migrating(session: Session, dateRestriction: Option[Date]) {
    stream.println(s"Migrating with date restriction $dateRestriction")
  }

  override def applying(migration: Migration) {
    stream.println(s"Applying ${migration.authoredAt.getTime}: ${migration.description}")
  }

  override def reversing(migration: Migration) {
    stream.println(s"Reversing ${migration.authoredAt.getTime}: ${migration.description}")
  }

  override def destroying(session: Session, keyspace: String) {
    stream.println(s"Destroying $keyspace")
  }

  override def creatingKeyspace(session: Session, keyspace: String, replicationStrategy: ReplicationStrategy): Unit = {
    stream.println(s"Creating keyspace $keyspace")
  }

  override def creatingMigrationsTable(session: Session, keyspace: String, appliedMigrationsTableName: String): Unit = {
    stream.println(s"Creating migrations-table [$appliedMigrationsTableName] in keyspace $keyspace")
  }

}
