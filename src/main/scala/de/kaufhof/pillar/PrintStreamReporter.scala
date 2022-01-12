package de.kaufhof.pillar

import com.datastax.oss.driver.api.core.CqlSession

import java.io.{File, PrintStream}
import java.time.Instant

class PrintStreamReporter(stream: PrintStream) extends Reporter {

  override def emptyDirectory(directory: File): Unit = {
    stream.println(s"Directory ${directory.getAbsolutePath} is empty.")
  }

  override def readingDirectory(directory: File): Unit = {
    stream.println(s"Reading migrations from ${directory.getAbsolutePath}")
  }

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

  override def initializing(session: CqlSession, keyspace: String, replicationStrategy: ReplicationStrategy): Unit = {
    stream.println(s"Initializing for keyspace $keyspace")
  }

  override def migrating(session: CqlSession, keyspace: String, dateRestriction: Option[Instant]) {
    stream.println(s"Migrating keyspace $keyspace with date restriction $dateRestriction")
  }

  override def applying(migration: Migration) {
    stream.println(s"Applying ${migration.authoredAt.toEpochMilli}: ${migration.description}")
  }

  override def reversing(migration: Migration) {
    stream.println(s"Reversing ${migration.authoredAt.toEpochMilli}: ${migration.description}")
  }

  override def destroying(session: CqlSession, keyspace: String) {
    stream.println(s"Destroying $keyspace")
  }

  override def usingKeyspace(session: CqlSession, keyspace: String): Unit = {
    stream.println(s"Using keyspace $keyspace")
  }

  override def creatingKeyspace(session: CqlSession, keyspace: String, replicationStrategy: ReplicationStrategy): Unit = {
    stream.println(s"Creating keyspace $keyspace")
  }

  override def creatingMigrationsTable(session: CqlSession, keyspace: String, appliedMigrationsTableName: String): Unit = {
    stream.println(s"Creating migrations-table [$appliedMigrationsTableName] in keyspace $keyspace")
  }

}
