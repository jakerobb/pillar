package de.kaufhof.pillar

import com.datastax.oss.driver.api.core.CqlSession

import java.io.File
import java.time.Instant


trait Reporter {
  def emptyDirectory(directory: File): Unit

  def readingDirectory(directory: File): Unit

  def parsing(file: File): Unit

  def parsed(file: File): Unit

  def parseFail(file: File, exception: Exception): Unit

  def initializing(session: CqlSession, keyspace: String, replicationStrategy: ReplicationStrategy): Unit

  def migrating(session: CqlSession, keyspace: String, dateRestriction: Option[Instant]): Unit

  def applying(migration: Migration): Unit

  def reversing(migration: Migration): Unit

  def destroying(session: CqlSession, keyspace: String): Unit

  def creatingKeyspace(session: CqlSession, keyspace: String, replicationStrategy: ReplicationStrategy): Unit

  def usingKeyspace(session: CqlSession, keyspace: String): Unit

  def creatingMigrationsTable(session: CqlSession, keyspace: String, appliedMigrationsTableName: String): Unit
}
