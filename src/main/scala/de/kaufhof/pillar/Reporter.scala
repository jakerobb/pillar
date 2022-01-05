package de.kaufhof.pillar

import com.datastax.driver.core.Session

import java.io.File
import java.util.Date


trait Reporter {
  def executing(command: String)
  def parsing(file: File)
  def parsed(file: File)
  def parseFail(file: File, exception: Exception)
  def migrating(session: Session, dateRestriction: Option[Date])
  def applying(migration: Migration)
  def reversing(migration: Migration)
  def destroying(session: Session, keyspace: String)
  def creatingKeyspace(session: Session, keyspace: String, replicationStrategy: ReplicationStrategy)
  def creatingMigrationsTable(session: Session, keyspace: String, appliedMigrationsTableName: String)
}
