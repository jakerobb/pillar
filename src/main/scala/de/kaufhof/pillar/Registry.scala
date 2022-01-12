package de.kaufhof.pillar

import java.io.{File, FileInputStream}
import java.time.Instant

object Registry {
  def apply(migrations: Seq[Migration]): Registry = {
    new Registry(migrations)
  }

  def fromDirectory(directory: File, reporter: Reporter): Registry = {
    reporter.readingDirectory(directory)
    new Registry(parseMigrationsInDirectory(directory, reporter).map(new ReportingMigration(reporter, _)))
  }

  private def parseMigrationsInDirectory(directory: File, reporter: Reporter): Seq[Migration] = {
    if (!directory.isDirectory) {
      reporter.emptyDirectory(directory)
      return List.empty
    }

    val parser = Parser()

    directory.listFiles().map {
      file =>
        reporter.parsing(file)
        val stream = new FileInputStream(file)
        try {
          val migration = parser.parse(stream)
          reporter.parsed(file)
          migration
        } catch {
          case e: Exception => reporter.parseFail(file, e)
            throw e
        } finally {
          stream.close()
        }
    }.toList
  }
}

class Registry(private var migrations: Seq[Migration]) {
  migrations = migrations.sortBy(_.authoredAt)

  private val migrationsByKey = migrations.foldLeft(Map.empty[MigrationKey, Migration]) {
    (memo, migration) => memo + (migration.key -> migration)
  }

  def authoredBefore(date: Instant): Seq[Migration] = {
    migrations.filter(migration => migration.authoredBefore(date))
  }

  def apply(key: MigrationKey): Migration = {
    migrationsByKey(key)
  }

  def all: Seq[Migration] = migrations
}
