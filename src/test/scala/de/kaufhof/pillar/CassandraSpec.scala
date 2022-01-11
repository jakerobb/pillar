package de.kaufhof.pillar

import com.datastax.oss.driver.api.core.CqlSession
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Suite}

import java.net.InetSocketAddress
import scala.concurrent.duration._

/**
  * A mixin for repo test specs that require a Cassandra instance for testing.
  *
  * When tests are run with this mixed in, it will attempt to start an embedded instance of Cassandra on a random port,
  * which will then be usable by the unit test code.
  *
  * The `session` is then available for use by the implementor.
  */
trait CassandraSpec extends ScalaFutures with BeforeAndAfterAll {
  this: Suite =>

  lazy val session: CqlSession = {
    startEmbeddedCassandra()
    CqlSession.builder()
      .addContactPoint(new InetSocketAddress("127.0.0.1", port))
      .withLocalDatacenter("datacenter1")
      .build()
  }
  //These must be lazy to ensure correct init order
  protected lazy val port: Int = EmbeddedCassandraServerHelper.getNativeTransportPort

  protected def startEmbeddedCassandra(): Unit = try {
    //Start the Cassandra Instance
    println("Starting embedded Cassandra...")
    EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra_embedded.yaml", 60.seconds.toMillis)
    println(s"Cassandra running on Port $port")
  } catch {
    case e: Exception =>
      System.err.println(s"Error starting Embedded Cassandra: $e")
  }

  override protected def afterAll(): Unit = {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    super.afterAll()
  }

}

