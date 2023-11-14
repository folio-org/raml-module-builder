package org.folio.postgres.testing;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.folio.util.PostgresTester;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class PostgresTesterContainerTest {

  @Test
  public void imageName() {
    assertEquals(PostgresTesterContainer.DEFAULT_IMAGE_NAME, PostgresTesterContainer.getImageName());
  }

  @Test
  public void imageNameEnvEmpty() {
    assertEquals(PostgresTesterContainer.DEFAULT_IMAGE_NAME, PostgresTesterContainer.getImageName(Map.of()));
  }

  @Test
  public void imageNameEnv() {
    var env = Map.of("TESTCONTAINERS_POSTGRES_IMAGE", "postgres:15.4-alpine3.18");
    assertEquals("postgres:15.4-alpine3.18", PostgresTesterContainer.getImageName(env));
  }

  @Test
  public void testStartClose() {
    PostgresTester tester = new PostgresTesterContainer();
    assertFalse(tester.isStarted());
    tester.start("db", "user", "pass");
    assertTrue(tester.isStarted());
    assertNotNull(tester.getHost());
    assertTrue(tester.getPort() >= 1024);
    tester.close();
    assertFalse(tester.isStarted());
    tester.close();
  }

  @Test(expected = IllegalStateException.class)
  public void testBadDockerImage() {
    PostgresTester tester = new PostgresTesterContainer("");
    tester.start(null, null, null);
  }

  @Test
  public void testGetDoubleStart() {
    try (var tester = new PostgresTesterContainer()) {
      tester.start("db", "user", "pass");
      var e = assertThrows(IllegalStateException.class, () -> tester.start("db", "user", "pass"));
      assertEquals("already started", e.getMessage());
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testGetHost() {
    new PostgresTesterContainer().getHost();
  }

  @Test(expected = IllegalStateException.class)
  public void testGetPort() {
    new PostgresTesterContainer().getPort();
  }

  @Test(expected = IllegalStateException.class)
  public void testGetReadHost() {
    new PostgresTesterContainer().getReadHost();
  }

  @Test(expected = IllegalStateException.class)
  public void testGetReadPort() {
    new PostgresTesterContainer().getReadPort();
  }

  @Test
  public void testGetNetwork() {
    assertNotNull(new PostgresTesterContainer().getNetwork());
  }

  @Test
  public void testReplication() throws Exception {
    testReplication(new PostgresTesterContainer());
  }

  @Test
  public void testReplicationAsync() throws Exception {
    System.setProperty(PostgresTesterContainer.POSTGRES_ASYNC_COMMIT, "yes");
    testReplication(new PostgresTesterContainer());
    System.clearProperty(PostgresTesterContainer.POSTGRES_ASYNC_COMMIT);
  }

  private void testReplication(PostgresTesterContainer tester) throws Exception {
    String user = "user";
    String db = "db";
    String pass = "pass";
    tester.start(db, user, pass);

    // Connect to the read-write host and create some data.
    int port = tester.getPort();
    int readOnlyPort = tester.getReadPort();
    String host = tester.getHost();
    String readOnlyHost = tester.getReadHost();

    String connString = String.format("jdbc:postgresql://%s:%d/%s", host, port, db);
    String connStringReadOnly = String.format("jdbc:postgresql://%s:%d/%s", readOnlyHost, readOnlyPort, db);

    try (Connection conn = DriverManager.getConnection(connString, user, pass);
         Connection readOnlyConn = DriverManager.getConnection(connStringReadOnly, user, pass);
         Statement stmt = conn.createStatement();
         Statement readOnlyStmt = readOnlyConn.createStatement()) {

      // Check that streaming replication is set up correctly.
      ResultSet state = stmt.executeQuery("SELECT state FROM pg_stat_replication;");
      assertTrue(state.next());
      assertEquals("streaming", state.getString("state"));

      // The standby should not accept writes of any kind since it is read-only.
      Exception exception = assertThrows(PSQLException.class, () -> {
        readOnlyStmt.executeUpdate("CREATE TABLE crew (id SERIAL PRIMARY KEY, name VARCHAR(50));");
      });
      assertEquals("ERROR: cannot execute CREATE TABLE in a read-only transaction", exception.getMessage());

      stmt.executeUpdate("CREATE TABLE crew (id SERIAL PRIMARY KEY, name VARCHAR(50));");

      // If the tester containers have been configured for async replication, we need to poll to verify the replication
      // since writes don't wait for the commit.
      boolean isAsyncTest = tester.hasAsyncCommitConfig();

      verify(() -> {
        ResultSet rs = readOnlyStmt.executeQuery("SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = 'crew');");
        assertTrue(rs.next());
        return rs.getBoolean(1);
      }, isAsyncTest);

      Arrays.asList("Queequeg", "Starbuck", "Stubb", "Flask", "Daggoo", "Tashtego", "Pip", "Fedallah", "Fleece", "Perth")
          .forEach(name -> {
            try {
              stmt.executeUpdate(String.format("INSERT INTO crew (name) VALUES ('%s');", name));
              verify(() -> {
                ResultSet rs = readOnlyStmt.executeQuery(String.format("SELECT name FROM crew where name = '%s';", name));
                if (rs.next()) {
                  return Objects.equals(name, rs.getString("name"));
                }
                return false;
              }, isAsyncTest);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
    }
    tester.close();
  }

  private void verify(Callable<Boolean> callable, boolean isAsync) throws Exception {
    if (isAsync) {
      await()
          .atLeast(PostgresTesterContainer.SIMULATED_ASYNC_REPLICATION_LAG_MILLISECONDS - 100, TimeUnit.MILLISECONDS)
          .atMost(PostgresTesterContainer.SIMULATED_ASYNC_REPLICATION_LAG_MILLISECONDS + 100, TimeUnit.MILLISECONDS)
          .until(callable);
    } else {
      assertTrue(callable.call());
    }
  }
}

