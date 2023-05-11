package org.folio.postgres.testing;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.folio.util.PostgresTester;
import org.folio.util.PostgresTesterStartException;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

import java.util.Arrays;

public class PostgresTesterContainerTest {

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

  public void testBadDockerImage() {
    var e = assertThrows(PostgresTesterStartException.class, () ->
      new PostgresTesterContainer("").start(null, null, null));
    assertEquals("java.lang.IllegalStateException", e.getCause().getClass().getName());
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
  public void testGetReadPort() { new PostgresTesterContainer().getReadPort(); }

  @Test
  public void testGetNetwork() {
    assertNotNull(new PostgresTesterContainer().getNetwork());
  }

  @Test
  public void testReadWrite() throws SQLException {
    PostgresTester tester = new PostgresTesterContainer();
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

      stmt.executeUpdate("CREATE TABLE crew (id SERIAL PRIMARY KEY, name VARCHAR(50))");
      Arrays.asList("Queequeg", "Starbuck", "Stubb", "Flask", "Daggoo", "Tashtego", "Pip", "Fedallah", "Fleece", "Perth")
          .forEach(name -> {
            try {
              stmt.executeUpdate(String.format("INSERT INTO crew (name) VALUES ('%s');", name));
              ResultSet rs = readOnlyStmt.executeQuery(String.format("SELECT name FROM crew where name = '%s';", name));
              assertTrue(rs.next());
              assertEquals(name, rs.getString("name"));
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          });
    }
    tester.close();
  }
}
