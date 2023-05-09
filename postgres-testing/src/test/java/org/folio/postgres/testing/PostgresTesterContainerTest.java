package org.folio.postgres.testing;

import org.folio.util.PostgresTester;
import org.folio.util.PostgresTesterStartException;
import org.junit.Assert;
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
  public void testStartClose() throws PostgresTesterStartException {
    PostgresTester tester = new PostgresTesterContainer();
    Assert.assertFalse(tester.isStarted());
    tester.start("db", "user", "pass");
    Assert.assertTrue(tester.isStarted());
    Assert.assertNotNull(tester.getHost());
    Assert.assertTrue(tester.getPort() >= 1024);
    tester.close();
    Assert.assertFalse(tester.isStarted());
    tester.close();
  }

  @Test(expected = IllegalStateException.class)
  public void testBadDockerImage() throws PostgresTesterStartException {
    PostgresTester tester = new PostgresTesterContainer("");
    tester.start(null, null, null);
  }

  @Test
  public void testGetDoubleStart() throws PostgresTesterStartException {
    PostgresTester tester = new PostgresTesterContainer();
    tester.start("db", "user", "pass");
    String msg = "";
    try {
      tester.start("db", "user", "pass");
    } catch (IllegalStateException e) {
      msg = e.getMessage();
    }
    Assert.assertEquals("already started", msg);
    tester.close();
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
  public void testReadWrite() throws SQLException, PostgresTesterStartException {
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
      Assert.assertTrue(state.next());
      Assert.assertEquals("streaming", state.getString("state"));

      // The standby should not accept writes of any kind since it is read-only.
      Exception exception = Assert.assertThrows(PSQLException.class, () -> {
        readOnlyStmt.executeUpdate("CREATE TABLE crew (id SERIAL PRIMARY KEY, name VARCHAR(50));");
      });
      Assert.assertEquals("ERROR: cannot execute CREATE TABLE in a read-only transaction", exception.getMessage());

      // Should we test synchronous commit? The default is true.
      boolean testSynchronousCommit = System.getProperty(PostgresTesterContainer.POSTGRES_ASYNC_COMMIT) == null;
      if (!testSynchronousCommit) {
        return;
      }

      // The first writes to the replicated cluster requires remote_apply. Subsequent ones don't.
      stmt.executeUpdate("BEGIN; SET LOCAL synchronous_commit = remote_apply; CREATE TABLE crew (id SERIAL PRIMARY KEY, name VARCHAR(50)); COMMIT;");
      Arrays.asList("Queequeg", "Starbuck", "Stubb", "Flask", "Daggoo", "Tashtego", "Pip", "Fedallah", "Fleece", "Perth")
          .forEach(name -> {
            try {
              stmt.executeUpdate(String.format("INSERT INTO crew (name) VALUES ('%s');", name));
              ResultSet rs = readOnlyStmt.executeQuery(String.format("SELECT name FROM crew where name = '%s';", name));
              Assert.assertTrue(rs.next());
              Assert.assertEquals(name, rs.getString("name"));
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          });
    }
  }
}
