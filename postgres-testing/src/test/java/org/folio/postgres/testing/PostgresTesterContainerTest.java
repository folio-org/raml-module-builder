package org.folio.postgres.testing;

import org.folio.util.PostgresTester;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.io.IOException;
import java.sql.*;

public class PostgresTesterContainerTest {

  @Test
  public void testStartClose() throws SQLException, IOException, InterruptedException {
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
  public void testBadDockerImage() throws SQLException, IOException, InterruptedException {
    PostgresTester tester = new PostgresTesterContainer("");
    tester.start(null, null, null);
  }

  @Test
  public void testGetDoubleStart() throws SQLException, IOException, InterruptedException {
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

  @Test
  public void testReadWrite() throws SQLException, IOException, InterruptedException {
    PostgresTester tester = new PostgresTesterContainer();
    String user = "user";
    String db = "db";
    String pass = "pass";
    tester.start(db, user, pass);

    // Connect to the read-write host and create some data.
    int port = tester.getPort();
    String host = tester.getHost();
    String connString = String.format("jdbc:postgresql://%s:%d/%s", host, port, db);

    Connection conn = DriverManager.getConnection(connString, user, pass);
    Statement stmt = conn.createStatement();
    // If we want to guarantee that these statements are going to reach the standby before returning we
    // must set synchronous_commit here.
    stmt.executeUpdate("BEGIN; SET LOCAL synchronous_commit = remote_apply; CREATE TABLE accounts (id SERIAL PRIMARY KEY, name VARCHAR(50)); COMMIT;");
    stmt.executeUpdate("BEGIN; SET LOCAL synchronous_commit = remote_apply; INSERT INTO accounts (name) VALUES ('John Doe'); COMMIT;");
    conn.close();

    // See that the data which we just wrote to the primary propagates to the read-only standby.
    int readOnlyPort = tester.getReadPort();
    String readOnlyHost = tester.getReadHost();
    String connStringReadOnly = String.format("jdbc:postgresql://%s:%d/%s", readOnlyHost, readOnlyPort, db);
    Connection readOnlyConn = DriverManager.getConnection(connStringReadOnly, user, pass);
    Statement readOnlyStmt = readOnlyConn.createStatement();

    ResultSet selectResult = readOnlyStmt.executeQuery("SELECT name FROM accounts;");
    if (selectResult.next()) {
      String name = selectResult.getString("name");
      Assert.assertEquals(name, "John Doe");
    }

    // The value of sync_state reflects whether the last transaction was synchronous.
    ResultSet replicationResult = readOnlyStmt.executeQuery("SELECT sync_state FROM pg_stat_replication;");
    if (replicationResult.next()) {
      String name = replicationResult.getString("sync_state");
      Assert.assertEquals(name, "sync");
    }

    // The standby should not accept writes of any kind since it is read-only.
    Exception exception = Assert.assertThrows(PSQLException.class, () -> {
      readOnlyStmt.executeUpdate("INSERT INTO accounts (name) VALUES ('John Doe');");
    });
    Assert.assertEquals("ERROR: cannot execute INSERT in a read-only transaction", exception.getMessage());

    readOnlyConn.close();
  }
}
