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
    String host = tester.getHost();
    String connString = String.format("jdbc:postgresql://%s:%d/%s", host, port, db);

    Connection conn = DriverManager.getConnection(connString, user, pass);
    Statement stmt = conn.createStatement();

    // Check that streaming replication is set up correctly.
    ResultSet checkResult = stmt.executeQuery("SELECT state FROM pg_stat_replication;");
    if (checkResult.next()) {
      String state = checkResult.getString("state");
      Assert.assertEquals("streaming", state);
    }

    // The first writes to the replicated cluster requires remote_apply. Subsequent ones don't.
    stmt.executeUpdate("BEGIN; SET LOCAL synchronous_commit = remote_apply; CREATE TABLE accounts (id SERIAL PRIMARY KEY, name VARCHAR(50)); COMMIT;");
    stmt.executeUpdate("BEGIN; SET LOCAL synchronous_commit = remote_apply; INSERT INTO accounts (name) VALUES ('Starbuck'); COMMIT;");

    // See that the data which we just wrote to the primary propagates to the read-only standby.
    int readOnlyPort = tester.getReadPort();
    String readOnlyHost = tester.getReadHost();
    String connStringReadOnly = String.format("jdbc:postgresql://%s:%d/%s", readOnlyHost, readOnlyPort, db);
    Connection readOnlyConn = DriverManager.getConnection(connStringReadOnly, user, pass);
    Statement readOnlyStmt = readOnlyConn.createStatement();

    ResultSet first = readOnlyStmt.executeQuery("SELECT name FROM accounts;");
    if (first.next()) {
      String name = first.getString("name");
      Assert.assertEquals("Starbuck", name);
    }

    // The second time we perform an insert it propagates right away.
    stmt.executeUpdate("INSERT INTO accounts (name) VALUES ('Ishmael');");
    ResultSet second = readOnlyStmt.executeQuery("SELECT name FROM accounts where name = 'Ishmael';");
    if (second.next()) {
      String secondName = second.getString("name");
      Assert.assertEquals("Ishmael", secondName);
    }

    // The standby should not accept writes of any kind since it is read-only.
    Exception exception = Assert.assertThrows(PSQLException.class, () -> {
      readOnlyStmt.executeUpdate("INSERT INTO accounts (name) VALUES ('Ahab');");
    });
    Assert.assertEquals("ERROR: cannot execute INSERT in a read-only transaction", exception.getMessage());

    conn.close();
    readOnlyConn.close();
  }
}
