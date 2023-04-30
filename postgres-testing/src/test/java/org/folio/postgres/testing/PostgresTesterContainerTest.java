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

  @Test(expected = PSQLException.class)
  public void testReadWrite() throws SQLException, IOException, InterruptedException {
    PostgresTester tester = new PostgresTesterContainer();
    String user = "user";
    String db = "db";
    String pass = "pass";
    tester.start(db, user, pass);
    int port = tester.getPort();
    String host = tester.getHost();
    int readOnlyPort = tester.getReadPort();
    String readOnlyHost = tester.getReadHost();
    String connString = String.format("jdbc:postgresql://%s:%d/%s", host, port, db);
    String connStringReadOnly = String.format("jdbc:postgresql://%s:%d/%s", readOnlyHost, readOnlyPort, db);

    Connection conn = DriverManager.getConnection(connString, user, pass);
    System.out.println("Connection to database established.");
    Statement stmt = conn.createStatement();
    stmt.executeUpdate("CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(50));");
    stmt.executeUpdate("INSERT INTO users (name) VALUES ('John Doe');");

    // Test that replication between primary and standby happens.
    Connection readOnlyConn = DriverManager.getConnection(connString, user, pass);
    Statement readOnlyStmt = readOnlyConn.createStatement();
    ResultSet rs = readOnlyStmt.executeQuery("SELECT * FROM users");
    while (rs.next()) {
      String name = rs.getString("name");
      Assert.assertEquals(name, "John Doe");
    }

    // Test that we can't write to read-only host.
    readOnlyStmt.executeUpdate("CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(50));");

    conn.close();
    readOnlyConn.close();
  }
}
