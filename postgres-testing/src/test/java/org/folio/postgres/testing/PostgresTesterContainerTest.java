package org.folio.postgres.testing;

import org.folio.util.PostgresTester;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;

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
}
