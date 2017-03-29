package org.folio.rest.tools;

import static org.junit.Assert.assertEquals;

import org.folio.rest.tools.utils.UtilityClassTester;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)  // run test() first with default load path
public class ApplicationPropertiesTest {
  @Test
  public void test() {
    assertEquals("FooBarApp", ApplicationProperties.getName());
    assertEquals("9.88.777", ApplicationProperties.getVersion());
    assertEquals("0123456789abcdef0123456789abcdef", ApplicationProperties.getGitCommitId());
    assertEquals("a1b2c3d", ApplicationProperties.getGitCommitIdAbbrev());
    assertEquals("https://example.com/foobar/foobarapp.git", ApplicationProperties.getGitRemoteOriginUrl());
  }

  @Test
  public void test2() {
    ApplicationProperties.load("application.empty.properties");
    assertEquals("", ApplicationProperties.getName());
    assertEquals("", ApplicationProperties.getVersion());
    assertEquals("", ApplicationProperties.getGitCommitId());
    assertEquals("", ApplicationProperties.getGitCommitIdAbbrev());
    assertEquals("", ApplicationProperties.getGitRemoteOriginUrl());
  }

  @Test(expected = InternalError.class)
  public void test3() {
    ApplicationProperties.load("nonexisting.properties");
  }

  @Test
  public void utilityClass() {
    UtilityClassTester.assertUtilityClass(ApplicationProperties.class);
  }
}
