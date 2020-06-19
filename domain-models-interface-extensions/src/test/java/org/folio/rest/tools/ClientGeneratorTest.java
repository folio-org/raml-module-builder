package org.folio.rest.tools;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.vertx.core.logging.LoggerFactory;

public class ClientGeneratorTest {

  static {
    System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME,
      "io.vertx.core.logging.Log4j2LogDelegateFactory");
  }

  private static String sourceDir;

  @Before
  public void setUp() {
    System.setProperty("client.generate", "true");
    System.setProperty("project.basedir", ".");
    sourceDir = System.getProperties().getProperty("project.basedir")
      + ClientGenerator.PATH_TO_GENERATE_TO
      + RTFConsts.CLIENT_GEN_PACKAGE.replace('.', '/');
  }

  @After
  public void cleanUp() throws IOException {
    System.clearProperty("client.generate");
    System.clearProperty("project.basedir");
    FileUtils.deleteDirectory(new File(sourceDir));
  }

  @Test
  public void doesGenerateTestResourceClient() throws Exception {
    ClientGenerator.main(null);
    Assert.assertTrue(new File(sourceDir + "/TestResourceClient.java").exists());
  }

}
