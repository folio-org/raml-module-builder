package org.folio.rest.tools;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;


import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
import org.folio.util.ResourceUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.vertx.core.logging.LoggerFactory;

public class ClientGeneratorTest {

  private static final Pattern TRAILING_SPACE_PATTERN = Pattern.compile("\\s+$", Pattern.MULTILINE);

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
    File expectedClient = new File(sourceDir + "/TestResourceClient.java");
    Assert.assertTrue(expectedClient.exists());

    // IDEs always removes trailing spaces from edited files, but java code generator adds then,
    // so we nee to remove them before the comparison

    String actual = removeTrailingSpaces(FileUtils.readFileToString(expectedClient, StandardCharsets.UTF_8));

    String expected = removeTrailingSpaces(ResourceUtil.asString("/clients/TestClient.txt", this.getClass()));

    Assert.assertEquals(expected, actual);
  }


  private String removeTrailingSpaces(String str) {
    return TRAILING_SPACE_PATTERN.matcher(str).replaceAll("");
  }

}
