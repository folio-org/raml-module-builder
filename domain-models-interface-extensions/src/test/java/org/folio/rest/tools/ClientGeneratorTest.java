package org.folio.rest.tools;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.folio.util.IoUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.vertx.core.logging.LoggerFactory;

public class ClientGeneratorTest {

  static {
    System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME, "io.vertx.core.logging.Log4j2LogDelegateFactory");
  }

  @Test
  public void canRunMain() throws Exception {
    // this is not taking
    // System.setProperty("client.generate", "true");
    System.setProperty("project.basedir", ".");
    ClientGenerator.main(null);
  }

}
