package org.folio.rest.tools;

import io.vertx.core.logging.LoggerFactory;
import java.io.IOException;
import java.util.List;
import org.apache.maven.model.Dependency;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PomReaderTest {
  static {
    System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME, "io.vertx.core.logging.Log4j2LogDelegateFactory");
  }

  @Test
  public void testGetModuleName() {
    assertThat(PomReader.INSTANCE.getModuleName(), is("raml_module_builder"));
  }

  @Test
  public void testGetVersion() {
    assertTrue(PomReader.INSTANCE.getVersion().matches("[0-9]+\\.[0-9]+\\..*"));
  }

  @Test
  public void testGetProps() {
    assertNull(PomReader.INSTANCE.getProps().getProperty("does_not_exist"));
  }

  @Test
  public void testGetDependencies() {
    List<Dependency> dependencies = PomReader.INSTANCE.getDependencies();
    assertTrue(!dependencies.isEmpty());
  }

  @Test
  public void testGetRmbVersion() {
    assertThat(PomReader.INSTANCE.getRmbVersion(), is(PomReader.INSTANCE.getVersion()));
  }

  @Test
  public void readFromJar() throws IOException, XmlPullParserException {
    PomReader pom = PomReader.INSTANCE;

    pom.readIt(false);
    // first dependency in main pom / but surefire sometimes?
    assertThat(pom.getModuleName(), anyOf(is("vertx_parent"), is("surefire")));
    pom.readIt(true);
  }

}
