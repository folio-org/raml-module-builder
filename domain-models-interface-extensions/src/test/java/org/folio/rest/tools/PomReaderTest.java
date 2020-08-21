package org.folio.rest.tools;

import io.vertx.core.logging.LoggerFactory;
import java.io.IOException;
import java.util.List;
import org.apache.maven.model.Dependency;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

class PomReaderTest {
  static {
    System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME, "io.vertx.core.logging.Log4j2LogDelegateFactory");
  }

  @AfterEach
  void tearDown() {
    PomReader pom = PomReader.INSTANCE;
    pom.init("pom.xml");  // restore for other unit tests (it's a singleton)
  }

  @Test
  void testGetModuleName() {
    assertThat(PomReader.INSTANCE.getModuleName(), is("raml_module_builder"));
  }

  @Test
  void testGetVersion() {
    assertTrue(PomReader.INSTANCE.getVersion().matches("[0-9]+\\.[0-9]+\\..*"));
  }

  @Test
  void testGetProps() {
    assertNull(PomReader.INSTANCE.getProps().getProperty("does_not_exist"));
  }

  @Test
  void testGetDependencies() {
    List<Dependency> dependencies = PomReader.INSTANCE.getDependencies();
    assertTrue(!dependencies.isEmpty());
  }

  @Test
  void testGetRmbVersion() {
    assertThat(PomReader.INSTANCE.getRmbVersion(), is(PomReader.INSTANCE.getVersion()));
  }


  @Test
  void readFromJar() throws IOException, XmlPullParserException {
    PomReader pom = PomReader.INSTANCE;

    pom.readIt(null, "META-INF/maven");  // force reading from Jar
    // first dependency in main pom / but surefire sometimes?
    assertThat(pom.getModuleName(), anyOf(is("vertx_parent"), is("surefire")));
  }

  @Test
  void readFromJarNoPom() {
    PomReader pom = PomReader.INSTANCE;

    Exception exception = assertThrows(NullPointerException.class,
        () -> pom.readIt(null, "ramls"));
  }

  @Test
  void readFromJarNoResource() {
    PomReader pom = PomReader.INSTANCE;

    assertThrows(NullPointerException.class, () -> pom.readIt(null, "pom/pom-sample.xml"));
  }

  @Test
  void BadFilename()  {
    PomReader pom = PomReader.INSTANCE;

    assertThrows(IllegalArgumentException.class, () -> pom.init("does_not_exist.xml"));
  }

  @Test
  void otherPom() throws IOException, XmlPullParserException {
    PomReader pom = PomReader.INSTANCE;

    pom.init("src/test/resources/pom/pom-sample.xml");
    assertThat(PomReader.INSTANCE.getModuleName(), is("mod_inventory_storage"));
    assertThat(PomReader.INSTANCE.getVersion(), is("19.4.0"));
    assertThat(PomReader.INSTANCE.getRmbVersion(), is("30.0.0"));
  }
}
