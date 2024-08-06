package org.folio.rest.tools;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import org.apache.maven.model.Dependency;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.text.MatchesPattern.matchesPattern;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.jupiter.api.Assertions.*;

class PomReaderTest {

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
    assertThat(PomReader.INSTANCE.getVersion(), matchesPattern("[0-9]+\\.[0-9]+\\..*"));
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
  void readFromJar() throws IOException, XmlPullParserException {
    PomReader pom = PomReader.INSTANCE;

    pom.readIt(null, "META-INF/maven/io.vertx");  // force reading from Jar
    // first dependency in main pom of Vert.x 4 and Vert.x 5
    assertThat(pom.getModuleName(), isOneOf("vertx_parent", "vertx_core_aggregator"));
  }

  @Test
  void readFromJarNoPom() {
    PomReader pom = PomReader.INSTANCE;

    assertThrows(FileNotFoundException.class, () -> pom.readIt(null, "javax/ws/rs"));
  }

  @Test
  void readFromJarNoResource() {
    PomReader pom = PomReader.INSTANCE;

    assertThrows(IllegalArgumentException.class, () -> pom.readIt(null, "ramls"));
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
  }
}
