package org.folio.rest.tools;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.apache.maven.model.Dependency;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;

/**
 * @author shale
 *
 */
public enum PomReader {

  INSTANCE;

  private String moduleName = null;
  private String version = null;
  private Properties props = null;
  private List<Dependency> dependencies = null;

  private final Logger log = LoggerFactory.getLogger(PomReader.class);

  private PomReader() {
    init("pom.xml");
  }

  /**
   * Read from pomFile if this is RMB itself; otherwise read JAR
   * @param pomFilename
   */
  void init(String pomFilename) {
    try {
      String currentRunningJar =
          PomReader.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
      boolean readCurrent = currentRunningJar != null && (currentRunningJar.contains("domain-models-runtime")
          || currentRunningJar.contains("domain-models-maven-plugin"));
      readIt(readCurrent ? pomFilename : null, "META-INF/maven");
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Read from pomFile if not null; otherwise read JAR
   * @param pomFilename POM filename; null for search in JAR
   * @param directoryName directory prefix for search of pom.xml in JAR
   */
  void readIt(String pomFilename, String directoryName) throws IOException, XmlPullParserException {
    Model model;
    if (pomFilename != null) {
      log.info("Reading from " + pomFilename);
      //the runtime is the jar run when deploying during unit tests
      //the interface-extensions is the jar run when running build time tools,
      //like MDGenerator, ClientGenerator, etc..
      //this is build time - not runtime, so just use the pom
      File pomFile = new File(pomFilename);
      MavenXpp3Reader mavenreader = new MavenXpp3Reader();
      model = mavenreader.read(new FileReader(pomFile));
    } else { //this is runtime, the jar called via java -jar is the module's jar
      log.info("Reading from jar");
      model = getModelFromJar(directoryName);
    }
    if (model.getParent() != null) {
      moduleName = model.getParent().getArtifactId();
      version = model.getParent().getVersion();
    } else {
      moduleName = model.getArtifactId();
      version = model.getVersion();
    }
    version = version.replaceAll("-.*", "");

    moduleName = moduleName.replace("-", "_");
    props = model.getProperties();
    dependencies = model.getDependencies();

    //the version is a placeholder to a value in the props section
    version = replacePlaceHolderWithValue(version);

    log.info("module name: " + moduleName + ", version: " + version);
  }

  private Model getModelFromJar(String directoryName) throws IOException, XmlPullParserException {
    MavenXpp3Reader mavenreader = new MavenXpp3Reader();
    URL url = Thread.currentThread().getContextClassLoader().getResource(directoryName);
    if (! url.getProtocol().equals("jar")) {
      throw new IllegalArgumentException("Is not jar: " + url);
    }
    String dirname = directoryName + "/";
    String path = url.getPath();
    String jarPath = path.substring(5, path.indexOf('!'));

    try (JarFile jar = new JarFile(URLDecoder.decode(jarPath, StandardCharsets.UTF_8.name()))) {
      Enumeration<JarEntry> entries = jar.entries();
      while (entries.hasMoreElements()) {
        JarEntry entry = entries.nextElement();
        String name = entry.getName();
        // first pom.xml should be the right one.
        if (name.startsWith(dirname) && !dirname.equals(name) && name.endsWith("pom.xml")) {
          InputStream pomFile = PomReader.class.getClassLoader().getResourceAsStream(name);
          return mavenreader.read(pomFile);
        }
      }
    }
    throw new FileNotFoundException("pom.xml not found in " + url);
  }

  private String replacePlaceHolderWithValue(String placeholder){
    String[] ret = new String[]{placeholder};
    if (placeholder != null && placeholder.startsWith("${")) {
      props.forEach( (k,v) -> {
        if (("${"+k+"}").equals(placeholder)) {
          ret[0] = (String)v;
        }
      });
    }
    return ret[0];
  }

  public String getVersion() {
    return version;
  }

  public String getModuleName() {
    return moduleName;
  }

  public Properties getProps() {
    return props;
  }

  public List<Dependency> getDependencies() {
    return dependencies;
  }

}
