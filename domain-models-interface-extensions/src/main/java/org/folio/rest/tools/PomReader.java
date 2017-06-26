package org.folio.rest.tools;

import java.io.File;
import java.io.FileReader;

import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ResourceInfo;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * @author shale
 *
 */
public enum PomReader {

  INSTANCE;

  private String moduleName = null;
  private String version = null;

  private final Logger log = LoggerFactory.getLogger(PomReader.class);

  @SuppressWarnings("checkstyle:methodlength")
  private PomReader() {
    try {
      System.out.println("Attempting to read in the module name....");
      MavenXpp3Reader mavenreader = new MavenXpp3Reader();
      Model model = null;
      String currentRunningJar =
          PomReader.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
      if(currentRunningJar != null && (currentRunningJar.contains("domain-models-runtime")
          || currentRunningJar.contains("domain-models-interface-extensions"))){
        //the runtime is the jar run when deploying during unit tests
        //the interface-extensions is the jar run when running build time tools,
        //like MDGenerator, ClientGenerator, etc..
        //this is build time - not runtime, so just use the pom
        File pomFile = new File("pom.xml");
        model = mavenreader.read(new FileReader(pomFile));
      }
      else{
        //this is runtime, the jar called via java -jar is the module's jar
        ClassPath classPath = ClassPath.from(Thread.currentThread().getContextClassLoader());
        ImmutableSet<ResourceInfo> resources = classPath.getResources();
        for (ResourceInfo info : resources) {
          if(info.getResourceName().endsWith("pom.xml")){
            //maven sets the classpath order according to the pom, so the poms project will be the first entry
            model = mavenreader.read(PomReader.class.getResourceAsStream("/"+info.getResourceName()));
            break;
          }
        }
      }
      if(model.getParent() != null){
        moduleName = model.getParent().getArtifactId();
      }
      else{
        moduleName = model.getArtifactId();
      }
      version = model.getVersion();
      if(version == null){
        version = "1.0.0";
      }
      version.replaceAll("-.*", "");
      moduleName = moduleName.replaceAll("-", "_");
      log.info("module name: " + moduleName + ", version: " + version);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
  }

  public String getVersion() {
    return version;
  }

  public String getModuleName() {
    return moduleName;
  }
}
