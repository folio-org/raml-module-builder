package org.folio.rest.tools;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.folio.rest.tools.plugins.CustomTypeAnnotator;
import org.folio.rest.tools.utils.RamlDirCopier;

/**
 * @requiresDependencyResolution runtime
 */
@Mojo(name = "java", defaultPhase = LifecyclePhase.GENERATE_SOURCES)
public class DomainModelsMojo extends AbstractMojo {

  @Parameter(defaultValue= "${project}", readonly = true)
  private MavenProject project;

  @Parameter(property = "ramlDirs")
  private File[] ramlDirs;

  @Parameter(property = "schemaPaths")
  private File[] schemaPaths;

  @Parameter(property = "jsonSchemaCustomFields", defaultValue = "")
  private String[] jsonSchemaCustomFields;

  @Parameter(property = "generateInterfaces", defaultValue = "true")
  private boolean generateInterfaces;

  @Parameter(property = "generateClients", defaultValue = "false")
  private boolean generateClients;

  /** for unit tests */
  DomainModelsMojo withDefaults() {
    project = new MavenProject();
    project.setFile(new File("pom.xml").getAbsoluteFile());
    project.setParent(project);
    ramlDirs = new File[] {};
    schemaPaths = new File[] {};
    jsonSchemaCustomFields = new String[] { "" };
    generateInterfaces = true;
    generateClients = false;
    return this;
  }

  DomainModelsMojo withProject(MavenProject project) {
    this.project = project;
    return this;
  }

  DomainModelsMojo withGenerateInterfaces(boolean generateInterfaces) {
    this.generateInterfaces = generateInterfaces;
    return this;
  }

  DomainModelsMojo withGenerateClients(boolean generateClients) {
    this.generateClients = generateClients;
    return this;
  }

  @Override
  public void execute() {
    System.err.println("DomainModelsMojo.execute()");
    System.err.println("generateInterfaces=" + generateInterfaces);
    System.err.println("generateClients=" + generateClients);
    System.err.println("project.baseDir = " + (project == null ? "" : project.getBasedir()));
    System.err.println("project.name = " + (project == null ? "" : project.getName()));
    System.err.println("project.actifactId = " + (project == null ? "" : project.getArtifactId()));
    System.err.println("Artifacts: " + project.getArtifacts());
    System.err.println("Artifacts: " + project.getArtifactMap());

    if (ramlDirs != null) {
      System.out.println("ramlDirs.size=" + ramlDirs.length);
      for (File ramlDir : ramlDirs) {
        System.err.println("ramlDir = " + ramlDir);
      }
    }

    if (schemaPaths != null) {
      System.out.println("schemaPaths.size=" + schemaPaths.length);
      for (File schemaPath : schemaPaths) {
        System.err.println("schemaPath = " + schemaPath);
      }
    }

    setRootLogLevel(Level.WARNING);
    try {
      if (generateInterfaces) {
        generateInterfaces();
        ModuleNameWriter.writeModuleNameClass(project);
      }
      if (generateClients) {
        int sz = ClientGenerator.generate(project.getBasedir().getAbsolutePath());
        System.err.println("Client classes generated: " + sz);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void generateInterfaces() throws IOException {
    String outputDirectory = project.getBasedir() + ClientGenerator.PATH_TO_GENERATE_TO;
    GenerateRunner generateRunner = new GenerateRunner(outputDirectory);
    CustomTypeAnnotator.setCustomFields(jsonSchemaCustomFields);

    if (ramlDirs.length == 0) {
      File file = ramlsDir(project);
      if (file == null || ! file.isDirectory()) {
        File parent = ramlsDir(project.getParent());
        if (parent == null) {
          throw new FileNotFoundException(
              "No <ramlDirs> configuration, and default dir not found: " + file);
        }
        file = parent;
      }
      ramlDirs = new File[] { file };
    }

    Path inputPath = GenerateRunner.rebase(ramlDirs[0]).toPath();
    Path outputPath = project.getBasedir().toPath()
        .resolve(GenerateRunner.RESOURCE_DEFAULT)
        .resolve(GenerateRunner.SOURCES_DEFAULT)
        .toAbsolutePath();
    RamlDirCopier.copy(inputPath, outputPath);

    for (File ramlDir : ramlDirs) {
      String dir = ramlDir.getAbsolutePath().replace(inputPath.toString(), outputPath.toString());
      generateRunner.generate(dir);
    }

    List<String> subdirectoryExpressions = Arrays.asList(schemaPaths).stream()
        .map(File::getPath)
        .collect(Collectors.toList());
    GenerateRunner.createLookupList(outputPath.toFile(), GenerateRunner.RAML_LIST,
        Collections.singletonList(".raml"));
    GenerateRunner.createLookupList(outputPath.toFile(), GenerateRunner.JSON_SCHEMA_LIST,
        Arrays.asList(".json", ".schema"), subdirectoryExpressions);
  }

  private void setRootLogLevel(Level level) {
    Logger rootLogger = Logger.getLogger("");
    rootLogger.setLevel(level);
    for (Handler handler : rootLogger.getHandlers()) {
        handler.setLevel(level);
    }
  }

  private File ramlsDir(MavenProject mavenProject) {
    if (mavenProject == null || mavenProject.getBasedir() == null) {
      return null;
    }
    return mavenProject.getBasedir().toPath()
        .resolve(GenerateRunner.SOURCES_DEFAULT).toAbsolutePath().toFile();
  }
}
