package org.folio.rest.tools;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.maven.project.MavenProject;

public class ModuleNameWriter {
  private static final String JAVA =
      "package org.folio.rest.tools.utils;\n"
      + "\n"
      + "public class ModuleName {\n"
      // {} is replaced before writing this .java file
      + "  private static final String MODULE_NAME = \"{}\";\n"
      + "\n"
      + "  public static String getModuleName() {\n"
      + "    return MODULE_NAME;\n"
      + "  }\n"
      + "}\n"
      + "";

  /**
   * Create ${basedir}/target/generated-sources/moduleversion/org/folio/rest/tools/utils/ModuleName.java
   * where ModuleName.getModuleName() returns the current artifactId and add the moduleversion directory as
   * a source directory.
   */
  public static void writeModuleNameClass(MavenProject project) throws IOException {
    File sourceRoot = new File(project.getBasedir(), "target/generated-sources/modulename");
    project.addCompileSourceRoot(sourceRoot.getPath());
    Path dir = new File(sourceRoot, "org/folio/rest/tools/utils").toPath();
    Files.createDirectories(dir);
    project.getParentArtifact();
    String moduleName = project.getParent() == null ? project.getArtifactId() : project.getParent().getArtifactId();
    moduleName = moduleName.replace('-', '_');
    Files.writeString(dir.resolve("ModuleName.java"), JAVA.replace("{}", moduleName));
  }
}
