package org.folio.rest.tools;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import org.apache.maven.project.MavenProject;

public class ModuleNameWriter {
  private static final String JAVA =
      "package org.folio.rest.tools.utils;\n"
      + "\n"
      + "public class ModuleName {\n"
      // {name} and {version} are replaced before writing this .java file
      + "  private static final String MODULE_NAME = \"{name}\";\n"
      + "  private static final String MODULE_VERSION = \"{version}\";\n"
      + "\n"
      + "  /**\n"
      + "   * The module name with minus replaced by underscore, for example {@code mod_foo_bar}.\n"
      + "   */\n"
      + "  public static String getModuleName() {\n"
      + "    return MODULE_NAME;\n"
      + "  }\n"
      + "\n"
      + "  /**\n"
      + "   * The module version taken from pom.xml at compile time.\n"
      + "   */\n"
      + "  public static String getModuleVersion() {\n"
      + "    return MODULE_VERSION;\n"
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
    MavenProject root = Objects.requireNonNullElse(project.getParent(), project);
    String name = root.getArtifactId().replace('-', '_');
    String version = root.getVersion();
    Files.writeString(dir.resolve("ModuleName.java"), JAVA.replace("{name}", name).replace("{version}", version));
  }
}
