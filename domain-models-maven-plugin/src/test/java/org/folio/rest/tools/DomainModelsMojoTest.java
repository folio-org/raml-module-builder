package org.folio.rest.tools;

import java.io.File;
import org.apache.maven.project.MavenProject;
import org.assertj.core.api.WithAssertions;
import org.junit.jupiter.api.Test;

class DomainModelsMojoTest implements WithAssertions {

  @Test
  void generateInterfaces() {
    new DomainModelsMojo().withDefaults()
    .execute();
  }

  @Test
  void generateClients() {
    new DomainModelsMojo().withDefaults().withGenerateInterfaces(false).withGenerateClients(true)
    .execute();
  }

  @Test
  void ramlsDirNotFound() {
    assertThatThrownBy(() ->
      new DomainModelsMojo().withDefaults().withProject(project("src/main"))
      .execute())
    .hasMessageContaining("No <ramlDirs> configuration")
    .hasMessageContaining("src/ramls");
  }

  MavenProject project(String file) {
    MavenProject project = new MavenProject();
    project.setFile(new File(file));
    return project;
  }
}
