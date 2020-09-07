package org.folio.rest.tools.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.text.MatchesPattern.matchesPattern;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class RmbVersionTest {

  private String versionFromPom() {
    try {
      String pom = new String(Files.readAllBytes(Paths.get("pom.xml")));
      Matcher matcher = Pattern.compile("<version>(.*?)<").matcher(pom);
      matcher.find();
      return matcher.group(1);
    } catch (IOException e) {
      throw new UncheckedIOException("pom.xml", e);
    }
  }

  @Test
  void test() {
    String rmbVersion = RmbVersion.getRmbVersion();
    assertThat(rmbVersion, matchesPattern("\\d+\\.\\d+\\.\\d+(-.*)?"));
    assertThat(rmbVersion, is(versionFromPom()));
  }

}
