package org.folio.rest.tools;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.text.MatchesPattern.matchesPattern;

import org.junit.jupiter.api.Test;

class PomReaderIT {
  @Test
  void rmbVersion() {
    PomReader pom = PomReader.INSTANCE;
    pom.init("src/test/resources/pom/pom-sample.xml");
    assertThat(PomReader.INSTANCE.getVersion(), is("19.4.0"));
    assertThat(PomReader.INSTANCE.getRmbVersion(), is(not("19.4.0")));
    assertThat(PomReader.INSTANCE.getRmbVersion(), matchesPattern("[0-9]+\\.[0-9]+\\..*"));
  }
}
