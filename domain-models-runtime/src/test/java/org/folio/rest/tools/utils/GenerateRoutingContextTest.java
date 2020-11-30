package org.folio.rest.tools.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import org.junit.jupiter.api.Test;

class GenerateRoutingContextTest {

  @Test
  void variableReplacement() {
    assertThat(GenerateRoutingContext.enabled(), contains("/rmbtests/test"));
  }

  @Test
  void test() {
    assertThat(GenerateRoutingContext.string2set(""), is(empty()));
    assertThat(GenerateRoutingContext.string2set("  /a  ,  /b  ,  /c  "), containsInAnyOrder("/a", "/b", "/c"));
    assertThat(GenerateRoutingContext.string2set("/z/z,/y/y,/x/x"), containsInAnyOrder("/x/x", "/y/y", "/z/z"));
  }
}
