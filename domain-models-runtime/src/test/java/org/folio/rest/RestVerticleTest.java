package org.folio.rest;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Test;

class RestVerticleTest {
  @Test
  void getDeploymentId() {
    assertThat(RestVerticle.getDeploymentId(), is(""));
  }
}
