package org.folio.rest.tools.utils;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import org.folio.okapi.testing.UtilityClassTester;
import org.junit.jupiter.api.Test;

class ResourceUtilsTest {

  @Test
  void isUtilityClass() {
    UtilityClassTester.assertUtilityClass(ResourceUtils.class);
  }

  @Test
  void resource2StringCanRead() {
    assertThat(ResourceUtils.resource2String("resourceUtil.txt"), is("domain-models-runtime"));
  }

  @Test
  void resource2StringNotFound() {
    assertThrows(IllegalArgumentException.class, () -> ResourceUtils.resource2String("nonexisting-file"));
  }
}
