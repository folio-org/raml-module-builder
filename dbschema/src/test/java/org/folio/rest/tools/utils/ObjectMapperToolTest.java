package org.folio.rest.tools.utils;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.folio.rest.persist.ddlgen.Schema;
import org.folio.rest.testing.UtilityClassTester;
import org.folio.util.ResourceUtil;
import org.junit.jupiter.api.Test;

class ObjectMapperToolTest {

  @Test
  void isUtilityClass() {
    UtilityClassTester.assertUtilityClass(ObjectMapperTool.class);
  }

  @Test
  void canReadSchema() throws Throwable {
    String dbJson = ResourceUtil.asString("schema.json");
    Schema dbSchema = ObjectMapperTool.getMapper().readValue(dbJson, org.folio.rest.persist.ddlgen.Schema.class);
    assertThat(dbSchema.getTables().get(0).getTableName(), is("item"));
  }
}
