package org.folio.rest.persist.ddlgen;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;

import org.junit.jupiter.api.Test;

class IndexTest {

  @Test
  void testIndexExpression() {
    Index idx = new Index();
    idx.setFieldName("testIdx");
    idx.setMultiFieldNames("field1,field2,field3");
    assertEquals("concat_space_sql(test_table.jsonb->>'field1' , test_table.jsonb->>'field2' , test_table.jsonb->>'field3')",idx.getFinalSqlExpression("test_table"));
    idx.setFieldName("testIdx");
    idx.setMultiFieldNames("blah.blah2.field1,blah.blah2.field2,blah.blah2.field3");
    assertEquals("concat_space_sql(test_table.jsonb->'blah'->'blah2'->>'field1' , test_table.jsonb->'blah'->'blah2'->>'field2' , test_table.jsonb->'blah'->'blah2'->>'field3')",idx.getFinalSqlExpression("test_table"));
  }
}
