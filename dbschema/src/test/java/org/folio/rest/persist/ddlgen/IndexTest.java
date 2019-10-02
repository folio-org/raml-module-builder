package org.folio.rest.persist.ddlgen;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;

import org.junit.jupiter.api.Test;

class IndexTest {

  @Test
  void testIndexExpression() {
    Index idx = new Index();
    idx.setFieldPath("testIdx");
    idx.setFieldName("testIdx");
    idx.setMultiFieldNames("field1,field2,field3");
    assertEquals("concat_space_sql(test_table.jsonb->>'field1' , test_table.jsonb->>'field2' , test_table.jsonb->>'field3')",idx.getFinalSqlExpression("test_table"));

  }

  void tesIndexExpressionDotsInPath() {
    Index idx = new Index();
    idx.setFieldName("testIdx");
    idx.setMultiFieldNames("blah.blah2.field1,blah.blah2.field2");
    assertEquals("concat_space_sql(test_table.jsonb->'blah'->'blah2'->>'field1' , test_table.jsonb->'blah'->'blah2'->>'field2')",idx.getFinalSqlExpression("test_table"));

  }

  void nullSQLExpression() {
    Index idx = new Index();
    idx.setFieldPath("testIdx");
    idx.setFieldName("testIdx");
    idx.setSqlExpression(null);
    assertEquals("testIdx",idx.getFinalSqlExpression("test_table"));
  }
}
