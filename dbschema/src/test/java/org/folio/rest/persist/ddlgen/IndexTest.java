package org.folio.rest.persist.ddlgen;

import static org.junit.jupiter.api.Assertions.*;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class IndexTest {

  @ParameterizedTest
  @CsvSource({
    "a    , ->>'a'",
    "a.b  , ->'a'->>'b'",
    "a.b.c, ->'a'->'b'->>'c'",
  })
  void appendExpandedSimpleTerm(String term, String expectedSql) {
    StringBuilder result = new StringBuilder();
    Index.appendExpandedTerm( "table", term,result);
    assertEquals(result.toString(), "table.jsonb" + expectedSql);
  }
  @ParameterizedTest
  @CsvSource({
    "a[*].b  , concat_array_object_values(table.jsonb->'a')",
    "a.b[*].c, concat_array_object_values(table.jsonb->'a'->'b')",
  })
  void appendExpandedArrayTerm(String term, String expectedSql) {
    StringBuilder result = new StringBuilder();
    Index.appendExpandedTerm( "table", term,result);
    assertEquals(result.toString(),  expectedSql);
  }
  @Test
  void multiFieldNames() {
    Index idx = new Index();
    idx.setFieldPath("testIdx");
    idx.setFieldName("testIdx");
    idx.setMultiFieldNames("field1,field2,field3");
    assertEquals("concat_space_sql(test_table.jsonb->>'field1' , test_table.jsonb->>'field2' , test_table.jsonb->>'field3')",idx.getFinalSqlExpression("test_table"));

  }

  @Test
  void arrayMultiFieldNames() {
    Index idx = new Index();
    idx.setFieldPath("testIdx");
    idx.setFieldName("testIdx");
    idx.setMultiFieldNames("field1[*].test,field2[*].name,field3.blah.blah2[*].foo");
    assertEquals("concat_space_sql(concat_array_object_values(test_table.jsonb->'field1','test') , concat_array_object_values(test_table.jsonb->'field2','name') , concat_array_object_values(test_table.jsonb->'field3'->'blah'->'blah2','foo'))",idx.getFinalSqlExpression("test_table"));

  }
  @Test
  void sqlExpression() {
    Index idx = new Index();
    idx.setFieldPath("testIdx");
    idx.setFieldName("testIdx");
    idx.setSqlExpression("concat_space_sql()");
    assertEquals("concat_space_sql()",idx.getFinalSqlExpression("test_table"));
  }
  @Test
  void tesIndexExpressionDotsInPath() {
    Index idx = new Index();
    idx.setFieldName("testIdx");
    idx.setMultiFieldNames("blah.blah2.field1,blah.blah2.field2");
    assertEquals("concat_space_sql(test_table.jsonb->'blah'->'blah2'->>'field1' , test_table.jsonb->'blah'->'blah2'->>'field2')",idx.getFinalSqlExpression("test_table"));
  }
  @Test
  void nullSQLExpression() {
    Index idx = new Index();
    idx.setFieldPath("testIdx");
    idx.setFieldName("testIdx");
    idx.setSqlExpression(null);
    assertEquals("testIdx",idx.getFinalSqlExpression("test_table"));
  }
}
