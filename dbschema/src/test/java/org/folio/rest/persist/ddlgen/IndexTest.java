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
    assertEquals("table.jsonb" + expectedSql,result.toString());
  }
  @ParameterizedTest
  @CsvSource(value = {
    "a[*].b|concat_array_object_values(table.jsonb->'a','b')",
    "a.b[*].c|concat_array_object_values(table.jsonb->'a'->'b','c')"
  }, delimiter = '|')
  void appendExpandedArrayTerm(String term, String expectedSql) {
    StringBuilder result = new StringBuilder();
    Index.appendExpandedTerm( "table", term,result);
    assertEquals( expectedSql,result.toString());
  }

  @ParameterizedTest
  @CsvSource(value = {
    "a[*]|concat_array_object(table.jsonb->'a')",
    "a.b[*]|concat_array_object(table.jsonb->'a'->'b')"
  }, delimiter = '|')
  void appendExpandedArrayTermNoVariable(String term, String expectedSql) {
    StringBuilder result = new StringBuilder();
    Index.appendExpandedTerm( "table", term,result);
    assertEquals( expectedSql,result.toString());
  }

  @Test
  void multiFieldNames() {
    Index idx = new Index();
    idx.setFieldPath("testIdx");
    idx.setFieldName("testIdx");
    idx.setCaseSensitive(true);
    idx.setRemoveAccents(false);
    idx.setMultiFieldNames("field1,field2,field3");
    assertEquals("concat_space_sql(test_table.jsonb->>'field1' , test_table.jsonb->>'field2' , test_table.jsonb->>'field3')",idx.getFinalSqlExpression("test_table"));

  }

  @Test
  void arrayMultiFieldNames() {
    Index idx = new Index();
    idx.setFieldPath("testIdx");
    idx.setFieldName("testIdx");
    idx.setCaseSensitive(true);
    idx.setRemoveAccents(false);
    idx.setMultiFieldNames("field1[*].test,field2[*].name,field3.blah.blah2[*].foo");
    assertEquals("concat_space_sql(left(concat_array_object_values(test_table.jsonb->'field1','test'),200) , left(concat_array_object_values(test_table.jsonb->'field2','name'),200) , left(concat_array_object_values(test_table.jsonb->'field3'->'blah'->'blah2','foo'),200))",idx.getFinalSqlExpression("test_table"));

  }
  @Test
  void sqlExpression() {
    Index idx = new Index();
    idx.setFieldPath("testIdx");
    idx.setFieldName("testIdx");
    idx.setCaseSensitive(true);
    idx.setRemoveAccents(false);
    idx.setSqlExpression("concat_space_sql()");
    assertEquals("concat_space_sql()",idx.getFinalSqlExpression("test_table"));
  }
  @Test
  void tesIndexExpressionDotsInPath() {
    Index idx = new Index();
    idx.setFieldName("testIdx");
    idx.setCaseSensitive(true);
    idx.setRemoveAccents(false);
    idx.setMultiFieldNames("blah.blah2.field1,blah.blah2.field2");
    assertEquals("concat_space_sql(left(test_table.jsonb->'blah'->'blah2'->>'field1',300) , left(test_table.jsonb->'blah'->'blah2'->>'field2',300))",idx.getFinalSqlExpression("test_table"));
  }
  @Test
  void nullSQLExpression() {
    Index idx = new Index();
    idx.setFieldPath("testIdx");
    idx.setFieldName("testIdx");
    idx.setCaseSensitive(true);
    idx.setRemoveAccents(false);
    idx.setSqlExpression(null);
    assertEquals("testIdx",idx.getFinalSqlExpression("test_table"));
  }

  @Test
  void sqlsetMultiFieldNamesWrapSetCaseSensitiveSetRemoveAccents() {
    Index idx = new Index();
    idx.setFieldPath("testIdx");
    idx.setFieldName("testIdx");
    idx.setCaseSensitive(true);
    idx.setRemoveAccents(true);
    idx.setMultiFieldNames("test1,test2.test3");
    assertEquals("f_unaccent(concat_space_sql(left(test_table.jsonb->>'test1',300) , left(test_table.jsonb->'test2'->>'test3',300)))",idx.getFinalSqlExpression("test_table"));
  }

  @Test
  void sqlsetMultiFieldNamesWrapSetRemoveAccents() {
    Index idx = new Index();
    idx.setFieldPath("testIdx");
    idx.setFieldName("testIdx");
    idx.setCaseSensitive(false);
    idx.setRemoveAccents(true);
    idx.setMultiFieldNames("test1,test2.test3");
    assertEquals("lower(f_unaccent(concat_space_sql(left(test_table.jsonb->>'test1',300) , left(test_table.jsonb->'test2'->>'test3',300))))",idx.getFinalSqlExpression("test_table"));
  }

  @Test
  void sqlsetMultiFieldNamesWrapSetCaseSensitive() {
    Index idx = new Index();
    idx.setFieldPath("testIdx");
    idx.setFieldName("testIdx");
    idx.setCaseSensitive(true);
    idx.setRemoveAccents(false);
    idx.setMultiFieldNames("test1,test2.test3");
    assertEquals("concat_space_sql(left(test_table.jsonb->>'test1',300) , left(test_table.jsonb->'test2'->>'test3',300))",idx.getFinalSqlExpression("test_table"));
  }

  @Test
  void sqlsetMultiFieldNamesWrap() {
    Index idx = new Index();
    idx.setFieldPath("testIdx");
    idx.setFieldName("testIdx");
    idx.setCaseSensitive(false);
    idx.setRemoveAccents(false);
    idx.setMultiFieldNames("test1,test2.test3");
    assertEquals("lower(concat_space_sql(left(test_table.jsonb->>'test1',300) , left(test_table.jsonb->'test2'->>'test3',300)))",idx.getFinalSqlExpression("test_table"));
  }
}
