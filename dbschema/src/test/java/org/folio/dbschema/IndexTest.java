package org.folio.dbschema;

import static org.junit.jupiter.api.Assertions.*;

import org.folio.dbschema.Index;
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
  void sqlExpressionQuery() {
    Index index = index("testIdx", "testIdx");
    assertNull(index.getSqlExpressionQuery());
    index.setSqlExpressionQuery("foo($)");
    assertEquals("foo($)", index.getSqlExpressionQuery());
  }

  /**
   * @return Index with fieldName, fieldPath, caseSensitive=true and removeAccents=false
   */
  private Index index(String fieldName, String fieldPath) {
    Index index = new Index();
    index.setFieldName(fieldName);
    index.setFieldPath(fieldPath);
    index.setCaseSensitive(true);
    index.setRemoveAccents(false);
    return index;
  }

  @Test
  void multiFieldNames() {
    Index idx = index("testIdx", "testIdx");
    idx.setMultiFieldNames("field1,field2,field3");
    assertEquals("concat_space_sql(test_table.jsonb->>'field1' , test_table.jsonb->>'field2' , test_table.jsonb->>'field3')",
        idx.getFinalSqlExpression("test_table"));
  }

  @Test
  void fieldNameWithMultipleNames() {
    Index idx = index("name1, name2", "name1, name2");
    assertThrows(UnsupportedOperationException.class, () -> idx.getFinalTruncatedSqlExpression("test_table"));
  }

  @Test
  void arrayMultiFieldNames() {
    Index idx = index("testIdx", "testIdx");
    idx.setMultiFieldNames("field1[*].test,field2[*].name,field3.blah.blah2[*].foo");
    assertEquals("concat_space_sql(concat_array_object_values(test_table.jsonb->'field1','test') ,"
        + " concat_array_object_values(test_table.jsonb->'field2','name') , concat_array_object_values(test_table.jsonb->'field3'->'blah'->'blah2','foo'))",
        idx.getFinalSqlExpression("test_table"));
  }

  @Test
  void sqlExpression() {
    Index idx = index("testIdx", "testIdx");
    idx.setSqlExpression("concat_space_sql()");
    assertEquals("concat_space_sql()", idx.getFinalSqlExpression("test_table"));
  }

  @Test
  void sqlExpressionNotTruncated() {
    Index idx = index("testIdx", "testIdx");
    idx.setSqlExpression("concat_space_sql(a, b)");
    assertEquals("concat_space_sql(a, b)", idx.getFinalTruncatedSqlExpression("test_table"));
  }

  @Test
  void indexExpressionDotsInPath() {
    Index idx = index("testIdx", "dummy");
    idx.setMultiFieldNames("blah.blah2.field1,blah.blah2.field2");
    assertEquals("concat_space_sql(test_table.jsonb->'blah'->'blah2'->>'field1' , test_table.jsonb->'blah'->'blah2'->>'field2')",
        idx.getFinalSqlExpression("test_table"));
  }

  @Test
  void indexExpressionDotsInPathTruncated() {
    Index idx = index("testIdx", "dummy");
    idx.setMultiFieldNames("blah.blah2.field1,blah.blah2.field2");
    assertEquals("left(concat_space_sql(test_table.jsonb->'blah'->'blah2'->>'field1' , test_table.jsonb->'blah'->'blah2'->>'field2'),600)",
        idx.getFinalTruncatedSqlExpression("test_table"));
  }

  @Test
  void fieldPath() {
    Index idx = index("testIdx", "testIdx");
    assertEquals("testIdx", idx.getFinalSqlExpression("test_table"));
  }

  @Test
  void nullSQLExpression() {
    Index idx = index("testIdx", "testIdx");
    idx.setSqlExpression(null);
    assertEquals("left(testIdx,600)", idx.getFinalTruncatedSqlExpression("test_table"));
  }

  @Test
  void sqlsetMultiFieldNamesWrapSetCaseSensitiveSetRemoveAccents() {
    Index idx = index("testIdx", "testIdx");
    idx.setCaseSensitive(true);
    idx.setRemoveAccents(true);
    idx.setMultiFieldNames("test1,test2.test3");
    assertEquals("f_unaccent(concat_space_sql(test_table.jsonb->>'test1' , test_table.jsonb->'test2'->>'test3'))",
        idx.getFinalSqlExpression("test_table"));
  }

  @Test
  void sqlsetMultiFieldNamesWrapSetRemoveAccents() {
    Index idx = index("testIdx", "testIdx");
    idx.setCaseSensitive(false);
    idx.setRemoveAccents(true);
    idx.setMultiFieldNames("test1,test2.test3");
    assertEquals("lower(f_unaccent(concat_space_sql(test_table.jsonb->>'test1' , test_table.jsonb->'test2'->>'test3')))",
        idx.getFinalSqlExpression("test_table"));
  }

  @Test
  void sqlsetMultiFieldNamesWrapSetCaseSensitive() {
    Index idx = index("testIdx", "testIdx");
    idx.setCaseSensitive(true);
    idx.setRemoveAccents(false);
    idx.setMultiFieldNames("test1,test2.test3");
    assertEquals("concat_space_sql(test_table.jsonb->>'test1' , test_table.jsonb->'test2'->>'test3')",
        idx.getFinalSqlExpression("test_table"));
  }

  @Test
  void sqlsetMultiFieldNamesWrap() {
    Index idx = index("testIdx", "testIdx");
    idx.setCaseSensitive(false);
    idx.setRemoveAccents(false);
    idx.setMultiFieldNames("test1,test2.test3");
    assertEquals("lower(concat_space_sql(test_table.jsonb->>'test1' , test_table.jsonb->'test2'->>'test3'))",
        idx.getFinalSqlExpression("test_table"));
  }

  @Test
  void indexStringTypeTrue() {
    Index idx = new Index();
    idx.setFieldName("testField");
    idx.setStringType(true);
    idx.setupIndex();
    assertEquals("left(lower(f_unaccent(jsonb->>'testField')),600)",
        idx.getFinalTruncatedSqlExpression("test_table"));
  }

  @Test
  void indexStringTypeFalse() {
    Index idx = new Index();
    idx.setFieldName("testField");
    idx.setStringType(false);
    idx.setupIndex();
    assertEquals("(jsonb->'testField')", idx.getFinalTruncatedSqlExpression("test_table"));
  }


}
