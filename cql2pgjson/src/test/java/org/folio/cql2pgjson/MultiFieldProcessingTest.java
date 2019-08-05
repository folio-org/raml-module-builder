package org.folio.cql2pgjson;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

import java.util.Arrays;

import org.folio.cql2pgjson.exception.FieldException;
import org.folio.cql2pgjson.exception.QueryValidationException;
import org.folio.cql2pgjson.exception.ServerChoiceIndexesException;
import org.junit.Test;

public class MultiFieldProcessingTest {

  @Test(expected = FieldException.class)
  public void testBadFieldName1() throws FieldException {
    new CQL2PgJSON( Arrays.asList("usersbl.json",""));
  }

  @Test(expected = FieldException.class)
  public void testBadFieldName2() throws FieldException {
    new CQL2PgJSON( Arrays.asList(null,"jsonb"));
  }

  @Test
  public void testApplicationOfFieldNames() throws FieldException, QueryValidationException {
    CQL2PgJSON converter = new CQL2PgJSON( Arrays.asList("field1","field2") );
    assertThat(converter.cql2pgJson("field1.name=v"),
        allOf(containsString("to_tsvector"),
            containsString("field1->>'name'")));
    assertThat(converter.cql2pgJson("field2.name=v"),
        allOf(containsString("to_tsvector"),
            containsString("field2->>'name'")));
    assertThat(converter.cql2pgJson("name=v"),
        allOf(containsString("to_tsvector"),
            containsString("field1->>'name'")));
  }

  @Test
  public void testApplicationOfFieldNamesWithServerChoiceIndexes()
      throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON converter = new CQL2PgJSON( Arrays.asList("field1","field2") );
    converter.setServerChoiceIndexes(Arrays.asList("field1.name"));
    assertThat(converter.cql2pgJson("v"),
        allOf(containsString("to_tsvector"),
            containsString("field1->>'name'")));
    converter.setServerChoiceIndexes(Arrays.asList("field2.name"));
    assertThat(converter.cql2pgJson("v"),
        allOf(containsString("to_tsvector"),
            containsString("field2->>'name'")));
    converter.setServerChoiceIndexes(Arrays.asList("name"));
    assertThat(converter.cql2pgJson("v"),
        allOf(containsString("to_tsvector"),
            containsString("field1->>'name'")));
  }

  @Test
  public void testMixedFieldQuery() throws FieldException, QueryValidationException {
    CQL2PgJSON converter = new CQL2PgJSON( Arrays.asList("field1","field2") );
    assertThat(converter.cql2pgJson(
            "name=Smith"
                + " AND email=gmail.com"
                + " sortBy field2.name/sort.ascending"),
        allOf(containsString("to_tsvector"),
            containsString("field1->>'name'"),
            containsString("Smith"),
            containsString("field1->>'email'"),
            containsString("gmail.com"),
            containsString("ORDER BY lower(f_unaccent(field2->>'name'))")));
  }

}
