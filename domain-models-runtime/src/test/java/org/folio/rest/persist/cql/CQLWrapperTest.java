package org.folio.rest.persist.cql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.fail;

import org.folio.cql2pgjson.CQL2PgJSON;
import org.folio.cql2pgjson.exception.FieldException;
import org.folio.cql2pgjson.exception.QueryValidationException;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.Criteria.Limit;
import org.folio.rest.persist.Criteria.Offset;
import org.junit.BeforeClass;
import org.junit.Test;

public class CQLWrapperTest {
  static CQL2PgJSON cql2pgJson;

  @BeforeClass
  public static void setUpCLass() throws FieldException {
    cql2pgJson = new CQL2PgJSON("field");
  }

  @Test
  public void returnsWhere() throws FieldException {
    CQLWrapper wrapper = new CQLWrapper().setField(cql2pgJson).setQuery("name=miller");
    assertThat(wrapper.toString(), startsWith("WHERE "));
  }

  @Test
  public void allRecords() throws FieldException {
    CQLWrapper wrapper = new CQLWrapper().setField(cql2pgJson).setQuery("cql.allRecords=1");
    assertThat(wrapper.toString(), is("WHERE true"));
  }

  @Test
  public void setWhereClause() throws FieldException {
    CQLWrapper wrapper = new CQLWrapper().setWhereClause("WHERE false");
    assertThat(wrapper.toString(), is("WHERE false"));
  }

  @Test(expected = IllegalStateException.class)
  public void invalidCQL() throws FieldException {
    CQLWrapper wrapper = new CQLWrapper().setField(cql2pgJson).setQuery("or name=miller");
    wrapper.toString();
  }

  @Test
  public void invalidCQLvalidation() throws FieldException {
    CQLWrapper wrapper = new CQLWrapper().setField(cql2pgJson).setQuery("or name=miller");
    try {
      wrapper.toString();
      fail("exception expected");
    }
    catch (CQLQueryValidationException e) {
      assertThat(e.getMessage(), allOf(containsString("ParseException"), containsString("unexpected relation")));
    }
  }

  @Test
  public void invalidCQLsortby() throws FieldException {
    CQLWrapper wrapper = new CQLWrapper().setField(cql2pgJson).setQuery("name=miller sortby");
    try {
      wrapper.getOrderByClause();
      fail("exception expected");
    }
    catch (CQLQueryValidationException e) {
      assertThat(e.getMessage(), allOf(containsString("ParseException"), containsString("no sort keys")));
    }
  }

  @Test
  public void emptyCqlQueryValidationException() throws FieldException {
    CQLWrapper wrapper = new CQLWrapper().setField(cql2pgJson).setQuery("");
    try {
      wrapper.toString();
      fail("exception expected");
    }
    catch (CQLQueryValidationException e) {
      assertThat(e.getCause(), is(instanceOf(QueryValidationException.class)));
    }
  }

  @Test
  public void constructor4Nothing() {
    CQLWrapper cqlWrapper = new CQLWrapper();
    assertThat(cqlWrapper.toString(), is(""));
    assertThat(cqlWrapper.getQuery(), is(nullValue()));
  }

  @Test
  public void constructor4LimitNoOffset() throws FieldException {
    assertThat(new CQLWrapper(cql2pgJson, "cql.allRecords=1", 5, -1).toString(),
        allOf(containsString("LIMIT 5"), not(containsString("OFFSET"))));
  }

  @Test
  public void constructor4NoLimitSomeOffset() throws FieldException {
    assertThat(new CQLWrapper(cql2pgJson, "cql.allRecords=1", -1, 7).toString(),
        allOf(not(containsString("LIMIT")), containsString("OFFSET 7")));
  }

  @Test
  public void constructor4NoLimitNoOffset() throws FieldException {
    CQLWrapper cqlWrapper = new CQLWrapper(cql2pgJson, "cql.allRecords=1", -1, -1);
    assertThat(cqlWrapper.toString(),
        allOf(not(containsString("LIMIT")), not(containsString("OFFSET"))));
    assertThat(cqlWrapper.getLimit().get(), is(-1));
    assertThat(cqlWrapper.getOffset().get(), is(-1));
  }

  @Test
  public void constructor4Limit0Offset0() throws FieldException {
    CQLWrapper cqlWrapper = new CQLWrapper(cql2pgJson, "cql.allRecords=1", 0, 0);
    assertThat(cqlWrapper.toString(),
        allOf(containsString("LIMIT 0"), containsString("OFFSET 0")));
    assertThat(cqlWrapper.getLimit().get(), is(0));
    assertThat(cqlWrapper.getOffset().get(), is(0));
  }

  @Test
  public void setLimitSetOffset() throws FieldException {
    CQLWrapper cqlWrapper = new CQLWrapper(cql2pgJson, "cql.allRecords=1")
        .setLimit(new Limit(9)).setOffset(new Offset(11));
    assertThat(cqlWrapper.toString(),
        allOf(containsString("LIMIT 9"), containsString("OFFSET 11")));
    assertThat(cqlWrapper.getLimit().get(), is(9));
    assertThat(cqlWrapper.getOffset().get(), is(11));
  }

  @Test
  public void wrap() throws FieldException {
    CQLWrapper wrapper = new CQLWrapper().setField(cql2pgJson).setQuery("cql.allRecords=1");
    wrapper.addWrapper(wrapper);
    wrapper.addWrapper(wrapper, "or");
    assertThat(wrapper.toString(), is("WHERE ((true) and true) or true"));
    assertThat(wrapper.getField(), is(cql2pgJson));
  }

  @Test
  public void wrapWithSorting() throws FieldException {
    CQLWrapper wrapper = new CQLWrapper(cql2pgJson, "author = abc sortby title");
    CQLWrapper wrapper2 = new CQLWrapper(cql2pgJson, "year = 1990 sortby date");
    wrapper.addWrapper(wrapper2);
    assertThat(wrapper.toString(), stringContainsInOrder("WHERE", "author", "abc", "year", "1990", "ORDER BY", "title"));
  }

  @Test
  public void wrapWithEmpty() throws FieldException {
    CQLWrapper wrapper = new CQLWrapper().setField(cql2pgJson);
    CQLWrapper wrapper2 = new CQLWrapper(cql2pgJson, "cql.allRecords=1");
    wrapper.addWrapper(wrapper2);
    assertThat(wrapper.toString(), is("WHERE true"));
  }

  @Test
  public void wrapWithEmpty2() throws FieldException {
    CQLWrapper wrapper = new CQLWrapper();
    CQLWrapper wrapper2 = new CQLWrapper(cql2pgJson, "cql.allRecords=1");
    wrapper.addWrapper(wrapper2);
    assertThat(wrapper.toString(), is("WHERE true"));
  }

  @Test
  public void sortBy() throws FieldException {
    CQLWrapper wrapper = new CQLWrapper().setField(cql2pgJson).setQuery("cql.allRecords=1 sortBy name");
    assertThat(wrapper.toString(), stringContainsInOrder("WHERE true ORDER BY ", "name"));
  }

  @Test
  public void empty() throws FieldException {
    assertThat(new CQLWrapper().setField(cql2pgJson).setQuery(null).toString(), is(""));
  }

  @Test
  public void combo() throws FieldException {
    CQLWrapper wrapperCql = new CQLWrapper().setField(cql2pgJson).setQuery("cql.allRecords=1 sortBy name");
    Criterion criterion = new Criterion().addCriterion(new Criteria().addField("id").setOperation("=").setVal("42"));
    CQLWrapper wrapperCriterion = new CQLWrapper(criterion);
    CQLWrapper wrapperWhere = new CQLWrapper().setWhereClause("where false");
    CQLWrapper wrapperNone = new CQLWrapper();

    assertThat(wrapperCql.getType(), is("CQL"));
    assertThat(wrapperCriterion.getType(), is("CRITERION"));
    assertThat(wrapperWhere.getType(), is("WHERE"));
    assertThat(wrapperNone.getType(), is("NONE"));

    assertThat(wrapperCql.toString(), is("WHERE true ORDER BY left(lower(f_unaccent(field->>'name')),600), lower(f_unaccent(field->>'name'))"));
    assertThat(wrapperCriterion.toString(), is("WHERE (jsonb->>id) = '42'"));
    assertThat(wrapperWhere.toString(), is("where false"));

    assertThat(wrapperCql.getQuery(), is("cql.allRecords=1 sortBy name"));
    assertThat(wrapperCriterion.getQuery().trim(), is("WHERE (jsonb->>id) = '42'"));
    assertThat(wrapperWhere.getQuery(), is("where false"));

    wrapperCql.addWrapper(wrapperNone);
    assertThat(wrapperCql.toString(), is("WHERE true ORDER BY left(lower(f_unaccent(field->>'name')),600), lower(f_unaccent(field->>'name'))"));
    wrapperCql.addWrapper(wrapperCriterion);
    assertThat(wrapperCql.toString(), is("WHERE (true) and (jsonb->>id) = '42' ORDER BY left(lower(f_unaccent(field->>'name')),600), lower(f_unaccent(field->>'name'))"));
    wrapperCriterion.addWrapper(wrapperCql, "or");
    assertThat(wrapperCriterion.toString(), is("WHERE ((jsonb->>id) = '42') or true"));
  }

}
