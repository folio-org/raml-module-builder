package org.folio.rest.persist.cql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.fail;

import org.folio.cql2pgjson.CQL2PgJSON;
import org.folio.cql2pgjson.exception.FieldException;
import org.folio.cql2pgjson.exception.QueryValidationException;
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
    assertThat(wrapper.toString(), startsWith(" WHERE "));
  }

  @Test
  public void allRecords() throws FieldException {
    CQLWrapper wrapper = new CQLWrapper().setField(cql2pgJson).setQuery("cql.allRecords=1");
    assertThat(wrapper.toString(), is(" WHERE true"));
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
    assertThat(wrapper.toString(), is(" WHERE ((true) and true) or true"));
  }

  @Test
  public void sortBy() throws FieldException {
    CQLWrapper wrapper = new CQLWrapper().setField(cql2pgJson).setQuery("cql.allRecords=1 sortBy name");
    assertThat(wrapper.toString(), stringContainsInOrder(" WHERE true ORDER BY ", "name"));
  }

  @Test
  public void empty() throws FieldException {
    assertThat(new CQLWrapper().setField(cql2pgJson).setQuery(null).toString(), is(""));
  }
}
