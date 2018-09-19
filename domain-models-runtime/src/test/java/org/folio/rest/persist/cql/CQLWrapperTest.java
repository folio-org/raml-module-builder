package org.folio.rest.persist.cql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.fail;

import org.folio.rest.persist.cql.CQLWrapper;
import org.junit.Test;
import org.z3950.zing.cql.cql2pgjson.CQL2PgJSON;
import org.z3950.zing.cql.cql2pgjson.FieldException;
import org.z3950.zing.cql.cql2pgjson.QueryValidationException;

public class CQLWrapperTest {
  @Test
  public void returnsWhere() throws FieldException {
    CQLWrapper wrapper = new CQLWrapper().setField(new CQL2PgJSON("field")).setQuery("name=miller");
    assertThat(wrapper.toString(), startsWith(" WHERE "));
  }

  @Test
  public void allRecords() throws FieldException {
    CQLWrapper wrapper = new CQLWrapper().setField(new CQL2PgJSON("field")).setQuery("cql.allRecords=1");
    assertThat(wrapper.toString(), is(" WHERE true"));
  }

  @Test(expected = IllegalStateException.class)
  public void invalidCQL() throws FieldException {
    CQLWrapper wrapper = new CQLWrapper().setField(new CQL2PgJSON("field")).setQuery("or name=miller");
    wrapper.toString();
  }

  @Test
  public void invalidCQLvalidation() throws FieldException {
    CQLWrapper wrapper = new CQLWrapper().setField(new CQL2PgJSON("field")).setQuery("or name=miller");
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
    CQLWrapper wrapper = new CQLWrapper().setField(new CQL2PgJSON("field")).setQuery("");
    try {
      wrapper.toString();
      fail("exception expected");
    }
    catch (CQLQueryValidationException e) {
      assertThat(e.getCause(), is(instanceOf(QueryValidationException.class)));
    }
  }

  @Test
  public void wrap() throws FieldException {
    CQLWrapper wrapper = new CQLWrapper().setField(new CQL2PgJSON("field")).setQuery("cql.allRecords=1");
    wrapper.addWrapper(wrapper);
    wrapper.addWrapper(wrapper, "or");
    assertThat(wrapper.toString(), is(" WHERE true and true or true"));
  }

  @Test
  public void sortBy() throws FieldException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("field");
    CQLWrapper wrapper = new CQLWrapper().setField(cql2pgJson).setQuery("cql.allRecords=1 sortBy name");
    assertThat(wrapper.toString(), stringContainsInOrder(" WHERE true ORDER BY ", "name"));
  }
}
