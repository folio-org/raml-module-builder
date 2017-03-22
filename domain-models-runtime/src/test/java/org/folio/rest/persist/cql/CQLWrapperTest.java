package org.folio.rest.persist.cql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.fail;

import org.folio.rest.persist.cql.CQLWrapper;
import org.junit.Test;
import org.z3950.zing.cql.cql2pgjson.CQL2PgJSON;
import org.z3950.zing.cql.cql2pgjson.FieldException;

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
  public void wrap() throws FieldException {
    CQLWrapper wrapper = new CQLWrapper().setField(new CQL2PgJSON("field")).setQuery("cql.allRecords=1");
    wrapper.addWrapper(wrapper);
    wrapper.addWrapper(wrapper, "or");
    assertThat(wrapper.toString(), is(" WHERE true and true or true"));
  }

}
