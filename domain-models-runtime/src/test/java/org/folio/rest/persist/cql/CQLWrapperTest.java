package org.folio.rest.persist.cql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

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
    assertThat(wrapper.toString(), matchesPattern("^ WHERE true *$"));
  }

  @Test(expected = IllegalStateException.class)
  public void invalidCQL() throws FieldException {
    CQLWrapper wrapper = new CQLWrapper().setField(new CQL2PgJSON("field")).setQuery("or name=miller");
    wrapper.toString();
  }
}
