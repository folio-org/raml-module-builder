package org.folio.rest.persist.cql;

import static org.junit.Assert.assertTrue;

import org.folio.rest.persist.cql.CQLWrapper;
import org.junit.Test;
import org.z3950.zing.cql.cql2pgjson.CQL2PgJSON;
import org.z3950.zing.cql.cql2pgjson.FieldException;

public class CQLWrapperTest {
  @Test
  public void returnsWhere() throws FieldException {
    CQLWrapper wrapper = new CQLWrapper().setField(new CQL2PgJSON("field")).setQuery("name=miller");
    String result = wrapper.toString();
    assertTrue(result.startsWith(" "));
    assertTrue(result.contains(" WHERE "));
  }

  @Test(expected = IllegalStateException.class)
  public void invalidCQL() throws FieldException {
    CQLWrapper wrapper = new CQLWrapper().setField(new CQL2PgJSON("field")).setQuery("or name=miller");
    wrapper.toString();
  }
}
