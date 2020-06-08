package org.folio.rest.persist;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Map;

import io.vertx.pgclient.PgException;
import org.junit.jupiter.api.Test;

public class PgExceptionFacadeTest {
  @Test
  void getter() {
    PgExceptionFacade f = new PgExceptionFacade(new PgException("msg", "FINAL WARNING", "22P02", "very bad"));
    assertThat(f.getMessage(), is("msg"));
    assertThat(f.getSeverity(), is("FINAL WARNING"));
    assertThat(f.getSqlState(), is("22P02"));
    assertThat(f.getDetail(), is("very bad"));
    Map<Character,String> fields = f.getFields();
    assertThat(fields.get('M'), is("msg"));
    assertThat(fields.get('S'), is("FINAL WARNING"));
    assertThat(fields.get('C'), is("22P02"));
    assertThat(fields.get('D'), is("very bad"));
  }

  @Test
  void isInvalidTextRepresentation() {
    PgExceptionFacade f = new PgExceptionFacade(new PgException(null, null, "22P02", null));
    assertThat(f.isInvalidTextRepresentation(), is(true));
    PgExceptionFacade f2 = new PgExceptionFacade(new PgException(null, null, "22P03", null));
    assertThat(f2.isInvalidTextRepresentation(), is(false));
  }
}
