package org.folio.rest.persist;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import io.vertx.pgclient.PgConnection;
import org.junit.jupiter.api.Test;

public class SQLConnectionTest {
  @Test
  void closeConnectionAfterTimerException() {
    PgConnection pgConnection = mock(PgConnection.class);
    assertThrows(NullPointerException.class, () -> new SQLConnection(pgConnection, null, 5L).close(null));
    verify(pgConnection).close();
  }
}
