package org.folio.rest.persist;

import static org.mockito.Mockito.*;
import java.util.Map;
import java.util.UUID;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.pgclient.PgConnection;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class PostgresClientMockTest {

  @Test
  public void testGetById(TestContext context) {
    String table = "a";
    String id = UUID.randomUUID().toString();
    PostgresClient pc = spy(PostgresClient.testClient());

    // mock empty query result
    @SuppressWarnings("unchecked")
    RowSet<Row> mockRowSet = mock(RowSet.class);
    when(mockRowSet.size()).thenReturn(0);
    @SuppressWarnings("unchecked")
    PreparedQuery<RowSet<Row>> mockPreparedQuery = mock(PreparedQuery.class);
    when(mockPreparedQuery.execute(any(Tuple.class)))
    .thenReturn(Future.succeededFuture(mockRowSet));
    PgConnection mockPgConnection = mock(PgConnection.class);
    when(mockPgConnection.preparedQuery(anyString())).thenReturn(mockPreparedQuery);
    PgPool mockPgPool = mock(PgPool.class);
    when(mockPgPool.getConnection()).thenReturn(Future.succeededFuture(mockPgConnection));
    when(pc.getReaderClient()).thenReturn(mockPgPool);
    SQLConnection mockSQLConnection = new SQLConnection(mockPgConnection, null, null);
    AsyncResult<SQLConnection> mockConn = Future.succeededFuture(mockSQLConnection);
    // tests
    pc.getByIdAsString(table, id, context.asyncAssertSuccess());
    pc.getByIdAsString(mockConn, table, id, context.asyncAssertSuccess());
    pc.getByIdAsStringForUpdate(mockConn, table, id, context.asyncAssertSuccess());
    pc.getById(table, id, context.asyncAssertSuccess());
    pc.getById(mockConn, table, id, context.asyncAssertSuccess());
    pc.getByIdForUpdate(mockConn, table, id, context.asyncAssertSuccess());
    pc.getById(table, id, Map.class, context.asyncAssertSuccess());
    pc.getById(mockConn, table, id, Map.class, context.asyncAssertSuccess());
    pc.getByIdForUpdate(mockConn, table, id, Map.class, context.asyncAssertSuccess());

    // mock query result
    String jsonString = "{\"id\": \"abc\"}";
    when(mockRowSet.size()).thenReturn(1);
    @SuppressWarnings("unchecked")
    RowIterator<Row> mockRowIterator = mock(RowIterator.class);
    Row mockRow = mock(Row.class);
    when(mockRowSet.iterator()).thenReturn(mockRowIterator);
    when(mockRowIterator.next()).thenReturn(mockRow);
    when(mockRow.getValue(anyInt())).thenReturn(jsonString);
    // tests
    pc.getByIdAsString(table, id, assertGetByIdAsString(context));
    pc.getByIdAsString(mockConn, table, id, assertGetByIdAsString(context));
    pc.getByIdAsStringForUpdate(mockConn, table, id, assertGetByIdAsString(context));
    pc.getById(table, id, assertGetByIdAsJson(context));
    pc.getById(mockConn, table, id, assertGetByIdAsJson(context));
    pc.getByIdForUpdate(mockConn, table, id, assertGetByIdAsJson(context));
    pc.getById(table, id, Map.class, assertGetByIdAsObject(context));
    pc.getById(mockConn, table, id, Map.class, assertGetByIdAsObject(context));
    pc.getByIdForUpdate(mockConn, table, id, Map.class, assertGetByIdAsObject(context));

    // test exceptions
    pc.getByIdAsString(Future.failedFuture("fail"), table, id, context.asyncAssertFailure());
    when(mockPgPool.getConnection()).thenReturn(Future.failedFuture("fail"));
    pc.getByIdAsString(table, id, context.asyncAssertFailure());
    when(mockPreparedQuery.execute(any(Tuple.class)))
    .thenReturn(Future.failedFuture("fail"));
    pc.getByIdAsString(mockConn, table, id, context.asyncAssertFailure());
    when(mockPreparedQuery.execute(any(Tuple.class)))
    .thenReturn(Future.succeededFuture(mockRowSet));
    when(mockRow.getValue(anyInt())).thenThrow(new RuntimeException("fail"));
    pc.getByIdAsString(mockConn, table, id, context.asyncAssertFailure());
    pc.getByIdAsString(mockConn, table, "1", context.asyncAssertFailure());
  }

  private Handler<AsyncResult<String>> assertGetByIdAsString(TestContext context) {
    return context.asyncAssertSuccess(r -> {
      context.assertTrue(r.contains("id"));
      context.assertTrue(r.contains("abc"));
    });
  }

  private Handler<AsyncResult<JsonObject>> assertGetByIdAsJson(TestContext context) {
    return context.asyncAssertSuccess(r -> {
      context.assertEquals("abc", r.getString("id"));
    });
  }

  @SuppressWarnings("rawtypes")
  private Handler<AsyncResult<Map>> assertGetByIdAsObject(TestContext context) {
    return context.asyncAssertSuccess(r -> {
      context.assertEquals("abc", r.get("id"));
    });
  }
}
