package org.folio.rest.persist;

import static org.mockito.Mockito.*;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.pgclient.PgConnection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.assertj.core.api.WithAssertions;
import org.folio.cql2pgjson.CQL2PgJSON;
import org.folio.cql2pgjson.exception.CQL2PgJSONException;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.persist.cql.CQLWrapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@ExtendWith(VertxExtension.class)
@Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
public class ConnIT implements WithAssertions {

  static private PostgresClient postgresClient;

  @BeforeAll
  static void setUp(Vertx vertx, VertxTestContext vtc) {
    PostgresClient.setPostgresTester(new PostgresTesterContainer());
    String sql =
        "CREATE ROLE tenant_raml_module_builder PASSWORD 'tenant' NOSUPERUSER NOCREATEDB INHERIT LOGIN;\n" +
        "CREATE SCHEMA tenant_raml_module_builder AUTHORIZATION tenant_raml_module_builder;\n" +
        "GRANT ALL PRIVILEGES ON SCHEMA tenant_raml_module_builder TO tenant_raml_module_builder;\n" +
        "CREATE TABLE tenant_raml_module_builder.t (id UUID PRIMARY KEY , jsonb JSONB NOT NULL);\n" +
        "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA tenant_raml_module_builder TO tenant_raml_module_builder;\n";
    PostgresClient admin = PostgresClient.getInstance(vertx);
    admin.execute(sql)
    .compose(x -> LoadGeneralFunctions.loadFuncs(admin, "tenant_raml_module_builder"))
    .onSuccess(x -> postgresClient = PostgresClient.getInstance(vertx, "tenant"))
    .onComplete(vtc.succeedingThenComplete());
  }

  @AfterAll
  static void tearDown() {
    PostgresClient.stopPostgresTester();
  }

  @BeforeEach
  void truncate(VertxTestContext vtc) {
    postgresClient.execute("TRUNCATE tenant_raml_module_builder.t")
    .onComplete(vtc.succeedingThenComplete());
  }

  private static <T> Handler<AsyncResult<T>> succeedingThenComplete(VertxTestContext vtc, Handler<T> nextHandler) {
    return vtc.succeeding(t -> {
      nextHandler.handle(t);
      vtc.completeNow();
    });
  }

  private static <T> Handler<AsyncResult<T>> failingThenComplete(VertxTestContext vtc, Handler<Throwable> nextHandler) {
    return vtc.failing(t -> {
      nextHandler.handle(t);
      vtc.completeNow();
    });
  }

  private String randomUuid() {
    return UUID.randomUUID().toString();
  }

  public static class Pojo {
    public String id;
    public String key;
    public Pojo() {
      // required by ObjectMapper.readValue for JSON to POJO conversion
    }
    public Pojo(String id, String key) {
      this.id = id;
      this.key = key;
    }
    public String getId() {
      return id;
    }
    @Override
    public String toString() {
      return "\"id\":\"" + id + "\", \"key\":\"" + key + "\"";
    }
  }

  /**
   * Insert Pojo(id, key) into table t and run PostgresClient#withTrans(function).
   */
  private <T> Future<T> with(String id, String key, Function<Conn, Future<T>> function) {
    return postgresClient.save("t", id, new Pojo(id, key))
        .compose(x -> postgresClient.withTrans(function));
  }

  /**
   * Insert Pojo(id1, key1) and Pojo(id2, key2) into table t and run
   * PostgresClient#withTrans(function).
   */
  private <T> Future<T> with(String id1, String key1, String id2, String key2,
      Function<Conn, Future<T>> function) {

    return            postgresClient.save("t", id1, new Pojo(id1, key1))
        .compose(x -> postgresClient.save("t", id2, new Pojo(id2, key2)))
        .compose(x -> postgresClient.withTrans(function));
  }

  @Test
  void getPgConnection() {
    PgConnection pgConnection = mock(PgConnection.class);
    assertThat(new Conn(null, pgConnection).getPgConnection()).isEqualTo(pgConnection);
  }

  @Test
  void durationMsg() {
    assertThat(Conn.durationMsg("desc", "sql", System.nanoTime())).matches("desc timer: sql took [0-9]+ ms");
  }

  @Test
  void getByIdAsString(VertxTestContext vtc) {
    String id = randomUuid();
    with(id, "a", trans -> trans.getByIdAsString("t", id))
    .onComplete(succeedingThenComplete(vtc, s -> assertThat(s).contains("\"key\":\"a\"")));
  }

  <T> void getByIdForUpdate(VertxTestContext vtc, BiFunction<Conn, String, Future<T>> forUpdate) {
    String id = randomUuid();
    with(id, "a", trans -> {
      return forUpdate.apply(trans, id)
      .compose(x -> {
        // upsert to c is blocked until the transaction closes
        postgresClient.upsert("t", id, new Pojo(id, "c"));
        return trans.getByIdAsString("t", id)
            .compose(s -> {
              assertThat(s).contains("\"a\"");
              return trans.upsert("t", id, new Pojo(id, "b"));
            })
            .compose(s -> trans.getByIdAsString("t", id))
            .compose(s -> {
              assertThat(s).contains("\"b\"");
              return Future.succeededFuture();
            });
      });
    })
    .compose(x -> postgresClient.execute("SELECT 1"))  // give some time for upsert
    .compose(x -> postgresClient.getByIdAsString("t", id))
    .onComplete(succeedingThenComplete(vtc, s -> assertThat(s).contains("\"c\"")));
  }

  @Test
  void getByIdForUpdate(VertxTestContext vtc) {
    getByIdForUpdate(vtc, (conn, id) -> conn.getByIdForUpdate("t", id));
  }

  @Test
  void getByIdClassForUpdate(VertxTestContext vtc) {
    getByIdForUpdate(vtc, (conn, id) -> conn.getByIdForUpdate("t", id, Pojo.class));
  }

  @Test
  void getByIdAsStringForUpdate(VertxTestContext vtc) {
    getByIdForUpdate(vtc, (conn, id) -> conn.getByIdAsStringForUpdate("t", id));
  }

  @Test
  void getById(VertxTestContext vtc) {
    String id = randomUuid();
    with(id, "x", trans -> trans.getById("t", id))
    .onComplete(succeedingThenComplete(vtc, jsonObject -> {
      assertThat(jsonObject.getMap()).containsEntry("key", "x");
    }));
  }

  @Test
  void getByIdNotFound(VertxTestContext vtc) {
    postgresClient.withTrans(trans -> trans.getById("t", randomUuid()))
    .onComplete(succeedingThenComplete(vtc, jsonObject -> {
      assertThat(jsonObject).isNull();
    }));
  }

  @Test
  void getByIdPostgresError(VertxTestContext vtc) {
    postgresClient.withTrans(trans -> trans.getById("foo", randomUuid()))
    .onComplete(failingThenComplete(vtc, t -> {
      assertThat(t).hasMessageContainingAll("foo", "does not exist");
    }));
  }

  @ParameterizedTest
  @CsvSource({
    "key=*, 2",
    "key=b, 1",
    "key=x, 0",
  })
  void streamGet(String cql, int total, VertxTestContext vtc) throws CQL2PgJSONException {
    AtomicInteger count = new AtomicInteger();
    AtomicBoolean end = new AtomicBoolean();
    CQLWrapper cqlWrapper = new CQLWrapper(new CQL2PgJSON("jsonb"), cql);
    with(randomUuid(), "a", randomUuid(), "b", trans -> {
      return trans.streamGet("t", Pojo.class, cqlWrapper, as -> {
        PostgresClientStreamResult<Pojo> r = as.result();
        assertThat(r.resultInfo().getTotalRecords()).isEqualTo(total);
        r.handler(x -> count.incrementAndGet());
        r.exceptionHandler(t -> vtc.failNow(t));
        r.endHandler(x -> end.set(true));
      });
    })
    .onComplete(succeedingThenComplete(vtc, x -> {
      assertThat(count.get()).isEqualTo(total);
      assertThat(end.get()).isTrue();
    }));
  }

  @Test
  void streamGetPgConnectionNull(VertxTestContext vtc) throws CQL2PgJSONException {
    Conn conn = new Conn(postgresClient, null);
    conn.streamGet("t", Pojo.class, new CQLWrapper(), i -> {})
    .onComplete(failingThenComplete(vtc, t -> assertThat(t).isInstanceOf(NullPointerException.class)));
  }

  @Test
  void streamGetDoneHandlerWithThrowable(VertxTestContext vtc) throws CQL2PgJSONException {
    CQLWrapper cqlWrapper = new CQLWrapper(new CQL2PgJSON("jsonb"), "key=b");
    with(randomUuid(), "a", randomUuid(), "b", trans -> {
      return trans.streamGet("t", Pojo.class, cqlWrapper,
          info -> info.result().handler(x -> { throw new RuntimeException("foo"); } ));
    })
    .onComplete(failingThenComplete(vtc, t -> assertThat(t).hasMessage("foo")));
  }

}
