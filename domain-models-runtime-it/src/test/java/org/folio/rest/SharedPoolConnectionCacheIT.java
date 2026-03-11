package org.folio.rest;

import static io.restassured.RestAssured.given;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.oneOf;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.restassured.RestAssured;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.cache.CachedPgConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class SharedPoolConnectionCacheIT extends ApiTestBase {
  private static final String TENANT_A = "cachetenanta";
  private static final String TENANT_B = "cachetenantb";
  private static final String TENANT_C = "cachetenantc";
  private static final List<String> TENANTS = List.of(TENANT_A, TENANT_B, TENANT_C);
  private static final String MODULE_TO = "mod-api-1.0.0";

  @BeforeEach
  void beforeEach() {
    PostgresClient.setSharedPgPool(true);
    PostgresClient.closeAllClients();
    TENANTS.forEach(this::recreateTenant);
  }

  @AfterEach
  void afterEach() {
    try {
      TENANTS.forEach(this::purgeTenant);
    } finally {
      PostgresClient.closeAllClients();
      PostgresClient.setSharedPgPool(false);
    }
  }

  @Test
  void sharedPoolEnabledWithThreeTenants() {
    assertThat(PostgresClient.isSharedPool(), is(true));

    for (String tenant : TENANTS) {
      var connection = getCachedConnection(tenant);
      assertThat(connection.getTenantId(), is(tenant));
      await(connection.close());
    }
  }

  @Test
  void shouldNotCreateNestedWrappersUnderThreeTenantChurn() {
    int iterations = Integer.getInteger("shared.pool.churn.iterations", 60);

    for (int i = 0; i < iterations; i++) {
      String tenant = TENANTS.get(i % TENANTS.size());
      CachedPgConnection connection = getCachedConnection(tenant);

      assertThat(cachedWrapperDepth(connection), is(1));
      assertThat(connection.getWrappedConnection(), is(not(instanceOf(CachedPgConnection.class))));

      await(connection.close());
    }
  }

  @Test
  void shouldRecoverFromRecycleFailureWithoutMetadataCorruption() {
    CachedPgConnection initial = getCachedConnection(TENANT_A);
    await(initial.close());

    String nonexistentTenant = "missing_" + System.nanoTime();
    Throwable failure = awaitFailure(PostgresClient.getInstance(vertx, nonexistentTenant).getConnection());
    assertThat(failure, is(notNullValue()));

    CachedPgConnection recovered = getCachedConnection(TENANT_A);
    assertThat(recovered.getTenantId(), is(TENANT_A));
    assertCurrentSchema(recovered, TENANT_A + "_raml_module_builder");
    await(recovered.close());
  }

  @Test
  void shouldAvoidPermissionDeniedDuringThreeTenantHttpChurn() {
    int iterations = Integer.getInteger("shared.pool.http.iterations", 12);

    for (int i = 0; i < iterations; i++) {
      for (String tenant : TENANTS) {
        assertTenantHttpCrudHealthy(tenant, i);
      }
    }
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void shouldHandleParallelBurstMultiTenantChurn() {
    int workers = Integer.getInteger("shared.pool.parallel.workers", 3);
    int operationsPerWorker = Integer.getInteger("shared.pool.parallel.operations", 5);
    ExecutorService executor = Executors.newFixedThreadPool(workers);

    try {
      CompletableFuture<?>[] tasks = new CompletableFuture<?>[workers];
      for (int worker = 0; worker < workers; worker++) {
        final int workerIndex = worker;
        tasks[worker] = CompletableFuture.runAsync(() -> {
          for (int operation = 0; operation < operationsPerWorker; operation++) {
            String tenant = TENANTS.get((workerIndex + operation) % TENANTS.size());
            assertTenantHttpCrudHealthy(tenant, workerIndex * operationsPerWorker + operation);
          }
        }, executor);
      }

      CompletableFuture.allOf(tasks).get(3, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException | TimeoutException e) {
      if (e.getCause() instanceof AssertionError assertionError) {
        throw assertionError;
      }
      throw new RuntimeException(e);
    } finally {
      executor.shutdownNow();
    }

    int verificationIterations = workers * operationsPerWorker;
    for (int i = 0; i < verificationIterations; i++) {
      String tenant = TENANTS.get(i % TENANTS.size());
      CachedPgConnection connection = getCachedConnection(tenant);
      assertConnectionHealthy(connection, tenant);
    }
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void shouldRecoverDuringFailureInterleavedMultiTenantChurn() {
    int iterations = Integer.getInteger("shared.pool.failure.interleaved.iterations", 45);
    int failureInterval = Integer.getInteger("shared.pool.failure.interval", 5);
    int failuresObserved = 0;

    for (int i = 0; i < iterations; i++) {
      if (i % failureInterval == 0) {
        String missingTenant = "missing_interleaved_" + i + "_" + System.nanoTime();
        Throwable failure = awaitFailure(PostgresClient.getInstance(vertx, missingTenant).getConnection());
        assertThat(failure, is(notNullValue()));
        failuresObserved++;
      }

      String tenant = TENANTS.get(i % TENANTS.size());
      assertTenantHttpCrudHealthy(tenant, i);

      CachedPgConnection connection = getCachedConnection(tenant);
      assertThat(connection.getTenantId(), is(tenant));
      assertThat(cachedWrapperDepth(connection), is(1));
      assertCurrentSchema(connection, tenant + "_raml_module_builder");
      await(connection.close());
    }

    assertThat(failuresObserved, is(greaterThan(0)));
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void shouldKeepTenantDataIsolatedDuringFailureAndChurn() {
    String stableId = randomUuid();
    createBee(TENANT_A, stableId, "stable-record");

    int iterations = Integer.getInteger("shared.pool.isolation.iterations", 20);
    for (int i = 0; i < iterations; i++) {
      if (i % 4 == 0) {
        String missingTenant = "missing_isolation_" + i + "_" + System.nanoTime();
        assertThat(awaitFailure(PostgresClient.getInstance(vertx, missingTenant).getConnection()), is(notNullValue()));
      }

      String churnTenant = (i % 2 == 0) ? TENANT_B : TENANT_C;
      assertTenantHttpCrudHealthy(churnTenant, i);
    }

    assertBeeStatus(TENANT_A, stableId, 200);
    assertBeeStatus(TENANT_B, stableId, 404);
    assertBeeStatus(TENANT_C, stableId, 404);

    deleteBee(TENANT_A, stableId);
  }

  private void recreateTenant(String tenant) {
    purgeTenant(tenant);
    TenantAttributes attributes = new TenantAttributes().withModuleTo(MODULE_TO);
    String location = given(r)
        .header("x-okapi-tenant", tenant)
        .header("x-okapi-url-to", "http://localhost:" + RestAssured.port)
        .contentType("application/json")
        .body(Json.encode(attributes))
        .when()
        .post("/_/tenant")
        .then()
        .statusCode(201)
        .extract()
        .header("Location");

    given(r)
        .header("x-okapi-tenant", tenant)
        .header("x-okapi-url-to", "http://localhost:" + RestAssured.port)
        .when()
        .get(location + "?wait=5000")
        .then()
        .statusCode(200);
  }

  private void purgeTenant(String tenant) {
    TenantAttributes attributes = new TenantAttributes().withPurge(true);
    given(r)
        .header("x-okapi-tenant", tenant)
        .header("x-okapi-url-to", "http://localhost:" + RestAssured.port)
        .contentType("application/json")
        .body(Json.encode(attributes))
        .when()
        .post("/_/tenant")
        .then()
        .statusCode(is(oneOf(204, 404)));
  }

  private CachedPgConnection getCachedConnection(String tenant) {
    PgConnection connection = awaitOnEventLoop(
        () -> PostgresClient.getInstance(vertx, tenant).getConnection());
    assertThat(connection, is(instanceOf(CachedPgConnection.class)));
    return (CachedPgConnection) connection;
  }

  private int cachedWrapperDepth(CachedPgConnection connection) {
    int depth = 0;
    PgConnection current = connection;
    while (current instanceof CachedPgConnection) {
      depth++;
      current = ((CachedPgConnection) current).getWrappedConnection();
      if (depth > 10) {
        throw new AssertionError("Cached wrapper depth exceeded 10");
      }
    }
    return depth;
  }

  private void assertTenantHttpCrudHealthy(String tenant, int marker) {
    String id = randomUuid();
    createBee(tenant, id, "bee-" + tenant + "-" + marker);

    given(r)
        .header("x-okapi-tenant", tenant)
        .when()
        .get("/bees/bees/" + id)
        .then()
        .statusCode(200);

    deleteBee(tenant, id);
  }

  private void createBee(String tenant, String id, String name) {
    JsonObject payload = new JsonObject()
        .put("id", id)
        .put("name", name)
        .put("generatedStatus", "ignored");

    given(r)
        .header("x-okapi-tenant", tenant)
        .body(payload.encode())
        .when()
        .post("/bees/bees")
        .then()
        .statusCode(201);
  }

  private void deleteBee(String tenant, String id) {
    given(r)
        .header("x-okapi-tenant", tenant)
        .when()
        .delete("/bees/bees/" + id)
        .then()
        .statusCode(204);
  }

  private void assertBeeStatus(String tenant, String id, int statusCode) {
    given(r)
        .header("x-okapi-tenant", tenant)
        .when()
        .get("/bees/bees/" + id)
        .then()
        .statusCode(statusCode);
  }

  private void assertCurrentSchema(CachedPgConnection connection, String expectedSchema) {
    RowSet<Row> result = awaitOnEventLoop(() -> connection.query("SELECT current_schema").execute());
    Row row = result.iterator().next();
    assertThat(row.getString(0), is(expectedSchema));
  }

  private void assertConnectionHealthy(CachedPgConnection connection, String tenant) {
    assertThat(cachedWrapperDepth(connection), is(1));
    RowSet<Row> pingResult = awaitOnEventLoop(() -> connection.query("SELECT 1").execute());
    assertThat(pingResult.iterator().next(), is(notNullValue()));
    assertCurrentSchema(connection, tenant + "_raml_module_builder");
    awaitOnEventLoop(connection::close);
  }

  private <T> T awaitOnEventLoop(Supplier<Future<T>> supplier) {
    Promise<T> promise = Promise.promise();
    vertx.runOnContext(ignored -> supplier.get().onComplete(promise));
    return await(promise.future());
  }

  private Throwable awaitFailure(Future<?> future) {
    try {
      await(future);
      throw new AssertionError("Expected Future to fail");
    } catch (AssertionError e) {
      throw e;
    } catch (RuntimeException e) {
      return e.getCause() != null ? e.getCause() : e;
    }
  }

  private <T> T await(Future<T> future) {
    try {
      return future.toCompletionStage().toCompletableFuture().get(20, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }
}
