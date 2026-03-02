package org.folio.rest.persist.cache;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.pgclient.impl.PgConnectionImpl;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.impl.TenantAPI;
import org.folio.rest.impl.TenantHelper;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.PostgresClientHelper;
import org.folio.rest.tools.utils.VertxUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

@ExtendWith(VertxExtension.class)
class CachedConnectionManagerIT extends TenantHelper {

  @BeforeAll
  static void setUpClass() {
    vertx = VertxUtils.getVertxWithExceptionHandler();
    PostgresClient.setPostgresTester(new PostgresTesterContainer());
  }

  @AfterAll
  static void afterClass() {
    PostgresClientHelper.setSharedPgPool(false);
  }

  @BeforeEach
  void beforeEachTest() {
    PostgresClientHelper.setSharedPgPool(true);
    PostgresClient.clearConnectionCache();
  }

  @Test
  void shouldNotCreateNestedWrappers(VertxTestContext testContext) {
    var originalSessionId = new AtomicReference<UUID>();

    prepareTenants("tenant1", "tenant2")
        .compose(v -> getConnection("tenant1"))
        .compose(conn1 -> {
          originalSessionId.set(conn1.getSessionId());
          return closeConnection(conn1); // Return it to the cache, so it can be reused as is for tenant2.
        })
        .compose(v -> getConnection("tenant2"))
        .compose(conn2 -> {
          assertThat(conn2.getSessionId(), is(originalSessionId.get()));
          assertThat(conn2.getWrappedConnection().getClass(), is(PgConnectionImpl.class));
          return conn2.close();
        })
        .onComplete(testContext.succeedingThenComplete());
  }

  @Test
  void shouldUpdateConnectionMetadataAfterConnectionSucceeds(VertxTestContext testContext) {
    var originalSessionId = new AtomicReference<UUID>();
    var firstConnectionRef = new AtomicReference<CachedPgConnection>();

    prepareTenants("tenant1", "tenant2")
        .compose(v -> getConnection("tenant1"))
        .compose(conn1 -> {
          firstConnectionRef.set(conn1);
          originalSessionId.set(conn1.getSessionId());

          assertThat(conn1.getTenantId(), is("tenant1"));
          assertThat(conn1.getSchemaName(), is("tenant1_raml_module_builder"));

          return closeConnection(conn1, () -> {
            // Before recycling it should still have the original metadata.
            assertThat(firstConnectionRef.get().getTenantId(), is("tenant1"));
            assertThat(firstConnectionRef.get().getSchemaName(), is("tenant1_raml_module_builder"));
          });
        })
        .compose(v -> getConnection("tenant2"))
        .compose(conn2 -> {
          assertThat(conn2.getSessionId(), is(originalSessionId.get()));
          assertThat(conn2.getTenantId(), is("tenant2"));
          assertThat(conn2.getSchemaName(), is("tenant2_raml_module_builder"));
          return conn2.close();
        })
        .onComplete(testContext.succeedingThenComplete());
  }

  @Test
  void shouldRecoverOnSetRoleFailure(VertxTestContext testContext) {
    var originalSessionId = new AtomicReference<UUID>();

    prepareTenants("tenant1")
        .compose(v -> getConnection("tenant1"))
        .compose(conn1 -> {
          originalSessionId.set(conn1.getSessionId());
          return recoverAfterFailure("tenant1", "nonexistent", conn1);
        })
        .compose(conn3 -> {
          assertThat(conn3.getSessionId(), is(originalSessionId.get()));
          assertThat(conn3.getTenantId(), is("tenant1"));
          assertThat(conn3.getSchemaName(), is("tenant1_raml_module_builder"));
          return conn3.close();
        })
        .onComplete(testContext.succeedingThenComplete());
  }

  @Test
  void shouldRecoverOnSetRoleFailureForNewConnection(VertxTestContext testContext) {
    prepareTenants("diku")
        .compose(v -> expectFailure(PostgresClient.getInstance(vertx, "nonexistent").getConnection()))
        .compose(ignored -> getConnection("diku"))
        .compose(conn -> {
          assertThat(conn.getTenantId(), is("diku"));
          return conn.close();
        })
        .onComplete(testContext.succeedingThenComplete());
  }

  private Future<CachedPgConnection> getConnection(String tenant) {
    return PostgresClient.getInstance(vertx, tenant)
        .getConnection()
        .map(conn -> (CachedPgConnection) conn);
  }

  private Future<CachedPgConnection> recoverAfterFailure(String originalTenant, String failingTenant, CachedPgConnection cachedConn) {
    return closeConnection(cachedConn)
        .compose(v -> expectFailure(PostgresClient.getInstance(vertx, failingTenant).getConnection()))
        .compose(v -> getConnection(originalTenant));
  }

  private Future<Void> closeConnection(CachedPgConnection connection) {
    return closeConnection(connection, null);
  }

  private Future<Void> closeConnection(CachedPgConnection connection, Runnable afterClose) {
    return connection.close().onSuccess(v -> {
      if (afterClose != null) {
        afterClose.run();
      }
    });
  }

  private Future<Void> prepareTenants(String... tenants) {
    var api = new TenantAPI();
    Future<Void> chain = Future.succeededFuture();
    for (String tenant : tenants) {
      chain = chain.compose(v -> tenantPostSync(api, null, tenant));
    }
    return chain;
  }

  private static <T> Future<Void> expectFailure(Future<T> future) {
    Promise<Void> promise = Promise.promise();
    future.onComplete(ar -> {
      if (ar.succeeded()) {
        promise.fail("Expected future to fail, but it succeeded");
      } else {
        promise.complete();
      }
    });
    return promise.future();
  }
}
