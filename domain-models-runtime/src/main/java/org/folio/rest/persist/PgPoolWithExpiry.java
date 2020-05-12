package org.folio.rest.persist;

import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;

public class PgPoolWithExpiry {
  static Logger log = LoggerFactory.getLogger(PgPoolWithExpiry.class);

  private PgPool pgPool;
  private boolean activity;
  private Long activityTimerId;
  private final PoolOptions poolOptions;
  private final PgConnectOptions pgConnectOptions;
  private final Vertx vertx;

  PgPoolWithExpiry(Vertx vertx, PgConnectOptions pgConnectOptions, PoolOptions poolOptions, long expiry) {
    this.pgConnectOptions = pgConnectOptions;
    this.poolOptions = poolOptions;
    this.vertx = vertx;
    pgPool = null;
    activity = false;
    activityTimerId = vertx.setPeriodic(expiry, res -> {
      if (!activity && pgPool != null) {
        pgPool.close();
        pgPool = null;
      }
      activity = false;
    });
  }

  private void stopExpiry() {
    if (activityTimerId != null) {
      // cancel expiry when we set a pool from the outside
      vertx.cancelTimer(activityTimerId);
      activityTimerId = null;
    }
  }

  void set(PgPool pool) {
    stopExpiry();
    activity = true;
    pgPool = pool;
  }

  PgPool get() {
    activity = true;
    if (pgPool == null) {
      pgPool = PgPool.pool(vertx, pgConnectOptions, poolOptions);
    }
    return pgPool;
  }

  void close() {
    stopExpiry();
    if (pgPool != null) {
      pgPool.close();
    }
    pgPool = null;
  }
}
