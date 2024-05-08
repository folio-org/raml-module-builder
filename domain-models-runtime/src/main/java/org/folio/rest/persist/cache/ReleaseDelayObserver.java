package org.folio.rest.persist.cache;

import io.vertx.core.Vertx;
import io.vertx.sqlclient.PoolOptions;

/**
 * Observes each cached connection and starts a countdown for the release delay seconds. which, if the timer
 * completes, will close the connection and remove it from the cache. There is one observer for each cached connection,
 * so each connection is able to manage its own lifecycle based on the release delay. See
 * {@link org.folio.rest.tools.utils.Envs#DB_CONNECTIONRELEASEDELAY}.
 * The release delay is the number of seconds after which an inactive connection can be closed.
 * The period of an inactive connection is defined as the amount of time the connection has been available.
 * A release delay of 0 means there is no timeout for the connection release. Note release delay is the RMB name for
 * the postgres idle timeout. See {@link PoolOptions#setIdleTimeout} for how this is set in vertx.
 * In {@link org.folio.rest.persist.PostgresClient} we set this value to 0 to allow the {@link CachedConnectionManager}
 * and this class to fully manage it. See the {@link CachedPgConnection} for how this observer is bound to the
 * connection. See {@link CachedPgConnection#setUnavailable()} and {@link CachedPgConnection#setAvailable()} for how
 * connection availability is managed. When a connection becomes available the timer starts.
 * @see ConnectionCache
 * @see CachedConnectionManager
 * @see CachedPgConnection
 */
public class ReleaseDelayObserver {
  private static final long MS_CONVERTER = 1000L;
  private final int releaseDelaySeconds;
  private final Vertx vertx;
  private Long timerId;

  public ReleaseDelayObserver(Vertx vertx, int releaseDelaySeconds) {
    this.vertx = vertx;
    this.releaseDelaySeconds = releaseDelaySeconds;
  }

  public int getReleaseDelaySeconds() {
    return releaseDelaySeconds;
  }

  public void startCountdown(Runnable whenDone) {
    if (releaseDelaySeconds == 0) {
      return;
    }

    if (timerId != null) {
      vertx.cancelTimer(this.timerId);
    }

    timerId = vertx.setTimer(toMilliseconds(releaseDelaySeconds), id -> {
      timerId = null;
      whenDone.run();
    });
  }

  public void cancelCountdown() {
    if (timerId != null) {
      vertx.cancelTimer(timerId);
      timerId = null;
    }
  }

  private static long toMilliseconds(int seconds) {
    return seconds * MS_CONVERTER;
  }
}
