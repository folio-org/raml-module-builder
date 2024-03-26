package org.folio.rest.persist.cache;

import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ReleaseDelayObserver {
  private static final Logger LOG = LogManager.getLogger(ReleaseDelayObserver.class);
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
    if (this.timerId != null) {
      vertx.cancelTimer(this.timerId);
    }

    vertx.setTimer(releaseDelaySeconds * 1000L, id -> {
      this.timerId = id;
      whenDone.run();
    });
  }

  public void cancelCountdown() {
    if (this.timerId != null) {
      vertx.cancelTimer(this.timerId);
    }
  }
}
