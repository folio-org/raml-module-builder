package org.folio.rest.persist.cache;

import io.vertx.core.Vertx;

public class ReleaseDelayObserver {
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
    vertx.cancelTimer(this.timerId);

    vertx.setTimer(releaseDelaySeconds * 1000L, id -> {
      this.timerId = id;
      whenDone.run();
    });
  }

  public void cancelCountdown() {
    vertx.cancelTimer(this.timerId);
  }
}
