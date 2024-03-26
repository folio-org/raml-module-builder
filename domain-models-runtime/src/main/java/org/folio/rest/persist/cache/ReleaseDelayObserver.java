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
    if (this.timerId != null) {
      vertx.cancelTimer(this.timerId);
    }

    vertx.setTimer(toMilliseconds(releaseDelaySeconds), id -> {
      this.timerId = id;
      whenDone.run();
    });
  }
  public void cancelCountdown() {
    if (this.timerId != null) {
      vertx.cancelTimer(this.timerId);
    }
  }

  private Long toMilliseconds(int seconds) {
    return seconds * 1000L;
  }
}
