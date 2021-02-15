package org.folio.rest.client;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.io.UnsupportedEncodingException;
import java.time.ZoneId;
import java.util.Date;
import java.util.TimeZone;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
class RmbtestsClientTest {

  private static TimeZone oldTimeZone;

  @BeforeAll
  static void saveTimeZone() {
    oldTimeZone = TimeZone.getDefault();
  }

  @AfterAll
  static void restoreTimeZone() {
    TimeZone.setDefault(oldTimeZone);
  }

  @Test
  void timeZone(Vertx vertx, VertxTestContext vtc) {
    // the server default time zone must not change the UTC based date-time string
    TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("-0600")));
    vertx.createHttpServer().requestHandler(req -> vtc.verify(() -> {
      assertThat(req.params().get("publicationDate"), is("1970-01-02T00:00:00"));
      vtc.completeNow();
    })).listen(8888, server -> {
      Date date = new Date(24 * 60 * 60 * 1000);  // 1 day (in milliseconds) after 1970-01-01T00:00:00
      new RmbtestsClient("http://localhost:8888", "test_tenant", "token")
          .getRmbtestsBooks("author", date, 1, 1, "isbn", null, response -> {});
    });
  }
}
