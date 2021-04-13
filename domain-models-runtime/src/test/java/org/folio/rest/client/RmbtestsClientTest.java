package org.folio.rest.client;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
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
  void webClient(Vertx vertx, VertxTestContext vtc) {
    WebClient webClient = WebClient.create(vertx);
    vertx.createHttpServer()
    .requestHandler(req -> req.response().end("Pong"))
    .listen(8888)
    .compose(x -> new RmbtestsClient("http://localhost:8888", "tenant", "token", webClient).getRmbtestsTest(null))
    .onComplete(vtc.succeeding(result -> assertThat(result.bodyAsString(), is("Pong"))))
    .compose(x -> new RmbtestsClient("http://localhost:8888", "tenant", "token", webClient).getRmbtestsTest(null))
    .onComplete(vtc.succeeding(result -> assertThat(result.bodyAsString(), is("Pong"))))
    .onComplete(x -> vtc.completeNow());
  }

  @Test
  void httpClient(Vertx vertx, VertxTestContext vtc) {
    HttpClient httpClient = vertx.createHttpClient();
    vertx.createHttpServer()
    .requestHandler(req -> req.response().end("Pong"))
    .listen(8888)
    .compose(x -> new RmbtestsClient("http://localhost:8888", "tenant", "token", httpClient).getRmbtestsTest(null))
    .onComplete(vtc.succeeding(result -> assertThat(result.bodyAsString(), is("Pong"))))
    .compose(x -> new RmbtestsClient("http://localhost:8888", "tenant", "token", httpClient).getRmbtestsTest(null))
    .onComplete(vtc.succeeding(result -> assertThat(result.bodyAsString(), is("Pong"))))
    .onComplete(x -> vtc.completeNow());
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
          .getRmbtestsBooks("author", date,  0,1, 1, "isbn", false, null, response -> {});
    });
  }

}
