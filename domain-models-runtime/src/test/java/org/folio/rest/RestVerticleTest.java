package org.folio.rest;

import static org.folio.rest.jaxrs.model.CalendarPeriodsServicePointIdCalculateopeningGetUnit.*;
import static org.hamcrest.collection.ArrayMatching.arrayContaining;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.regex.Pattern;
import org.folio.rest.jaxrs.resource.support.ResponseDelegate;
import org.folio.rest.tools.client.exceptions.ResponseException;
import org.junit.jupiter.api.Test;
import io.vertx.core.Future;
import io.vertx.ext.web.RoutingContext;
import javax.ws.rs.core.Response;

class RestVerticleTest {

  Object parseEnum(String value, String defaultValue) throws Exception {
    return RestVerticle.parseEnum(
          "org.folio.rest.jaxrs.model.CalendarPeriodsServicePointIdCalculateopeningGetUnit",
          value, defaultValue);
  }

  @Test
  void parseEnum() throws Exception {
    assertThat(parseEnum(null,           null  ), is(nullValue()));
    assertThat(parseEnum("",             ""    ), is(nullValue()));
    assertThat(parseEnum("day",          "hour"), is(DAY));
    assertThat(parseEnum("hour",         "day" ), is(HOUR));
    assertThat(parseEnum("foo",          "day" ), is(DAY));
    assertThat(parseEnum(null,           "day" ), is(DAY));
    assertThat(parseEnum("foo",          "hour"), is(HOUR));
    assertThat(parseEnum(null,           "hour"), is(HOUR));
    assertThat(parseEnum("bee interval", ""    ), is(BEEINTERVAL));
  }

  @Test
  void parseEnumUnknownType() {
    assertThrows(ClassNotFoundException.class, () -> RestVerticle.parseEnum("foo.bar", "foo", "bar"));
  }

  @Test
  void parseEnumNonEnumClass() throws Exception {
    assertThat(RestVerticle.parseEnum("java.util.Vector", "foo", "bar"), is(nullValue()));
  }

  @Test
  void routeExceptionReturns500() {
    class MyRestVerticle extends RestVerticle {
      public int status = -1;
      @Override
      void endRequestWithError(RoutingContext rc, int status, boolean chunked, String message, boolean[] isValid) {
        this.status = status;
      }
    };
    MyRestVerticle myRestVerticle = new MyRestVerticle();
    myRestVerticle.route(null, null, null, null);
    assertThat(myRestVerticle.status, is(500));
  }

  @Test
  void matchUrl() {
    assertThat(RestVerticle.matchPath("/", Pattern.compile("^/x$")), is(nullValue()));
    assertThat(RestVerticle.matchPath("/x", Pattern.compile("^/$")), is(nullValue()));
    assertThat(RestVerticle.matchPath("/x", Pattern.compile("^/y$")), is(nullValue()));
    assertThat(RestVerticle.matchPath("/", Pattern.compile("^/$")), is(emptyArray()));
    assertThat(RestVerticle.matchPath("/x/yy", Pattern.compile("^/x/yy$")), is(emptyArray()));
    assertThat(RestVerticle.matchPath("/x/yy", Pattern.compile("^/x/([^/]+)$")),
        is(arrayContaining("yy")));
    assertThat(RestVerticle.matchPath("/abc/def/ghi", Pattern.compile("^/([^/]+)/([^/]+)/([^/]+)$")),
        is(arrayContaining("abc", "def", "ghi")));
    assertThat(RestVerticle.matchPath("/%2F%3F%2B%23/12%2334", Pattern.compile("^/([^/]+)/([^/]+)$")),
        is(arrayContaining("/?+#", "12#34")));
  }

  @Test
  void getResponseSucceeded() {
    Response response = ResponseDelegate.status(234).build();
    assertThat(RestVerticle.getResponse(Future.succeededFuture(response)).getStatus(), is(234));
  }

  @Test
  void getResponseFailed() {
    Response response = ResponseDelegate.status(456).build();
    assertThat(RestVerticle.getResponse(Future.failedFuture(new ResponseException(response))).getStatus(), is(456));
  }

  @Test
  void getResponseFailedWithoutResponse() {
    assertThat(RestVerticle.getResponse(Future.failedFuture("foo")), is(nullValue()));
  }
}
