package org.folio.rest;

import static org.folio.rest.jaxrs.model.CalendarPeriodsServicePointIdCalculateopeningGetUnit.*;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.collection.ArrayMatching.arrayContaining;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.folio.rest.jaxrs.resource.support.ResponseDelegate;
import org.folio.rest.testing.UtilityClassTester;
import org.folio.rest.tools.client.exceptions.ResponseException;
import org.junit.jupiter.api.Test;
import io.vertx.core.Future;
import javax.ws.rs.core.Response;
import java.util.regex.Pattern;

public class RestRoutingTest {
  @Test
  public void utilityClass() {
    UtilityClassTester.assertUtilityClass(RestRouting.class);
  }

  Object parseEnum(String value, String defaultValue) throws Exception {
    return RestRouting.parseEnum(
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
    assertThrows(ClassNotFoundException.class, () -> RestRouting.parseEnum("foo.bar", "foo", "bar"));
  }

  @Test
  void parseEnumNonEnumClass() throws Exception {
    assertThat(RestRouting.parseEnum("java.util.Vector", "foo", "bar"), is(nullValue()));
  }

  @Test
  void getResponseSucceeded() {
    Response response = ResponseDelegate.status(234).build();
    assertThat(RestRouting.getResponse(Future.succeededFuture(response)).getStatus(), is(234));
  }

  @Test
  void getResponseFailed() {
    Response response = ResponseDelegate.status(456).build();
    assertThat(RestRouting.getResponse(Future.failedFuture(new ResponseException(response))).getStatus(), is(456));
  }

  @Test
  void getResponseFailedWithoutResponse() {
    assertThat(RestRouting.getResponse(Future.failedFuture("foo")), is(nullValue()));
  }

  @Test
  void matchUrl() {
    assertThat(RestRouting.matchPath("/", Pattern.compile("^/x$")), is(emptyArray()));
    assertThat(RestRouting.matchPath("/x", Pattern.compile("^/$")), is(emptyArray()));
    assertThat(RestRouting.matchPath("/x", Pattern.compile("^/y$")), is(emptyArray()));
    assertThat(RestRouting.matchPath("/", Pattern.compile("^/$")), is(emptyArray()));
    assertThat(RestRouting.matchPath("/x/yy", Pattern.compile("^/x/yy$")), is(emptyArray()));
    assertThat(RestRouting.matchPath("/x/yy", Pattern.compile("^/x/([^/]+)$")),
        is(arrayContaining("yy")));
    assertThat(RestRouting.matchPath("/abc/def/ghi", Pattern.compile("^/([^/]+)/([^/]+)/([^/]+)$")),
        is(arrayContaining("abc", "def", "ghi")));
    assertThat(RestRouting.matchPath("/%2F%3F%2B%23/12%2334", Pattern.compile("^/([^/]+)/([^/]+)$")),
        is(arrayContaining("/?+#", "12#34")));
  }
}
