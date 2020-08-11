package org.folio.rest;

import static org.folio.rest.jaxrs.model.CalendarPeriodsServicePointIdCalculateopeningGetUnit.*;
import static org.hamcrest.collection.ArrayMatching.arrayContaining;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.oneOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.folio.rest.jaxrs.model.Book;
import org.folio.rest.jaxrs.resource.support.ResponseDelegate;
import org.folio.rest.tools.client.exceptions.ResponseException;
import org.junit.jupiter.api.Test;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
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
  void javaVersion() {
    assertThat(RestVerticle.compareJavaVersion("1.2.3_4", "1.2.3_4"), is(0));
    assertThat(RestVerticle.compareJavaVersion("1.2.3_4", "1.2.3_5"), is(lessThan(0)));
    assertThat(RestVerticle.compareJavaVersion("1.2.3_4", "1.2.3_10"), is(lessThan(0)));
    assertThat(RestVerticle.compareJavaVersion("1.2.3_4", "1.2.10_3"), is(lessThan(0)));
    assertThat(RestVerticle.compareJavaVersion("1.2.3_4", "1.10.2_3"), is(lessThan(0)));
    assertThrows(InternalError.class, () -> RestVerticle.checkJavaVersion("1.8.0_99"));
    RestVerticle.checkJavaVersion("1.8.0_1000000");  // assert no exception
  }


  private static String jwtToken(JsonObject payload) {
    return "header." + Base64.getEncoder().encodeToString(payload.encode().getBytes()) + ".signature";
  }

  private static final String TOKEN = jwtToken(new JsonObject().put("user_id", "Willy"));
  private static final String EMPTY_TOKEN = jwtToken(new JsonObject());

  @Test
  void populateMetaData() {
    Map<String, String> okapiHeaders = new HashMap<>();
    okapiHeaders.put(RestVerticle.OKAPI_USERID_HEADER, "Maya");
    okapiHeaders.put(RestVerticle.OKAPI_HEADER_TOKEN, TOKEN);
    Book book = new Book();
    Date date1 = new Date();
    RestVerticle.populateMetaData(book, okapiHeaders, null);
    Date date2 = new Date();
    assertThat(book.getMetadata().getCreatedByUserId(), is("Maya"));
    assertThat(book.getMetadata().getUpdatedByUserId(), is("Maya"));
    assertThat(book.getMetadata().getCreatedDate(), is(oneOf(date1, date2)));
    assertThat(book.getMetadata().getUpdatedDate(), is(oneOf(date1, date2)));
  }

  @Test
  void populateMetadataNullEntity() {
    Map<String, String> okapiHeaders = new HashMap<>();
    okapiHeaders.put(RestVerticle.OKAPI_USERID_HEADER, "Maya");
    okapiHeaders.put(RestVerticle.OKAPI_HEADER_TOKEN, TOKEN);
    Book book = null;
    RestVerticle.populateMetaData(book, okapiHeaders, null);
    assertTrue(true);  // assert that no Exception was thrown
  }

  @Test
  void populateMetaDataFromToken() {
    Map<String, String> okapiHeaders = new HashMap<>();
    okapiHeaders.put(RestVerticle.OKAPI_HEADER_TOKEN, TOKEN);
    Book book = new Book();
    String date1 = new Date().toString();
    RestVerticle.populateMetaData(book, okapiHeaders, null);
    String date2 = new Date().toString();
    assertThat(book.getMetadata().getCreatedByUserId(), is("Willy"));
    assertThat(book.getMetadata().getUpdatedByUserId(), is("Willy"));
    assertThat(book.getMetadata().getCreatedDate().toString(), is(oneOf(date1, date2)));
    assertThat(book.getMetadata().getUpdatedDate().toString(), is(oneOf(date1, date2)));
  }

  @Test
  void populateMetaDataEmptyToken() {
    Map<String, String> okapiHeaders = new HashMap<>();
    okapiHeaders.put(RestVerticle.OKAPI_HEADER_TOKEN, EMPTY_TOKEN);
    Book book = new Book();
    String date1 = new Date().toString();
    RestVerticle.populateMetaData(book, okapiHeaders, null);
    String date2 = new Date().toString();
    assertThat(book.getMetadata().getCreatedByUserId(), is(nullValue()));
    assertThat(book.getMetadata().getUpdatedByUserId(), is(nullValue()));
    assertThat(book.getMetadata().getCreatedDate().toString(), is(oneOf(date1, date2)));
    assertThat(book.getMetadata().getUpdatedDate().toString(), is(oneOf(date1, date2)));
  }

  @Test
  void populateMetaMalformedToken() {
    Map<String, String> okapiHeaders = new HashMap<>();
    okapiHeaders.put(RestVerticle.OKAPI_HEADER_TOKEN, "malformed");
    Book book = new Book();
    String date1 = new Date().toString();
    RestVerticle.populateMetaData(book, okapiHeaders, null);
    String date2 = new Date().toString();
    assertThat(book.getMetadata().getCreatedByUserId(), is(nullValue()));
    assertThat(book.getMetadata().getUpdatedByUserId(), is(nullValue()));
    assertThat(book.getMetadata().getCreatedDate().toString(), is(oneOf(date1, date2)));
    assertThat(book.getMetadata().getUpdatedDate().toString(), is(oneOf(date1, date2)));
  }

  @Test
  void populateMetaForEntityWithoutMetadata() {
    Map<String, String> okapiHeaders = new HashMap<>();
    okapiHeaders.put(RestVerticle.OKAPI_HEADER_TOKEN, TOKEN);
    RestVerticle.populateMetaData(new Date(), okapiHeaders, null);
    assertTrue(true);  // assert no exception was thrown
  }

  @Test
  void populateMetadataWithException() {
    Book book = new Book();
    RestVerticle.populateMetaData(book, null, null);
    assertThat(book.getMetadata(), is(nullValue()));
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
