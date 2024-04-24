package org.folio.rest;

import static org.folio.rest.jaxrs.model.CalendarPeriodsServicePointIdCalculateopeningGetUnit.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.ArrayMatching.arrayContaining;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.okapi.testing.UtilityClassTester;
import org.folio.rest.jaxrs.model.Errors;
import org.folio.rest.jaxrs.resource.support.ResponseDelegate;
import org.folio.rest.tools.client.exceptions.ResponseException;
import org.folio.rest.tools.utils.BinaryOutStream;
import org.folio.rest.tools.utils.OutStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Null;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@ExtendWith(VertxExtension.class)
public class RestRoutingTest {
  @Test
  public void utilityClass() {
    UtilityClassTester.assertUtilityClass(RestRouting.class);
  }

  private static class Foo {
    @NotNull
    @JsonProperty("id")
    private String id;
    @Valid
    @JsonProperty("bar")
    private Bar bar;
    public Foo() {
    }
    public Foo(String id, Bar bar) {
      this.id = id;
      this.bar = bar;
    }
  };

  private static class Bar {
    @Valid
    @JsonProperty("baz")
    private Baz baz;
    public Bar() {
    }
    public Bar(String readme) {
      baz = new Baz(readme);
    }
  }

  private static class Baz {
    @Null  // used for read-only
    @JsonProperty("readme")
    private String readme;
    public Baz() {
    }
    public Baz(String readme) {
      this.readme = readme;
    }
  }

  @Test
  void isValidRequestFail() {
    Errors errors = new Errors();
    RestRouting.isValidRequest(null, new Foo(null, null), errors, List.of(), null);
    assertThat(errors.getErrors().get(0).getCode(), is("jakarta.validation.constraints.NotNull.message"));
  }

  @Test
  void isValidRequestSuccess() {
    Errors errors = new Errors();
    RestRouting.isValidRequest(null, new Foo("id", null), errors, List.of(), null);
    assertThat(errors.getErrors(), is(empty()));
  }

  private <T> T isValidRequest(T t, Class<T> clazz) {
    Errors errors = new Errors();
    Object [] o = RestRouting.isValidRequest(null, t, errors, List.of(), clazz);
    assertThat(errors.getErrors(), is(empty()));
    return (T) o[1];
  }

  @Test
  void isValidRequestRemoveNullSubfield() {
    assertThat(isValidRequest(new Baz("x"), Baz.class).readme, is(nullValue()));
    assertThat(isValidRequest(new Bar("y"), Bar.class).baz.readme, is(nullValue()));
    assertThat(isValidRequest(new Foo("id", new Bar("z")), Foo.class).bar.baz.readme, is(nullValue()));
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

  @Test
  void getOkapiHeaders() {
    MultiMap v = MultiMap.caseInsensitiveMultiMap();
    Map<String, String> okapiHeaders = RestRouting.getOkapiHeaders(v);
    assertThat(okapiHeaders.keySet(), empty());

    v.set("x-Okapi-ToKen", "mytoken");
    v.set("X-okapi", "1");
    v.set("other", "2");
    v.set("x-Okapi-tenant", "mytenant");

    okapiHeaders = RestRouting.getOkapiHeaders(v);
    assertThat(okapiHeaders.keySet(), containsInAnyOrder("x-okapi-token", "x-okapi", "x-okapi-tenant"));
    assertThat(okapiHeaders.get(XOkapiHeaders.TENANT), is("mytenant"));
    assertThat(okapiHeaders.get(XOkapiHeaders.TOKEN), is("mytoken"));
    assertThat(okapiHeaders.get("x-okapi"), is("1"));
  }

  Future<HttpResponse<Buffer>> sendResponse(Vertx vertx, ResponseBuilder responseBuilder) {
    Router router = Router.router(vertx);
    router.route().handler(rc -> {
      AsyncResult<Response> asyncResult = Future.succeededFuture(responseBuilder.build());
      RestRouting.sendResponse(rc, asyncResult, 0, "diku");
    });
    return vertx.createHttpServer()
        .requestHandler(router)
        .listen(0)
        .compose(httpServer -> WebClient.create(vertx)
            .getAbs("http://localhost:" + httpServer.actualPort())
            .send());
  }

  @Test
  void sendResponse200(Vertx vertx, VertxTestContext vtc) {
    sendResponse(vertx, Response.status(200).entity("foo"))
    .onComplete(vtc.succeeding(httpResponse -> {
      assertThat(httpResponse.statusCode(), is(200));
      assertThat(httpResponse.bodyAsString(), is("foo"));
      vtc.completeNow();
    }));
  }

  @Test
  void sendResponse204(Vertx vertx, VertxTestContext vtc) {
    sendResponse(vertx, Response.status(204))
    .onComplete(vtc.succeeding(httpResponse -> {
      assertThat(httpResponse.statusCode(), is(204));
      assertThat(httpResponse.bodyAsString(), is(nullValue()));
      vtc.completeNow();
    }));
  }

  @Test
  void sendResponseOutStream(Vertx vertx, VertxTestContext vtc) {
    OutStream outStream = new OutStream();
    outStream.setData("abc");
    sendResponse(vertx, Response.status(201).entity(outStream))
    .onComplete(vtc.succeeding(httpResponse -> {
      assertThat(httpResponse.statusCode(), is(201));
      assertThat(httpResponse.bodyAsString(), is("\"abc\""));
      vtc.completeNow();
    }));
  }

  @Test
  void sendResponseBinaryOutStream(Vertx vertx, VertxTestContext vtc) {
    BinaryOutStream binaryOutStream = new BinaryOutStream();
    binaryOutStream.setData("xyz".getBytes(StandardCharsets.UTF_8));
    sendResponse(vertx, Response.status(500).entity(binaryOutStream))
    .onComplete(vtc.succeeding(httpResponse -> {
      assertThat(httpResponse.statusCode(), is(500));
      assertThat(httpResponse.bodyAsString(), is("xyz"));
      vtc.completeNow();
    }));
  }

  @Test
  void sendResponseInteger(Vertx vertx, VertxTestContext vtc) {
    sendResponse(vertx, Response.status(500).entity(42))
    .onComplete(vtc.succeeding(httpResponse -> {
      assertThat(httpResponse.statusCode(), is(500));
      assertThat(httpResponse.bodyAsString(), is("42"));
      vtc.completeNow();
    }));
  }
}
