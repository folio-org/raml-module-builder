package org.folio;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import io.vertx.core.AsyncResult;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import java.io.IOException;
import java.util.Date;
import java.util.Locale;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.AdminClient;
import org.folio.rest.jaxrs.model.AdminLoglevelPutLevel;
import org.folio.rest.jaxrs.model.Book;
import org.folio.rest.jaxrs.model.Books;
import org.folio.rest.jaxrs.model.Data;
import org.folio.rest.jaxrs.model.Datetime;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.tools.parser.JsonPathParser;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.rest.tools.utils.VertxUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetSocket;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.rest.persist.PostgresClient;

/**
 * This is our JUnit test for our verticle. The test uses vertx-unit, so we declare a custom runner.
 */
@RunWith(VertxUnitRunner.class)
public class DemoRamlRestTest {

  private static final Logger log = LogManager.getLogger(DemoRamlRestTest.class);

  private static Vertx vertx;
  private static int port;
  private static Locale oldLocale = Locale.getDefault();
  private static String TENANT = "folio_shared";
  private static RequestSpecification tenant;

  @Rule
  public Timeout timeout = Timeout.seconds(5);

  /**
   * @param context  the test context.
   */
  @BeforeClass
  public static void setUp(TestContext context) throws IOException {
    // some tests (withoutParameter, withoutYearParameter) fail under other locales like Locale.GERMANY
    Locale.setDefault(Locale.US);

    // do not use PostgresClient.setPostgresTester here so we check that PostgresTesterEmbedded is working

    vertx = VertxUtils.getVertxWithExceptionHandler();
    port = NetworkUtils.nextFreePort();
    RestAssured.port = port;
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    tenant = new RequestSpecBuilder().addHeader("x-okapi-tenant", TENANT).build();

    try {
      dropSchemaRole(context);
      deployRestVerticle(context);
      Buffer buf = Buffer.buffer("{\"module_to\":\"raml-module-builder-1.0.0\"}");
      String location = postData(context, "http://localhost:" + port + "/_/tenant", buf,
        201, HttpMethod.POST, "application/json", TENANT, false);
      checkURLs(context, "http://localhost:" + port + location + "?wait=10000", 200);

    } catch (Exception e) {
      context.fail(e);
    }
  }

  private static void dropSchemaRole(TestContext context) {
    // Dropping is needed when developers reuse the database to save startup time.
    Async async = context.async();
    PostgresClient postgresClient = PostgresClient.getInstance(Vertx.vertx());
    postgresClient.execute("drop schema " + TENANT + "_raml_module_builder cascade", ignore1 -> {
      postgresClient.execute("drop role " + TENANT + "_raml_module_builder", ignore2 -> {
        PostgresClient.closeAllClients();
        async.complete();
      });
    });
    async.await(5000);
  }

  private static void deployRestVerticle(TestContext context) {
    Async async = context.async();
    DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(
        new JsonObject().put("http.port", port));
    vertx.deployVerticle(RestVerticle.class.getName(), deploymentOptions,
        context.asyncAssertSuccess(done -> async.complete()));
    async.await(5000);
  }

  /**
   * Cleanup: Delete temporary file, restore Locale, close the vert.x instance.
   *
   * @param context  the test context
   */
  @AfterClass
  public static void tearDown(TestContext context) {
    try {
      Buffer buf = Buffer.buffer("{\"purge\": true}");
      postData(context, "http://localhost:" + port + "/_/tenant", buf,
          204, HttpMethod.POST, "application/json", TENANT, false);
    } catch (Exception e) {
      context.fail(e);
    }
    Locale.setDefault(oldLocale);
    PostgresClient.stopEmbeddedPostgres();
    vertx.close(context.asyncAssertSuccess());
  }

  @Test
  public void date(TestContext context) {
    checkURLs(context, "http://localhost:" + port + "/rmbtests/books?publicationDate=&author=me", 400);
  }

  @Test
  public void datex(TestContext context) {
    checkURLs(context, "http://localhost:" + port + "/rmbtests/books?publicationDate=x&author=me", 400);
  }

  @Test
  public void date1(TestContext context) {
    checkURLs(context, "http://localhost:" + port + "/rmbtests/books?publicationDate=1&author=me", 400);
  }

  @Test
  public void withoutDateParameter(TestContext context) {
    checkURLs(context, "http://localhost:" + port + "/rmbtests/books?author=me", 400);
  }

  @Test
  public void withoutParameter(TestContext context) {
    checkURLs(context, "http://localhost:" + port + "/rmbtests/books", 400);
  }

  @Test
  public void wrongPath(TestContext context) {
    checkURLs(context, "http://localhost:" + port + "/rmbtests/x/books", 400); // should be 404
  }

  @Test
  public void getOk(TestContext context) {
    checkURLs(context, "http://localhost:" + port + "/rmbtests/books?publicationDate=1900-01-01&author=me&rating=1.2", 200);
  }

  @Test
  public void getOkWithDatetime(TestContext context) {
    checkURLs(context, "http://localhost:" + port + "/rmbtests/books?publicationDate=2011-12-03T10:15:30&author=you&rating=1.2", 200);
  }

  @Test
  public void history(TestContext context) {
    checkURLs(context, "http://localhost:" + port + "/admin/memory?history=true", 200, "text/html");
  }

  @Test
  public void acceptDefault(TestContext context) {
    checkURLs(context, "http://localhost:" + port + "/rmbtests/test", 200, null);
  }

  @Test
  public void acceptNoMatch(TestContext context) {
    checkURLs(context, "http://localhost:" + port + "/rmbtests/test", 400, "text/html");
  }

  @Test
  public void contentTypeNoMatch(TestContext context) {
    postBook(context, "?validate_field=data.description", 400,  "text/html");
  }

  private void postBook(TestContext context, String parameterString, int expectedStatus) {
    postBook(context, parameterString, expectedStatus, "application/json");
  }

  private void postBook(TestContext context, String parameterString, int expectedStatus, String contentType) {
    Book b = new Book();
    Data d = new Data();
    d.setAuthor("a");
    d.setGenre("g");
    d.setDescription("asdfss");
    b.setData(d);
    ObjectMapper om = new ObjectMapper();
    String book = "";
    try {
      book = om.writerWithDefaultPrettyPrinter().writeValueAsString(b);
    }
    catch (JsonProcessingException e) {
      context.fail(e);
    }
    postData(context, "http://localhost:" + port + "/rmbtests/books"+parameterString, Buffer.buffer(book),
        expectedStatus, HttpMethod.POST, contentType, TENANT, false);
  }

  @Test
  public void getBookWithRoutingContext(TestContext context)  throws Exception {
    Buffer buf = checkURLs(context, "http://localhost:" + port + "/rmbtests/test?query=nullpointer%3Dtrue", 500);
    context.assertEquals("java.lang.NullPointerException", buf.toString());

    Books books;
    buf = checkURLs(context, "http://localhost:" + port + "/rmbtests/test", 200);
    books = Json.decodeValue(buf, Books.class);
    context.assertEquals(0, books.getTotalRecords());
    context.assertEquals(0, books.getResultInfo().getDiagnostics().size());
    context.assertEquals(0, books.getResultInfo().getTotalRecords());
    context.assertEquals(0, books.getBooks().size());

    buf = checkURLs(context, "http://localhost:" + port + "/rmbtests/test?query=title%3Dwater", 200);
    books = Json.decodeValue(buf, Books.class);
    context.assertEquals(0, books.getTotalRecords());

    buf = checkURLs(context, "http://localhost:" + port + "/rmbtests/test?query=a%3D", 400);
    context.assertTrue(buf.toString().contains("expected index or term, got EOF"));

    Data d = new Data();
    d.setAuthor("a");
    d.setGenre("g");
    d.setDescription("description1");
    d.setTitle("title");
    d.setDatetime(new Datetime());
    d.setLink("link");

    Book b = new Book();
    b.setData(d);
    b.setStatus(0);
    b.setSuccess(true);

    ObjectMapper om = new ObjectMapper();
    String book = om.writerWithDefaultPrettyPrinter().writeValueAsString(b);

    postData(context, "http://localhost:" + port + "/rmbtests/test", Buffer.buffer(book), 201,
      HttpMethod.POST, null, TENANT, false);

    buf = checkURLs(context, "http://localhost:" + port + "/rmbtests/test", 200);
    books = Json.decodeValue(buf, Books.class);
    context.assertEquals(1, books.getTotalRecords());
    context.assertEquals(0, books.getResultInfo().getDiagnostics().size());
    context.assertEquals(1, books.getResultInfo().getTotalRecords());
    context.assertEquals(1, books.getBooks().size());

    d.setDescription("description2");
    book = om.writerWithDefaultPrettyPrinter().writeValueAsString(b);
    postData(context, "http://localhost:" + port + "/rmbtests/test", Buffer.buffer(book), 201,
      HttpMethod.POST, "application/json", TENANT, false);

    buf = checkURLs(context, "http://localhost:" + port + "/rmbtests/test", 200);
    books = Json.decodeValue(buf, Books.class);
    context.assertEquals(2, books.getTotalRecords());
    context.assertEquals(0, books.getResultInfo().getDiagnostics().size());
    context.assertEquals(2, books.getResultInfo().getTotalRecords());
    context.assertEquals(2, books.getBooks().size());

    // need at least one record in result before we can trigger this error
    buf = checkURLs(context, "http://localhost:" + port + "/rmbtests/test?query=badclass%3Dtrue", 200);
    books = Json.decodeValue(buf, Books.class);
    context.assertEquals(2, books.getTotalRecords());
    context.assertEquals(1, books.getResultInfo().getDiagnostics().size());
    context.assertTrue(books.getResultInfo().getDiagnostics().get(0).getMessage().contains("Cannot deserialize instance of"));
    context.assertEquals(2, books.getResultInfo().getTotalRecords());
    context.assertEquals(0, books.getBooks().size());

    // see that we can handle a subset of the Book properties: id and status
    // use case: replace "SELECT jsonb FROM ..." by "SELECT jsonb_build_object('id', id, 'status', jsonb->'status') FROM ..."
    buf = checkURLs(context, "http://localhost:" + port + "/rmbtests/test?query=slim%3Dtrue", 200);
    JsonObject jo = new JsonObject(buf);
    context.assertEquals(2, jo.getInteger("totalRecords"));
    SlimBook sb = jo.getJsonArray("books").getJsonObject(0).mapTo(SlimBook.class);
    context.assertEquals(0, sb.getStatus());
    context.assertNotNull(sb.getId());
    sb = jo.getJsonArray("books").getJsonObject(1).mapTo(SlimBook.class);
    context.assertEquals(0, sb.getStatus());
    context.assertNotNull(sb.getId());

    buf = checkURLs(context, "http://localhost:" + port + "/rmbtests/test?query=wrapper%3Dtrue", 200);
    books = Json.decodeValue(buf, Books.class);
    context.assertEquals(2, books.getTotalRecords());
  }

  @Test
  public void postBookNoParameters(TestContext context) {
    postBook(context, "", 422);
  }

  /**
   * 4 calls with invalid CQL cause RMB to hang if PostgreSQL connections
   * are not closed: https://issues.folio.org/browse/RMB-677
   */
  @Test
  public void invalidCqlClosesConnection(TestContext context) {
    for (int i=0; i<10; i++) {
      given().spec(tenant).when().get("/rmbtests/test?query=()").then().statusCode(400);
    }
  }


  @Test
  public void postBookValidateAuthor(TestContext context) {
    postBook(context, "?validate_field=author", 200);
  }

  @Test
  public void postBookValidateDescription(TestContext context) {
    postBook(context, "?validate_field=data.description", 200);
  }

  @Test
  public void postBookValidateTitle(TestContext context) {
    postBook(context, "?validate_field=data.title", 422);
  }

  @Test
  public void postBookValidateTitleAndDescription(TestContext context) {
    postBook(context, "?validate_field=data.title&validate_field=data.description", 422);
  }

  /**
   * just send a get request for books api with and without the required author query param
   * 1. one call should succeed and the other should fail (due to
   * validation aspect that should block the call and return 400)
   * 2. test the built in upload functionality
   * @param context - the test context
   */
  @Test
  public void test(TestContext context) throws Exception {
    Book b = new Book();
    Data d = new Data();
    d.setAuthor("a");
    d.setGenre("g");
    d.setDescription("asdfss");
    b.setData(d);
    d.setTitle("title");

    d.setDatetime(new Datetime());
    d.setLink("link");
    b.setStatus(0);
    b.setSuccess(true);
    ObjectMapper om = new ObjectMapper();
    String book = om.writerWithDefaultPrettyPrinter().writeValueAsString(b);

    postData(context, "http://localhost:" + port + "/rmbtests/books", Buffer.buffer(book), 201, HttpMethod.POST, "application/json", TENANT, true);
    postData(context, "http://localhost:" + port + "/rmbtests/books", Buffer.buffer(book), 201, HttpMethod.POST, "application/json", TENANT, false);

    //check that additionalProperties (fields not appearing in schema) - returns 422
    JsonObject jo = new JsonObject(book);
    jo.put("lalala", "non existant");
    postData(context, "http://localhost:" + port + "/rmbtests/books", Buffer.buffer(jo.encode()), 422, HttpMethod.POST, "application/json", TENANT, false);


    postData(context, "http://localhost:" + port + "/admin/loglevel?level=FINE&java_package=org.folio.rest", null, 200, HttpMethod.PUT, "application/json", TENANT, false);

    Metadata md = new Metadata();
    md.setCreatedByUserId("12345678-1234-1234-1234-123456789098");
    md.setCreatedByUsername("you");
    md.setCreatedDate(new Date());
    md.setUpdatedDate(new Date());
    md.setUpdatedByUserId("123456789098");
    b.setMetadata(md);
    postData(context, "http://localhost:" + port + "/rmbtests/books",
      Buffer.buffer(om.writerWithDefaultPrettyPrinter().writeValueAsString(b)), 422, HttpMethod.POST, "application/json", TENANT, false);

    md.setUpdatedByUserId("12345678-1234-1234-1234-123456789098");
    postData(context, "http://localhost:" + port + "/rmbtests/books",
      Buffer.buffer(om.writerWithDefaultPrettyPrinter().writeValueAsString(b)), 201, HttpMethod.POST, "application/json", TENANT, false);

    checkURLs(context, "http://localhost:" + port + "/apidocs/index.html", 200);
    checkURLs(context, "http://localhost:" + port + "/admin/loglevel", 200);
  }

  private void testStreamTcpClient(TestContext context, int size) {
    Async async = context.async();
    NetClient netClient = vertx.createNetClient();
    netClient.connect(port, "localhost", con -> {
      context.assertTrue(con.succeeded());
      if (con.failed()) {
        async.complete();
        return;
      }
      NetSocket socket = con.result();
      socket.write("POST /rmbtests/testStream HTTP/1.1\r\n");
      socket.write("Host: localhost:" + Integer.toString(port) + "\r\n");
      socket.write("Content-Type: application/octet-stream\r\n");
      socket.write("Accept: application/json,text/plain\r\n");
      socket.write("X-Okapi-Tenant: " + TENANT + "\r\n");
      socket.write("Content-Length: " + Integer.toString(size) + "\r\n");
      socket.write("\r\n");
      socket.write("123\r\n");  // body is 5 bytes
      Buffer buf = Buffer.buffer();
      socket.handler(buf::appendBuffer);
      vertx.setTimer(100, x -> {
        socket.end();
        if (!async.isCompleted()) {
          async.complete();
        }
      });
      socket.endHandler(x -> {
        if (!async.isCompleted()) {
          async.complete();
        }
      });
    });
  }

  @Test
  public void testStreamManual(TestContext context) {
    testStreamTcpClient(context, 5);
  }

  @Test
  public void testStreamAbort(TestContext context) {
    testStreamTcpClient(context, 10);
  }

  private void testStream(TestContext context, boolean chunked) {
    int chunkSize = 1024;
    int numberChunks = 50;
    Async async = context.async();
    vertx.createHttpClient()
    .request(HttpMethod.POST, port, "localhost", "/rmbtests/testStream")
    .onComplete(context.asyncAssertSuccess(request -> {
      request.response().onComplete(context.asyncAssertSuccess(response -> {
        assertThat(response.statusCode(), is(200));
        response.body(context.asyncAssertSuccess(body -> {
          assertThat(body.toJsonObject().getBoolean("complete"), is(true));
          async.complete();
        }));
      }));

      if (chunked) {
        request.setChunked(true);
      } else {
        request.putHeader("Content-Length", Integer.toString(chunkSize * numberChunks));
      }
      request.putHeader("Accept", "application/json,text/plain");
      request.putHeader("Content-type", "application/octet-stream");
      request.putHeader("x-okapi-tenant", TENANT);
      String chunk = "X".repeat(chunkSize);
      for (int i = 0; i < numberChunks; i++) {
        request.write(chunk);
      }
      request.end(context.asyncAssertSuccess());
    }));
  }

  @Test
  public void testStreamWithLength(TestContext context) {
    testStream(context, false);
  }

  @Test
  public void testStreamChunked(TestContext context) {
    testStream(context, true);
  }

  @Test
  public void options() {
    given().spec(tenant).when().options("/rmbtests/test").then().statusCode(200);
  }

  /**
   * @param context
   *
   */
  @Test
  public void checkClientCode(TestContext context) throws Exception {
    AdminClient aClient = new AdminClient("http://localhost:" + port, "abc", "abc", false);
    aClient.putAdminLoglevel(AdminLoglevelPutLevel.FINE, "org", context.asyncAssertSuccess());
    aClient.getAdminJstack(context.asyncAssertSuccess());
    aClient.getAdminMemory(false, context.asyncAssertSuccess());
  }

  public static Buffer checkURLs(TestContext context, String url, int codeExpected) {
    String accept = "application/json";
    return checkURLs(context, url, codeExpected, accept);
  }

  public static Buffer checkURLs(TestContext context, String url, int codeExpected, String accept) {
    Buffer res = Buffer.buffer();
    try {
      Async async = context.async();
      WebClient client = WebClient.create(vertx);
      final HttpRequest<Buffer> request = client.getAbs(url);
      request.headers().add("x-okapi-tenant", TENANT);
      if (accept != null) {
        request.headers().add("Accept", accept);
      }
      request.send(x -> {
        x.map(httpClientResponse->
        {
          res.appendBuffer(httpClientResponse.body());
          log.info(httpClientResponse.statusCode() + ", " + codeExpected + " status expected: " + url);
          log.info(res);
          context.assertEquals(codeExpected, httpClientResponse.statusCode(), url);
          async.complete();
          return null;
        }).otherwise(f-> {
          context.fail(url + " - " + f.getMessage());
          async.complete();
          return null;
         }
        );
        });
      async.await();
    } catch (Throwable e) {
      log.error(e.getMessage(), e);
      context.fail(e);
    }
    return res;
  }

  /**
   * for POST
   */
  private static String postData(TestContext context, String url, Buffer buffer,
      int errorCode, HttpMethod method, String contenttype, String tenant, boolean userIdHeader) {
    Exception stacktrace = new RuntimeException();  // save stacktrace for async handler
    Async async = context.async();
    WebClient client =  WebClient.create(vertx);
    HttpRequest<Buffer> request = client.requestAbs(method, url);
    request.putHeader("X-Okapi-Request-Id", "999999999999");
    if (tenant != null) {
      request.putHeader("x-okapi-tenant", tenant);
    }
    request.putHeader("Accept", "application/json,text/plain");
    if (userIdHeader) {
      request.putHeader("X-Okapi-User-Id", "af23adf0-61ba-4887-bf82-956c4aae2260");
    }
    if (contenttype != null) {
      request.putHeader("Content-type", contenttype);
    }
    StringBuilder location = new StringBuilder();
    if (buffer != null) {
      request.sendBuffer(buffer, e-> postDataHandler(e, async, context, errorCode,
          stacktrace, method, url, userIdHeader, location));
    } else {
      request.send(e -> postDataHandler(e, async, context, errorCode,
          stacktrace, method, url, userIdHeader, location));
    }
    async.await();
    client.close();
    return location.toString();
  }

  private static void postDataHandler(AsyncResult<HttpResponse<Buffer>> asyncResult,
                                      Async async, TestContext context, int expectedStatusCode,
                                      Exception stacktrace,
                                      HttpMethod method, String url, boolean userIdHeader,
                                      StringBuilder location) {
    asyncResult.map(response -> {
      int statusCode = response.statusCode();
      // is it 2XX
      log.info(statusCode + ", " + expectedStatusCode + " expected status at "
          + System.currentTimeMillis() + " " + method.name() + " " + url);

      Buffer responseData = response.body();
      if (statusCode == expectedStatusCode) {
        final String str = response.getHeader("Content-type");
        if (str == null && statusCode >= 400) {
          context.fail(new RuntimeException("No Content-Type", stacktrace));
        }
        if (statusCode == 422) {
          if (str.contains("application/json")) {
            context.assertTrue(true);
          } else {
            context.fail(new RuntimeException(
                "422 response code should contain a Content-Type header of application/json",
                stacktrace));
          }
        } else if (statusCode == 201) {
          if (userIdHeader) {
            String date = (String) new JsonPathParser(responseData.toJsonObject()).getValueAt("metadata.createdDate");
            if (date == null) {
              context.fail(new RuntimeException(
                  "metaData schema createdDate missing from returned json", stacktrace));
            }
          }
          String tmp = response.getHeader("Location");
          if (tmp != null) {
            location.append(tmp);
          }
        }
        context.assertTrue(true);
      } else {
        log.info(" ---------------xxxxxx-1------------------- {}", responseData.toString());

        context.fail(new RuntimeException("got unexpected response code, expected: "
            + expectedStatusCode + ", received code: " + statusCode + " " + method.name() + " " + url
            + "\ndata:" + responseData.toString(), stacktrace));
      }
      if (!async.isCompleted()) {
        async.complete();
      }
      return null;
    }).otherwise(error -> {
      log.error(" ---------------xxxxxx-------------------- " + error.getMessage(), error);
      context.fail(new RuntimeException(error.getMessage(), stacktrace));
      return null;
    });
  }
}
