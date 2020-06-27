package org.folio;

import static io.restassured.RestAssured.given;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.io.IOUtils;
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
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetSocket;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.rest.persist.PostgresClient;

/**
 * This is our JUnit test for our verticle. The test uses vertx-unit, so we declare a custom runner.
 */
@RunWith(VertxUnitRunner.class)
public class DemoRamlRestTest {

  static {
    System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME, "io.vertx.core.logging.Log4j2LogDelegateFactory");
  }

  private static final Logger log = LoggerFactory.getLogger(DemoRamlRestTest.class);

  private static Vertx vertx;
  private static int port;
  private static Locale oldLocale = Locale.getDefault();
  private static String TENANT = "folio_shared";
  private static RequestSpecification tenant;

  /**
   * @param context  the test context.
   */
  @BeforeClass
  public static void setUp(TestContext context) throws IOException {
    // some tests (withoutParameter, withoutYearParameter) fail under other locales like Locale.GERMANY
    Locale.setDefault(Locale.US);

    vertx = VertxUtils.getVertxWithExceptionHandler();
    port = NetworkUtils.nextFreePort();
    RestAssured.port = port;
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    tenant = new RequestSpecBuilder().addHeader("x-okapi-tenant", TENANT).build();

    try {
      deployRestVerticle(context);
    } catch (Exception e) {
      context.fail(e);
    }
  }

  private static void deployRestVerticle(TestContext context) {
    DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(
        new JsonObject().put("http.port", port));
    vertx.deployVerticle(RestVerticle.class.getName(), deploymentOptions,
            context.asyncAssertSuccess());
  }

  /**
   * Cleanup: Delete temporary file, restore Locale, close the vert.x instance.
   *
   * @param context  the test context
   */
  @AfterClass
  public static void tearDown(TestContext context) {
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
  public void history(TestContext context) {
    checkURLs(context, "http://localhost:" + port + "/admin/memory?history=true", 200, "text/html");
  }

  private void postBook(TestContext context, String parameterString, int expectedStatus) {
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
        expectedStatus, HttpMethod.POST, "application/json", TENANT, false);
  }

  @Test
  public void getBookWithRoutingContext(TestContext context)  throws Exception {
    Buffer buf = Buffer.buffer("{\"module_to\":\"raml-module-builder-1.0.0\"}");
    NetClient cli = vertx.createNetClient();
    postData(context, "http://localhost:" + port + "/_/tenant", buf,
      201, HttpMethod.POST, "application/json", TENANT, false);

    buf = checkURLs(context, "http://localhost:" + port + "/rmbtests/test?query=nullpointer%3Dtrue", 500);
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
      HttpMethod.POST, "application/json", TENANT, false);

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

    List<Object> list = getListOfBooks();

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

  private void testStream(TestContext context, boolean chunk) {
    int chunkSize = 1024;
    int numberChunks = 50;
    Async async = context.async();
    HttpClient httpClient = vertx.createHttpClient();
    HttpClientRequest req = httpClient.post(port, "localhost", "/rmbtests/testStream", res -> {
      Buffer resBuf = Buffer.buffer();
      res.handler(resBuf::appendBuffer);
      res.endHandler(x -> {
        context.assertEquals(200, res.statusCode());
        JsonObject jo = new JsonObject(resBuf);
        context.assertTrue(jo.getBoolean("complete"));
        async.complete();
      });
      res.exceptionHandler(x -> {
        if (!async.isCompleted()) {
          context.assertTrue(false, "exceptionHandler res: " + x.getLocalizedMessage());
          async.complete();
        }
      });
    });
    req.exceptionHandler(x -> {
      if (!async.isCompleted()) {
        context.assertTrue(false, "exceptionHandler req: " + x.getLocalizedMessage());
        async.complete();
      }
    });
    if (chunk) {
      req.setChunked(true);
    } else {
      req.putHeader("Content-Length", Integer.toString(chunkSize * numberChunks));
    }
    req.putHeader("Accept", "application/json,text/plain");
    req.putHeader("Content-type", "application/octet-stream");
    req.putHeader("x-okapi-tenant", TENANT);
    Buffer buf = Buffer.buffer(chunkSize);
    for (int i = 0; i < chunkSize; i++) {
      buf.appendString("X");
    }
    for (int i = 0; i < numberChunks; i++) {
      req.write(buf);
    }
    req.end();
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
  public void options(TestContext context) {
    given().spec(tenant).when().options("/rmbtests/test").then().statusCode(200);
  }

  /**
   * @param context
   *
   */
  @Test
  public void checkClientCode(TestContext context)  {

    try {
      Async async = context.async(3);

      AdminClient aClient = new AdminClient("http://localhost:" + port, "abc", "abc", false);
      aClient.putAdminLoglevel(AdminLoglevelPutLevel.FINE, "org", reply -> {
        reply.bodyHandler( body -> {
          //System.out.println(body.toString("UTF8"));
          async.countDown();
        });
      });

      aClient.getAdminJstack( trace -> {
        trace.bodyHandler( content -> {
          //System.out.println(content);
          async.countDown();
        });
      });

      aClient.getAdminMemory(false , resp -> {
        resp.bodyHandler( content -> {
          //System.out.println(content);
          async.countDown();
        });
      });

    }
    catch (Exception e) {
      log.error(e.getMessage(), e);
      context.fail();
    }
  }

  /**
   * @param context
   */
  private void jobsTest(TestContext context) {

    String url = "/jobs/jobconfs";
    Async async = context.async();
    HttpClient client = vertx.createHttpClient();
    HttpClientRequest request = null;
    request = client.postAbs("http://localhost:" + port + url);
    request.exceptionHandler(error -> {
      async.complete();
      context.fail(error.getMessage());
    }).handler(response -> {
      int statusCode = response.statusCode();
      String location = response.getHeader("Location");
      // is it 2XX
      log.info(statusCode + " status at " + System.currentTimeMillis() + " for " +
        "http://localhost:" + port + url);

      if (statusCode == 201) {
        checkURLs(context, "http://localhost:" + port + url, 200);
        try {
          postData(context, "http://localhost:" + port + url + "/" +location+ "/jobs"
          , Buffer.buffer(getFile("job.json")), 201, HttpMethod.POST, "application/json", TENANT, false);
          postData(context, "http://localhost:" + port + url + "/" +location
          , Buffer.buffer(getFile("job_conf_post.json")), 204, HttpMethod.PUT, null, TENANT, false);
          postData(context, "http://localhost:" + port + url + "/12345"
          , Buffer.buffer(getFile("job_conf_post.json")), 404, HttpMethod.DELETE, null, TENANT, false);
          postData(context, "http://localhost:" + port + url + "/" +location+ "/jobs/12345"
          , Buffer.buffer(getFile("job_conf_post.json")), 404, HttpMethod.DELETE, null, TENANT, false);
        } catch (Exception e) {
          log.error(e.getMessage(), e);
          context.fail();
        }

      } else {
        context.fail("got incorrect response code");
      }
      if(!async.isCompleted()){
        async.complete();
      }
    });
    request.setChunked(true);
    request.putHeader("Accept", "application/json,text/plain");
    request.putHeader("Content-type", "application/json");
    try {
      request.write(getFile("job_conf_post.json"));
    } catch (IOException e) {
      log.error(e.getMessage(), e);
      context.fail("unable to read file");
    }
    request.end();
  }

  public Buffer checkURLs(TestContext context, String url, int codeExpected) {
    String accept = "application/json";
    return checkURLs(context, url, codeExpected, accept);
  }

  public Buffer checkURLs(TestContext context, String url, int codeExpected, String accept) {
    Buffer res = Buffer.buffer();
    try {
      Async async = context.async();
      HttpClient client = vertx.createHttpClient();
      HttpClientRequest request = client.getAbs(url, httpClientResponse -> {
        httpClientResponse.handler(res::appendBuffer);
        httpClientResponse.endHandler(x -> {
          log.info(httpClientResponse.statusCode() + ", " + codeExpected + " status expected: " + url);
          context.assertEquals(codeExpected, httpClientResponse.statusCode(), url);
          log.info(res.toString());
          async.complete();
        });
      });
      request.exceptionHandler(error -> {
        context.fail(url + " - " + error.getMessage());
        async.complete();
      });
      request.headers().add("x-okapi-tenant", TENANT);
      request.headers().add("Accept", accept);
      request.setChunked(true);
      request.end();
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
  private void postData(TestContext context, String url, Buffer buffer, int errorCode, HttpMethod method, String contenttype, String tenant, boolean userIdHeader) {
    Exception stacktrace = new RuntimeException();  // save stacktrace for async handler
    Async async = context.async();
    HttpClient client = vertx.createHttpClient();
    HttpClientRequest request = client.requestAbs(method, url);

    request.exceptionHandler(error -> {
      async.complete();
      System.out.println(" ---------------xxxxxx-------------------- " + error.getMessage());
      context.fail(new RuntimeException(error.getMessage(), stacktrace));
    }).handler(response -> {
      int statusCode = response.statusCode();
      // is it 2XX
      log.info(statusCode + ", " + errorCode + " expected status at "
        + System.currentTimeMillis() + " " + method.name() + " " + url);

      response.bodyHandler(responseData -> {
        if (statusCode == errorCode) {
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
          }
          context.assertTrue(true);
        } else {
          System.out.println(" ---------------xxxxxx-1------------------- " + responseData.toString());

          context.fail(new RuntimeException("got unexpected response code, expected: "
            + errorCode + ", received code: " + statusCode + " " + method.name() + " " + url
            + "\ndata:" + responseData.toString(), stacktrace));
        }
        if (!async.isCompleted()) {
          async.complete();
        }
      });
    });
    request.setChunked(true);
    request.putHeader("X-Okapi-Request-Id", "999999999999");
    if(tenant != null){
      request.putHeader("x-okapi-tenant", tenant);
    }
    request.putHeader("Accept", "application/json,text/plain");
    if(userIdHeader){
      request.putHeader("X-Okapi-User-Id", "af23adf0-61ba-4887-bf82-956c4aae2260");
    }
    request.putHeader("Content-type",  contenttype);
    if(buffer != null){
      request.write(buffer);
    }
    request.end();
    async.await();
  }

  private String getFile(String filename) throws IOException {
    return IOUtils.toString(getClass().getClassLoader().getResourceAsStream(filename), "UTF-8");
  }


  private List<Object> getListOfBooks(){
    List<Object> list = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      Book b = createBook();
      b.setStatus(i);
      list.add(b);
    }
    return list;
  }

  private Book createBook(){
    int ran = ThreadLocalRandom.current().nextInt(0, 11);
    Book b = new Book();
    b.setStatus(99+ran);
    b.setSuccess(true);
    b.setData(null);
    Data d = new Data();
    d.setAuthor("a" + ran);
    Datetime dt = new Datetime();

    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS\'Z\'");
    String parsedDate = format.format(new Date());
    dt.set$date(parsedDate);
    d.setDatetime(dt);
    d.setGenre("b");
    d.setDescription("c");
    d.setLink("d");
    d.setTitle("title"+ran);
    b.setData(d);
    return b;
  }
}
