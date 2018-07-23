package org.folio;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;

import javax.crypto.SecretKey;
import javax.mail.BodyPart;
import javax.mail.internet.InternetHeaders;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;

import org.apache.commons.io.IOUtils;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.AdminClient;
import org.folio.rest.client.RmbtestsClient;
import org.folio.rest.jaxrs.model.Book;
import org.folio.rest.jaxrs.model.Data;
import org.folio.rest.jaxrs.model.Datetime;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.resource.AdminResource.PersistMethod;
import org.folio.rest.security.AES;
import org.folio.rest.tools.messages.Messages;
import org.folio.rest.tools.parser.JsonPathParser;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.rest.tools.utils.VertxUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

/**
 * This is our JUnit test for our verticle. The test uses vertx-unit, so we declare a custom runner.
 */
@RunWith(VertxUnitRunner.class)
public class DemoRamlRestTest {
  private static final Logger log = LoggerFactory.getLogger(Messages.class);

  private static Vertx vertx;
  private static int port;
  private static Locale oldLocale = Locale.getDefault();
  private static String TENANT = "abcdefg";

  static {
    System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME, "io.vertx.core.logging.Log4jLogDelegateFactory");
  }

  /**
   * @param context  the test context.
   */
  @BeforeClass
  public static void setUp(TestContext context) throws IOException {
    // some tests (withoutParameter, withoutYearParameter) fail under other locales like Locale.GERMANY
    Locale.setDefault(Locale.US);

    vertx = VertxUtils.getVertxWithExceptionHandler();
    port = NetworkUtils.nextFreePort();

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
    deleteTempFilesCreated();
    Locale.setDefault(oldLocale);
    vertx.close(context.asyncAssertSuccess());
  }

  private static void deleteTempFilesCreated(){
    log.info("deleting created files");
    // Lists all files in folder
    File folder = new File(RestVerticle.DEFAULT_TEMP_DIR);
    File fList[] = folder.listFiles();
    // Searchs test.json
    for (int i = 0; i < fList.length; i++) {
        String pes = fList[i].getName();
        if (pes.endsWith("test.json")) {
            // and deletes
            boolean success = fList[i].delete();
        }
    }
  }

  @Test
  public void year(TestContext context) {
    checkURLs(context, "http://localhost:" + port + "/rmbtests/books?publicationYear=&author=me", 400);
  }

  @Test
  public void yearx(TestContext context) {
    checkURLs(context, "http://localhost:" + port + "/rmbtests/books?publicationYear=x&author=me", 400);
  }

  @Test
  public void year1(TestContext context) {
    checkURLs(context, "http://localhost:" + port + "/rmbtests/books?publicationYear=1&author=me", 400);
  }

  @Test
  public void withoutYearParameter(TestContext context) {
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
    postData(context, "http://localhost:" + port + "/rmbtests/books", Buffer.buffer(book),
        expectedStatus, 1, "application/json", TENANT, false);
  }

  @Test
  public void postBookNoParameters(TestContext context) {
    postBook(context, "", 422);
  }

  @Test
  public void postBookValidateAuthor(TestContext context) {
    postBook(context, "validate_field=author", 200);
  }

  @Test
  public void postBookValidateDescription(TestContext context) {
    postBook(context, "validate_field=data.description", 200);
  }

  @Test
  public void postBookValidateTitle(TestContext context) {
    postBook(context, "validate_field=data.title", 422);
  }

  @Test
  public void postBookValidateTitleAndDescription(TestContext context) {
    postBook(context, "validate_field=data.title&validate_field=data.description", 422);
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

    ObjectMapper om = new ObjectMapper();
    String book = om.writerWithDefaultPrettyPrinter().writeValueAsString(b);

    //check File Uploads
    postData(context, "http://localhost:" + port + "/admin/uploadmultipart", getBody("uploadtest.json", true), 200, 1, null, null, false);
    postData(context, "http://localhost:" + port + "/admin/uploadmultipart?file_name=test.json", getBody("uploadtest.json", true),
      200, 1, null, null, false);
    postData(context, "http://localhost:" + port + "/rmbtests/test", Buffer.buffer(book), 200, 1,
      "application/json", TENANT, false);

    d.setDatetime(new Datetime());
    d.setTitle("title");
    d.setLink("link");
    b.setStatus(0);
    b.setSuccess(true);
    book = om.writerWithDefaultPrettyPrinter().writeValueAsString(b);

    postData(context, "http://localhost:" + port + "/rmbtests/books", Buffer.buffer(book), 201, 1, "application/json", TENANT, true);
    postData(context, "http://localhost:" + port + "/rmbtests/books", Buffer.buffer(book), 201, 1, "application/json", TENANT, false);

    //check that additionalProperties (fields not appearing in schema) - returns 422
    JsonObject jo = new JsonObject(book);
    jo.put("lalala", "non existant");
    postData(context, "http://localhost:" + port + "/rmbtests/books", Buffer.buffer(jo.encode()), 422, 1, "application/json", TENANT, false);


    postData(context, "http://localhost:" + port + "/admin/loglevel?level=FINE&java_package=org", null, 200, 0, "application/json", TENANT, false);

    Metadata md = new Metadata();
    md.setCreatedByUserId("12345678-1234-1234-1234-123456789098");
    md.setCreatedByUsername("you");
    md.setCreatedDate(new Date());
    md.setUpdatedDate(new Date());
    md.setUpdatedByUserId("123456789098");
    b.setMetadata(md);
    postData(context, "http://localhost:" + port + "/rmbtests/books",
      Buffer.buffer(om.writerWithDefaultPrettyPrinter().writeValueAsString(b)), 422, 1, "application/json", TENANT, false);

    md.setUpdatedByUserId("12345678-1234-1234-1234-123456789098");
    postData(context, "http://localhost:" + port + "/rmbtests/books",
      Buffer.buffer(om.writerWithDefaultPrettyPrinter().writeValueAsString(b)), 201, 1, "application/json", TENANT, false);

    List<Object> list = getListOfBooks();

    checkURLs(context, "http://localhost:" + port + "/apidocs/index.html", 200); // should be 200
    checkURLs(context, "http://localhost:" + port + "/admin/loglevel", 200); // should be 200

    //use generated client
    checkClientCode(context);

    RmbtestsClient testClient = new RmbtestsClient("localhost", port, "abc", "abc", false);
    String[] facets = new String[]{"author:10", "name:5"};
    testClient.getBooks("aaa", new BigDecimal(1999), new BigDecimal(1999), null, facets, handler -> {
      if(handler.statusCode() != 200){
        context.fail();
      }
      else{
        log.info(handler.statusCode() + "----------------------------------------- passed ---------------------------");
      }
    });
  }

  /**
   * @param context
   *
   */
  private void checkClientCode(TestContext context)  {
    Async async = context.async(1);
    log.info("checkClientCode test");
    try {
      MimeMultipart mmp = new MimeMultipart();
      BodyPart bp = new MimeBodyPart(new InternetHeaders(),
        IOUtils.toByteArray(getClass().getClassLoader().getResourceAsStream("job.json")));
      bp.setDisposition("form-data");
      bp.setFileName("abc.raml");
      BodyPart bp2 = new MimeBodyPart(new InternetHeaders(),
        IOUtils.toByteArray(getClass().getClassLoader().getResourceAsStream("job.json")));
      bp2.setDisposition("form-data");
      bp2.setFileName("abcd.raml");
      log.debug("--- bp content --- "+bp.getContent());
      mmp.addBodyPart(bp);
      mmp.addBodyPart(bp2);
      AdminClient aClient = new AdminClient("localhost", port, "abc", "abc", false);
      aClient.postUploadmultipart(PersistMethod.SAVE, null, "abc",
        mmp, reply -> {
        if(reply.statusCode() != 200){
          context.fail();
        }
        log.debug("checkClientCode statusCode 1 " + reply.statusCode());
        String key;
        try {
          SecretKey sk = AES.generateSecretKey();
          key = AES.convertSecretKeyToString(sk);
          final String expected = AES.encryptPasswordAsBase64("abc", sk);
          aClient.postGetPassword(key, reply2 -> {
            reply2.bodyHandler(bodyHandler -> {
              if(!expected.equals(bodyHandler.toString())){
                context.fail("expected : " + expected + " got " + bodyHandler.toString());
              }
              else{
                log.info("received expected password: " + expected);
                aClient.getModuleStats( r -> {
                  r.bodyHandler( br -> {
                    log.info("received: " + br.toString());
                  });
                  async.countDown();
                });
              }
            });

          });
        } catch (Exception e) {
          log.error(e.getMessage(), e);
        }
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
          , Buffer.buffer(getFile("job.json")), 201, 1, "application/json", TENANT, false);
          postData(context, "http://localhost:" + port + url + "/" +location
          , Buffer.buffer(getFile("job_conf_post.json")), 204, 0, null, TENANT, false);
          postData(context, "http://localhost:" + port + url + "/12345"
          , Buffer.buffer(getFile("job_conf_post.json")), 404, 2, null, TENANT, false);
          postData(context, "http://localhost:" + port + url + "/" +location+ "/jobs/12345"
          , Buffer.buffer(getFile("job_conf_post.json")), 404, 2, null, TENANT, false);
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

  public void checkURLs(TestContext context, String url, int codeExpected) {
    String accept = "application/json";
    checkURLs(context, url, codeExpected, accept);
  }

  public void checkURLs(TestContext context, String url, int codeExpected, String accept) {
    try {
      Async async = context.async();
      HttpMethod method = HttpMethod.GET;
      HttpClient client = vertx.createHttpClient();
      HttpClientRequest request = client.requestAbs(method,
              url, new Handler<HttpClientResponse>() {
        @Override
        public void handle(HttpClientResponse httpClientResponse) {
          log.info(httpClientResponse.statusCode() + ", " + codeExpected + " status expected: " + url);
          context.assertEquals(codeExpected, httpClientResponse.statusCode(), url);
          httpClientResponse.bodyHandler( body -> {
            log.info(body.toString());
          });
          async.complete();
        }
      });
      request.exceptionHandler(error -> {
        context.fail(url + " - " + error.getMessage());
        async.complete();
      });
      request.headers().add("x-okapi-tenant", TENANT);
      request.headers().add("Accept", accept);
      request.setChunked(true);
      request.end();
    } catch (Throwable e) {
      log.error(e.getMessage(), e);
    } finally {
    }
  }

  private void httpClientTest(){

  }

  /**
   * for POST
   */
  private void postData(TestContext context, String url, Buffer buffer, int errorCode, int mode, String contenttype, String tenant, boolean userIdHeader) {
    Exception stacktrace = new RuntimeException();  // save stacktrace for async handler
    Async async = context.async();
    HttpClient client = vertx.createHttpClient();
    HttpClientRequest request = null;
    if(mode == 0){
      request = client.putAbs(url);
    }
    else if(mode == 1){
      request = client.postAbs(url);
    }
    else {
      request = client.deleteAbs(url);
    }
    request.exceptionHandler(error -> {
      async.complete();
      context.fail(new RuntimeException(error.getMessage(), stacktrace));
    }).handler(response -> {
      int statusCode = response.statusCode();
      // is it 2XX
      log.info(statusCode + ", " + errorCode + " expected status at "
            + System.currentTimeMillis() + " mode " + mode + " for " + url);

      if (statusCode == errorCode) {
        if(statusCode == 422){
          String str = response.getHeader("Content-type");
          if(str != null && str.contains("application/json")){
            context.assertTrue(true);
          }
          else{
            context.fail(new RuntimeException(
                "422 response code should contain a content type header of application/json",
                stacktrace));
          }
        }
        else if (statusCode == 201) {
          response.bodyHandler(responseData -> {
            String date = (String)new JsonPathParser(responseData.toJsonObject()).getValueAt("metadata.createdDate");
            if(date == null && userIdHeader){
              context.fail(new RuntimeException(
                  "metaData schema createdDate missing from returned json", stacktrace));
            }
          });
        }
        context.assertTrue(true);
      } else {
        response.bodyHandler(responseData -> {
          context.fail(new RuntimeException("got unexpected response code, expected: " +
              errorCode + ", received code: " + statusCode + " mode " + mode + " for url " +  url +
              "\ndata:" + responseData.toString(), stacktrace));
        });
      }
      if(!async.isCompleted()){
        async.complete();
      }
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
    if(contenttype != null){
      request.putHeader("Content-type",
        contenttype);
    }
    else{
      if(mode == 0 || mode == 2){
        request.putHeader("Content-type",
            "application/json");
      }
      else{
        request.putHeader("Content-type",
            "multipart/form-data; boundary=MyBoundary");
      }
    }
    if(buffer != null){
      request.write(buffer);
    }
    request.end();
  }

  private String getFile(String filename) throws IOException {
    return IOUtils.toString(getClass().getClassLoader().getResourceAsStream(filename), "UTF-8");
  }


  /**
   *
   * @param filename
   * @param closeBody - if creating a request with multiple parts in the body
   * close the body once all parts have been added - for example - passing multiple files
   * in the body - you would close the body after adding the final file
   * @return
   */
  private Buffer getBody(String filename, boolean closeBody) {
    Buffer buffer = Buffer.buffer();
    buffer.appendString("--MyBoundary\r\n");
    buffer.appendString("Content-Disposition: form-data; name=\"uploadtest\"; filename=\"uploadtest.json\"\r\n");
    buffer.appendString("Content-Type: application/octet-stream\r\n");
    buffer.appendString("Content-Transfer-Encoding: binary\r\n");
    buffer.appendString("\r\n");
    try {
      buffer.appendString(getFile(filename));
      buffer.appendString("\r\n");
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }
    if(closeBody){
      buffer.appendString("--MyBoundary--\r\n");
    }
    return buffer;
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
