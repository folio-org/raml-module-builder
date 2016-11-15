package org.folio;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.io.IOUtils;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.AdminClient;
import org.folio.rest.jaxrs.model.Book;
import org.folio.rest.jaxrs.model.Data;
import org.folio.rest.jaxrs.model.Datetime;
import org.folio.rest.persist.MongoCRUD;
import org.folio.rest.persist.mongo.DateEnum;
import org.folio.rest.persist.mongo.GroupBy;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * This is our JUnit test for our verticle. The test uses vertx-unit, so we declare a custom runner.
 */
@RunWith(VertxUnitRunner.class)
public class DemoRamlRestTest {

  private static Vertx vertx;
  private static int port;

  /**
   * @param context  the test context.
   */
  @Before
  public void setUp(TestContext context) throws IOException {
    vertx = Vertx.vertx();
    port = NetworkUtils.nextFreePort();

    try {
      startEmbeddedMongo();
      deployRestVerticle(context);
    } catch (Exception e) {
      context.fail(e);
    }
  }

  private static void startEmbeddedMongo() throws Exception {
    MongoCRUD.setIsEmbedded(true);
    MongoCRUD.getInstance(vertx).startEmbeddedMongo();
  }

  private static void deployRestVerticle(TestContext context) {
    DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(
        new JsonObject().put("http.port", port));
    vertx.deployVerticle(RestVerticle.class.getName(), deploymentOptions,
            context.asyncAssertSuccess());
  }


  /**
   * This method, called after our test, just cleanup everything by closing the vert.x instance
   *
   * @param context  the test context
   */
  @After
  public void tearDown(TestContext context) {
    MongoCRUD.stopEmbeddedMongo();
    deleteTempFilesCreated();
    vertx.close(context.asyncAssertSuccess());
  }

  private void deleteTempFilesCreated(){
    System.out.println("deleting created files");
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

  /**
   * just send a get request for books api with and without the required author query param
   * 1. one call should succeed and the other should fail (due to
   * validation aspect that should block the call and return 400)
   * 2. test the built in upload functionality
   * @param context - the test context
   */
  @Test
  public void test(TestContext context) throws Exception {
    //check GET
    checkURLs(context, "http://localhost:" + port + "/books?author=me", 200);
    checkURLs(context, "http://localhost:" + port + "/books", 400);
    checkURLs(context, "http://localhost:" + port + "/x/books", 400); // should be 404
    checkURLs(context, "http://localhost:" + port + "/admin/memory?history=true", 200, "text/html");

    //update periodic handler (MongoStatsPrinter) with which collection to print stats for and at which interval
    postData(context, "http://localhost:" + port + "/admin/collstats", Buffer.buffer("{\"books\": 30}"), 200, 0, null);

    //check File Uploads
    postData(context, "http://localhost:" + port + "/admin/upload", getBody("uploadtest.json", true), 400, 1, null);
    postData(context, "http://localhost:" + port + "/admin/upload?file_name=test.json", getBody("uploadtest.json", true),
      204, 1, null);

    //this will create an entry in the job conf and jobs collections as this is done automatically
    //when param SAVE_AND_NOTIFY is passed.
    postData(context, "http://localhost:" + port +
      "/admin/upload?file_name=test.json&bus_address=a.b.c&persist_method=SAVE_AND_NOTIFY",
      Buffer.buffer(getFile("uploadtest.json")), 204, 1, null);

    List<Object> list = getListOfBooks();

    //check bulk insert Mongo
    bulkInsert(context, list);

    //check insert with fail if id exists already Mongo
    insertUniqueTest(context, list.get(0));

    //check save and get of binary Mongo
    binaryInsert(context);

    //check save and get object with encoded binary base64 field Mongo
    //calls collection list and collection stats when done
    base64EncTest(context);

    checkURLs(context, "http://localhost:" + port + "/apidocs/index.html", 200); // should be 200

    //group by
    groupBy(context, "distinct");
    groupBy(context, "multi");
    groupBy(context, "max");
    groupBy(context, "sum");
    groupBy(context, "avg");
    groupBy(context, "all");
    groupBy(context, "pivot"); //for example: return all titles for each genre
    groupBy(context, null);
    groupBy(context, "expression");
    groupBy(context, "date");
    groupBy(context, "matching");

    jobsTest(context);

    AdminClient ac = new AdminClient("localhost", port, "abc");
    ac.getAdminJstack( response -> {
      StringBuffer sb = new StringBuffer();
      response.exceptionHandler( error -> {
        System.out.println("ERROR -> " + error.getMessage());
      });
      response.handler( data -> {
        sb.append(data);
      });
      response.endHandler(endHandler -> {
        System.out.println("JSTACK" + sb.toString());
      });
    });
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
      System.out.println("Status - " + statusCode + " at " + System.currentTimeMillis() + " for " +
        "http://localhost:" + port + url);

      if (statusCode == 201) {
        checkURLs(context, "http://localhost:" + port + url, 200);
        try {
          postData(context, "http://localhost:" + port + url + "/" +location+ "/jobs"
          , Buffer.buffer(getFile("job.json")), 201, 1, "application/json");
          postData(context, "http://localhost:" + port + url + "/" +location
          , Buffer.buffer(getFile("job_conf_post.json")), 204, 0, null);
          postData(context, "http://localhost:" + port + url + "/12345"
          , Buffer.buffer(getFile("job_conf_post.json")), 404, 2, null);
          postData(context, "http://localhost:" + port + url + "/" +location+ "/jobs/12345"
          , Buffer.buffer(getFile("job_conf_post.json")), 404, 2, null);
        } catch (Exception e) {
          e.printStackTrace();
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
      e.printStackTrace();
      context.fail("unable to read file");
    }
    request.end();
  }

  private void getCollectionStats(TestContext context){
    Async async = context.async();
    MongoCRUD.getInstance(vertx).getStatsForCollection("books", reply -> {
      if(reply.succeeded()){
        //System.out.println(reply.result().encodePrettily());
        context.assertInRange(7 ,(reply.result().getInteger("count")), 1);
      }
      else{
        context.fail(reply.cause().getMessage());
      }
      async.complete();
    });
  }

  private void getCollectionList(TestContext context){
    Async async = context.async();
    MongoCRUD.getInstance(vertx).getListOfCollections(reply -> {
      if(reply.succeeded()){
        context.assertInRange(2 , reply.result().size(), 1);
        System.out.println("list of collection success");
      }
      else{
        context.fail(reply.cause().getMessage());
      }
      async.complete();
    });
  }

  private void base64EncTest(TestContext context){
    Book b = createBook();
    String file = getClass().getClassLoader().getResource("folio.jpg").getFile();
    Buffer buffer = Buffer.buffer();
    Async async = context.async();
    vertx.fileSystem().open(file, new OpenOptions(), ar -> {
      if (ar.succeeded()) {
        AsyncFile rs = ar.result();
        rs.handler( buf -> {
          buffer.appendBuffer(buf);
        });
        rs.exceptionHandler(t -> {
          context.fail();
          async.complete();
        });
        rs.endHandler(v -> {
          ar.result().close(ar2 -> {
            if (ar2.failed()) {
              context.fail();
            }
            else{
              Async async2 = context.async();
              b.setImage(Base64.getEncoder().encodeToString(buffer.getBytes()));
              MongoCRUD client = MongoCRUD.getInstance(vertx);
              client.save("books", b, reply -> {
                if(reply.succeeded()){
                  String id = reply.result();
                  client.get(Book.class.getName(), "books", id, reply2 -> {
                    Async async3 = context.async();
                    if(reply2.succeeded()){
                      byte[] image = Base64.getDecoder().decode(((Book)reply2.result()).getImage());
                      if(image.length == 2747){
                        context.assertTrue(true);
                        System.out.println("save binary success");

                        //check collection stats Mongo
                        getCollectionStats(context);

                        //get list of collections Mongo
                        getCollectionList(context);
                      }
                      else{
                        context.fail("size of file is incorrect");
                      }
                      /*try {
                        org.apache.commons.io.FileUtils.writeByteArrayToFile(new File("test.jpg"), image);
                      } catch (Exception e) {
                        e.printStackTrace();
                      }*/
                    }else{
                      context.fail(reply2.cause().getMessage());
                    }
                    async3.complete();
                  });
                }
                else{
                  context.fail(reply.cause().getMessage());
                }
                async2.complete();
              });
            }
          });
          async.complete();
        });
      }});


  }

  private void binaryInsert(TestContext context){
    //check binary save in mongo
    String file = getClass().getClassLoader().getResource("folio.jpg").getFile();
    Buffer buffer = Buffer.buffer();
    Async async = context.async();
    vertx.fileSystem().open(file, new OpenOptions(), ar -> {
      if (ar.succeeded()) {
        AsyncFile rs = ar.result();
        rs.handler( buf -> {
          buffer.appendBuffer(buf);
        });
        rs.exceptionHandler(t -> {
          context.fail();
          async.complete();
        });
        rs.endHandler(v -> {
          ar.result().close(ar2 -> {
            if (ar2.failed()) {
              context.fail();
            }
            else{
              Async async1 = context.async();
              MongoCRUD client = MongoCRUD.getInstance(vertx);
              client.saveBinary("files", buffer, "images", reply -> {
                if(reply.succeeded()){
                  //context.assertTrue(true, "Binary image file saved successfully");
                  Async async2 = context.async();
                  String id = reply.result();
                  client.getBinary("files","images", 0, 1, res2 -> {
                    if(res2.succeeded()) {

                      byte[]image = (byte[])((List<?>)res2.result()).get(0);
                      if(image.length == 2747){
                        context.assertTrue(true);
                        System.out.println("save binary success");
                      }
                      else{
                        context.fail("size of file is incorrect");
                      }
                    } else {
                      context.fail(res2.cause().getMessage());
                    }
                    async2.complete();
                  });
                }
                else{
                  context.fail();
                }
                async1.complete();
              });
            }
          });
          async.complete();
        });
      } else {
        context.fail();
        async.complete();
      }
    });
  }

  private void groupBy(TestContext context, String op){
    //check bulk insert in MONGO
    Async async = context.async();
    GroupBy gb = new GroupBy();
    if(op == null){
      gb = new GroupBy(null);
    }
    if("multi".equals(op) || "all".equals(op)){
      //group by two fields
      gb.addGroupByField("data.author").addGroupByField("data.genre");
    }
    else if("pivot".equals(op)){
      //show all titles per genre
      gb.addGroupByField("data.genre", "titles", "data.title");
    }
    else if("date".equals(op)){
      //must pass a valid mongo date field as the second argument
      gb.addGroupByField("year","data.datetime", DateEnum.YEAR).
      addGroupByField("month","data.datetime", DateEnum.DAY_OF_MONTH);
      gb.addCount();
    }
    else if(op != null){
      //group by author
      gb.addGroupByField("data.author");
    }
    if("expression".equals(op)){
      String expression = "{ \"$sum\": {\"$multiply\": [ \"$status\", \"$status\" ] }}";
      gb.addConstraint("alias", new JsonObject(expression));
    }
    if("sum".equals(op)  || "all".equals(op) || op == null){
      //add a count value of items in group
      gb.addCount();
    }
    if("avg".equals(op)  || "all".equals(op) || op == null){
      gb.addAVGForField("average","status");
    }
    if("max".equals(op)  || "all".equals(op) || op == null){
      gb.addMAXForField("max", "status");
    }
    if("matching".equals(op)){
      //remove random values from object and leave only constant values
      //so that we have a match
      Book book = createBook();
      book.getData().setAuthor(null);
      book.getData().setDatetime(null);
      book.getData().setTitle(null);
      book.setStatus(null);

      MongoCRUD.getInstance(vertx).groupBy("books", gb, book, reply -> {
        if(reply.succeeded()){
          System.out.println(op + " group by result: "+reply.result());
        }
        else{
          context.fail();
          System.out.println(reply.cause().getMessage());
        }
        async.complete();
      });
    }
    else{
      MongoCRUD.getInstance(vertx).groupBy("books", gb, reply -> {
        if(reply.succeeded()){
          System.out.println(op + " group by result: "+reply.result());
        }
        else{
          context.fail();
          System.out.println(reply.cause().getMessage());
        }
        async.complete();
      });
    }
  }

  private void bulkInsert(TestContext context, List<Object> list){
    //check bulk insert in MONGO
    Async async = context.async();
    MongoCRUD.getInstance(vertx).bulkInsert("books", list, reply -> {
      if(reply.succeeded()){
        context.assertEquals(5, reply.result().getInteger("n"),
          "bulk insert updated " + reply.result().getInteger("n") + " records instead of 5");
      }
      else{
        context.fail();
        System.out.println(reply.cause().getMessage());
      }
      async.complete();
    });
  }

  private void insertUniqueTest(TestContext context, Object book){
    //insert fail if id exists test MONGO
    Async async2 = context.async();
    MongoCRUD.getInstance(vertx).save("books", book, reply -> {
      if(reply.succeeded()){
        String id = reply.result();
        ((Book)book).setId(id);
        Async async3 = context.async();
        MongoCRUD.getInstance(vertx).save("books", book, true , reply2 -> {
          if(reply2.succeeded()){
            context.fail("save succeeded but should have failed!");
          }
          else{
            context.assertTrue(true);
          }
          async3.complete();
        });
      }
      else{
        context.fail();
        System.out.println(reply.cause().getMessage());
      }
      async2.complete();
    });
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

          context.assertTrue(httpClientResponse.statusCode() == codeExpected);
          System.out.println("status " + url + " " + httpClientResponse.statusCode());
          httpClientResponse.bodyHandler( body -> {
            System.out.println(body.toString());
          });
          async.complete();
        }
      });
      request.exceptionHandler(error -> {
        context.fail(error.getMessage());
        async.complete();
      });
      request.headers().add("Authorization", "abcdefg");
      request.headers().add("Accept", accept);
      request.setChunked(true);
      request.end();
    } catch (Throwable e) {
      e.printStackTrace();
    } finally {
    }
  }

  /**
   * for POST
   */
  private void postData(TestContext context, String url, Buffer buffer, int errorCode, int mode, String contenttype) {
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
      context.fail(error.getMessage());
    }).handler(response -> {
      int statusCode = response.statusCode();
      // is it 2XX
      System.out.println("Status - " + statusCode + " at " + System.currentTimeMillis() + " for " + url);

      if (statusCode == errorCode) {
        context.assertTrue(true);
      } else {
        response.bodyHandler(responseData -> {
          context.fail("got non 200 response, error: " + responseData + " code " + statusCode);
        });
      }
      if(!async.isCompleted()){
        async.complete();
      }
    });
    request.setChunked(true);
    request.putHeader("Authorization", "abcdefg");
    request.putHeader("Accept", "application/json,text/plain");
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
    request.write(buffer);
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
      e.printStackTrace();

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
