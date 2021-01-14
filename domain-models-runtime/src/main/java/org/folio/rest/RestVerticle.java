package org.folio.rest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.ValidatorFactory;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.google.common.base.Joiner;
import com.google.common.io.ByteStreams;

import io.vertx.core.Future;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.folio.okapi.common.logging.FolioLoggingContext;
import org.folio.rest.annotations.Stream;
import org.folio.rest.jaxrs.model.Error;
import org.folio.rest.jaxrs.model.Errors;
import org.folio.rest.jaxrs.model.Parameter;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.AnnotationGrabber;
import org.folio.rest.tools.ClientGenerator;
import org.folio.rest.tools.PomReader;
import org.folio.rest.tools.RTFConsts;
import org.folio.rest.tools.client.exceptions.ResponseException;
import org.folio.rest.tools.client.test.HttpClientMock2;
import org.folio.rest.tools.messages.MessageConsts;
import org.folio.rest.tools.messages.Messages;
import org.folio.rest.tools.utils.AsyncResponseResult;
import org.folio.rest.tools.utils.BinaryOutStream;
import org.folio.rest.tools.utils.InterfaceToImpl;
import org.folio.rest.tools.utils.JsonUtils;
import org.folio.rest.tools.utils.LogUtil;
import org.folio.rest.tools.utils.MetadataUtil;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.rest.tools.utils.OutStream;
import org.folio.rest.tools.utils.ResponseImpl;
import org.folio.rest.tools.utils.ValidationHelper;
import org.folio.util.StringUtil;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.StaticHandler;

public class RestVerticle extends AbstractVerticle {

  public static final String        OKAPI_HEADER_TENANT             = ClientGenerator.OKAPI_HEADER_TENANT;
  public static final String        OKAPI_HEADER_TOKEN              = "x-okapi-token";
  public static final String        OKAPI_HEADER_PREFIX             = "x-okapi";
  public static final String        OKAPI_USERID_HEADER             = "X-Okapi-User-Id";
  public static final String        OKAPI_REQUESTID_HEADER          = "X-Okapi-Request-Id";
  public static final String        STREAM_ID                       =  "STREAMED_ID";
  public static final String        STREAM_COMPLETE                 =  "COMPLETE";
  public static final String        STREAM_ABORT                    =  "STREAMED_ABORT";

  public static final Map<String, String> MODULE_SPECIFIC_ARGS  = new HashMap<>(); //NOSONAR

  private static final String       SUPPORTED_CONTENT_TYPE_JSON_DEF = "application/json";
  private static final String       DEFAULT_CONTENT_TYPE            = "application/json";
  private static final String       SUPPORTED_CONTENT_TYPE_TEXT_DEF = "text/plain";
  private static final String       FILE_UPLOAD_PARAM               = "javax.mail.internet.MimeMultipart";
  private static final String       HTTP_PORT_SETTING               = "http.port";
  private static String             className                       = RestVerticle.class.getName();
  private static final Logger       log                             = LogManager.getLogger(RestVerticle.class);
  private static final ObjectMapper MAPPER                          = ObjectMapperTool.getMapper();

  private static final String[]     DATE_PATTERNS = {
    "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
    "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
    "yyyy-MM-dd'T'HH:mm:ss'Z'",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd",
    "yyyy-MM",
    "yyyy"
  };

  private static ValidatorFactory   validationFactory;
  private static String             deploymentId                     = "";

  private final Messages            messages                        = Messages.getInstance();

  static {
    //validationFactory used to validate the pojos which are created from the json
    //passed in the request body in put and post requests. The constraints validated by this factory
    //are the ones in the json schemas accompanying the raml files
    validationFactory = Validation.buildDefaultValidatorFactory();
  }

  // https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html
  // first match - no q val check
  static String acceptCheck(JsonArray l, String h) {
    String []hl = h.split(",");
    String hBest = null;
    for (int i = 0; i < hl.length; i++) {
      String mediaRange = hl[i].split(";")[0].trim();
      for (int j = 0; j < l.size(); j++) {
        String c = l.getString(j);
        if (mediaRange.compareTo("*/*") == 0 || c.equalsIgnoreCase(mediaRange)) {
          hBest = c;
          break;
        }
      }
    }
    return hBest;
  }

  @Override
  public void start(Promise<Void> startPromise) throws Exception {

    readInGitProps();

    //process cmd line arguments
    cmdProcessing();

    deploymentId = UUID.randomUUID().toString();

    LogUtil.formatLogMessage(className, "start", "metrics enabled: " + vertx.isMetricsEnabled());

    // maps paths found in raml to the generated functions to route to when the paths are requested
    MappedClasses mappedURLs = populateConfig();

    // set of exposed urls as declared in the raml
    Set<String> urlPaths = mappedURLs.getAvailURLs();

    // create a map of regular expression to url path
    Map<String, Pattern> regex2Pattern = mappedURLs.buildURLRegex();

    // Create a router object.
    Router router = Router.router(vertx);

    log.info(context.getInstanceCount() + " verticles deployed ");

    // run pluggable startup code in a class implementing the InitAPI interface
    // in the "org.folio.rest.impl" package
    runHook(vv -> {
      if (((Future<?>) vv).failed()) {
        String reason = ((Future<?>) vv).cause().getMessage();
        log.error( messages.getMessage("en", MessageConsts.InitializeVerticleFail, reason));
        startPromise.fail(reason);
        vertx.close();
        System.exit(-1);
      } else {
        log.info("init succeeded.......");
        try {
          // startup periodic impl if exists
          runPeriodicHook();
        } catch (Exception e2) {
          log.error(e2.getMessage(), e2);
        }
        //single handler for all url calls other then documentation
        //which is handled separately
        router.routeWithRegex("^(?!.*apidocs).*$").handler(rc -> route(mappedURLs, urlPaths, regex2Pattern, rc));
        // routes requests on “/assets/*” to resources stored in the “assets”
        // directory.
        router.route("/assets/*").handler(StaticHandler.create("assets"));

        // In the following example all requests to paths starting with
        // /apidocs/ will get served from the directory resources/apidocs:
        // example:
        // http://localhost:8181/apidocs/index.html?raml=raml/_patrons.raml
        router.route("/apidocs/*").handler(StaticHandler.create("apidocs"));
        // startup http server on port 8181 to serve documentation

        String portS = System.getProperty(HTTP_PORT_SETTING);
        int port;
        if (portS != null) {
          port = Integer.parseInt(portS);
          config().put(HTTP_PORT_SETTING, port);
        } else {
          // we are here if port was not passed via cmd line
          port = config().getInteger(HTTP_PORT_SETTING, 8081);
        }

        //check if mock mode requested and set sys param so that http client factory
        //can config itself accordingly
        String mockMode = config().getString(HttpClientMock2.MOCK_MODE);
        if(mockMode != null){
          System.setProperty(HttpClientMock2.MOCK_MODE, mockMode);
        }

        //if client includes an Accept-Encoding header which includes
        //the supported compressions - deflate or gzip.
        HttpServerOptions serverOptions = new HttpServerOptions();
        serverOptions.setCompressionSupported(true);

        HttpServer server = vertx.createHttpServer(serverOptions);
        server.requestHandler(router)
        // router object (declared in the beginning of the atrt function accepts request and will pass to next handler for
        // specified path

        .listen(port,
          // Retrieve the port from the configuration file - file needs to
          // be passed as arg to command line,
          // for example: -conf src/main/conf/my-application-conf.json
          // default to 8181.
          result -> {
            if (result.failed()) {
              startPromise.fail(new RuntimeException("Listening on port " + port, result.cause()));
            } else {
              try {
                runPostDeployHook( res2 -> {
                  if(!res2.succeeded()){
                    log.error(res2.cause().getMessage(), res2.cause());
                  }
                });
              } catch (Exception e) {
                log.error(e.getMessage(), e);
              }
              LogUtil.formatLogMessage(className, "start", "http server for apis and docs started on port " + port + ".");
              LogUtil.formatLogMessage(className, "start", "Documentation available at: " + "http://localhost:" + port + "/apidocs/");
              startPromise.complete();
            }
          });
      }
    });
  }

  /**
   * Match the path agaist pattern.
   * @return the matching groups urldecoded, may be an empty array, or null if the pattern doesn't match
   */
  @SuppressWarnings("java:S1168")  // suppress "Empty arrays should be returned instead of null"
  static String[] matchPath(String path, Pattern pattern) {
    Matcher m = pattern.matcher(path);
    if (! m.find()) {
      return null;
    }

    int groups = m.groupCount();
    // pathParams are the place holders in the raml query string
    // for example /admin/{admin_id}/yyy/{yyy_id} - the content in between the {} are path params
    // they are replaced with actual values and are passed to the function which the url is mapped to
    String[] pathParams = new String[groups];
    for (int i = 0; i < groups; i++) {
      pathParams[i] = StringUtil.urlDecode(m.group(i + 1));
    }
    return pathParams;
  }

  /**
   * Handler for all url calls other then documentation.
   * @param mappedURLs  maps paths found in raml to the generated functions to route to when the paths are requested
   * @param urlPaths  set of exposed urls as declared in the raml
   * @param regex2Pattern  create a map of regular expression to url path
   * @param rc  RoutingContext of this URL
   */
  void route(MappedClasses mappedURLs, Set<String> urlPaths, Map<String, Pattern> regex2Pattern,
      RoutingContext rc) {
    long start = System.nanoTime();
    try {
      boolean validPath = false;
      boolean[] validRequest = { true };
      // urlPaths = list of regex urls created from urls declared in the raml.
      // loop over regex patterns and try to match them against the requested
      // URL if no match is found, then the requested url is not supported by
      // the ramls and we return an error - this has positive security implications as well
      for (String urlPath : urlPaths) {
        String[] pathParams = matchPath(rc.request().path(), regex2Pattern.get(urlPath));
        if (pathParams != null) {  // if matched
          validPath = true;

          // get the function that should be invoked for the requested
          // path + requested http_method pair
          JsonObject ret = mappedURLs.getMethodbyPath(urlPath, rc.request().method().toString());
          // if a valid path was requested but no function was found
          if (ret == null) {

            // if the path is valid and the http method is options
            // assume a cors request
            if (rc.request().method() == HttpMethod.OPTIONS) {
              rc.response().end();
              return;
            }

            // the url exists but the http method requested does not match a function
            // meaning url+http method != a function
            endRequestWithError(rc, 400, true, messages.getMessage("en", MessageConsts.HTTPMethodNotSupported),
              validRequest);
            return;
          }
          Class<?> aClass;
          try {
            if (validRequest[0]) {
              //create okapi headers map and inject into function
              Map<String, String> okapiHeaders = new CaseInsensitiveMap<>();
              String []tenantId = new String[]{null};
              getOkapiHeaders(rc, okapiHeaders, tenantId);
              if(tenantId[0] == null && !rc.request().path().startsWith("/admin")){
                //if tenant id is not passed in and this is not an /admin request, return error
                endRequestWithError(rc, 400, true, messages.getMessage("en", MessageConsts.UnableToProcessRequest)
                  + " Tenant must be set", validRequest);
              }

              if (validRequest[0]) {
                //get interface mapped to this url
                String iClazz = ret.getString(AnnotationGrabber.CLASS_NAME);
                // convert from interface to an actual class implementing it, which appears in the impl package
                aClass = InterfaceToImpl.convert2Impl(RTFConsts.PACKAGE_OF_IMPLEMENTATIONS, iClazz, false).get(0);
                Object o = null;
                // call back the constructor of the class - gives a hook into the class not based on the apis
                // passing the vertx and context objects in to it.
                try {
                  o = aClass.getConstructor(Vertx.class, String.class).newInstance(vertx, tenantId[0]);
                } catch (Exception e) {
                  // if no such constructor was implemented call the
                  // default no param constructor to create the object to be used to call functions on
                  o = aClass.newInstance();
                }
                final Object instance = o;

                // function to invoke for the requested url
                String function = ret.getString(AnnotationGrabber.FUNCTION_NAME);
                // parameters for the function to invoke
                JsonObject params = ret.getJsonObject(AnnotationGrabber.METHOD_PARAMS);
                // all methods in the class whose function is mapped to the called url
                // needed so that we can get a reference to the Method object and call it via reflection
                Method[] methods = aClass.getMethods();
                // what the api will return as output (Accept)
                JsonArray produces = ret.getJsonArray(AnnotationGrabber.PRODUCES);
                // what the api expects to get (content-type)
                JsonArray consumes = ret.getJsonArray(AnnotationGrabber.CONSUMES);

                HttpServerRequest request = rc.request();

                //check that the accept and content-types passed in the header of the request
                //are as described in the raml
                checkAcceptContentType(produces, consumes, rc, validRequest);

                //Get method in class to be run for this requested API endpoint
                Method[] method2Run = new Method[]{null};
                for (int i = 0; i < methods.length; i++) {
                  if (methods[i].getName().equals(function)) {
                    method2Run[0] = methods[i];
                    break;
                  }
                }
                //is function annotated to receive data in chunks as they come in.
                //Note that the function controls the logic to this if this is the case
                boolean streamData = isStreamed(method2Run[0].getAnnotations());
                // check if we are dealing with a file upload , currently only multipart/form-data and application/octet
                //in the raml definition for such a function
                final boolean[] isContentUpload = new boolean[] { false };
                final int[] uploadParamPosition = new int[] { -1 };
                params.forEach(param -> {
                  if(((JsonObject) param.getValue()).getString("type").equals("java.io.InputStream")){
                    //application/octet-stream passed - this is handled in a stream like manner
                    //and the corresponding function called must annotate with a @Stream - and be able
                    //to handle the function being called repeatedly on parts of the data
                    uploadParamPosition[0] = ((JsonObject) param.getValue()).getInteger("order");
                    isContentUpload[0] = true;
                  }
                });

                // create the array and then populate it by parsing the url parameters which are needed to invoke the function mapped
                //to the requested URL - array will be populated by parseParams() function
                Iterator<Map.Entry<String, Object>> paramList = params.iterator();
                Object[] paramArray = new Object[params.size()];

                if (streamData) {
                  parseParams(rc, null, paramList, validRequest, consumes, paramArray, pathParams, okapiHeaders);
                  handleStream(method2Run[0], rc, request, instance, tenantId, okapiHeaders,
                    uploadParamPosition, paramArray, validRequest, start);
                } else {
                  // regular request (no streaming).. Read the request body before checking params + body
                  Buffer body = Buffer.buffer();
                  rc.request().handler(body::appendBuffer);
                  rc.request().endHandler(endRes -> {
                    parseParams(rc, body, paramList, validRequest, consumes, paramArray, pathParams, okapiHeaders);
                    if (validRequest[0]) {
                      //if request is valid - invoke it
                      try {
                        invoke(method2Run[0], paramArray, instance, rc,  tenantId, okapiHeaders, new StreamStatus(), v -> {
                          withRequestId(rc, () -> LogUtil.formatLogMessage(className, "start", " invoking " + function));
                          sendResponse(rc, v, start, tenantId[0]);
                        });
                      } catch (Exception e1) {
                        withRequestId(rc, () -> log.error(e1.getMessage(), e1));
                        rc.response().end();
                      }
                    } else {
                      endRequestWithError(rc, 400, true, messages.getMessage("en", MessageConsts.UnableToProcessRequest),
                          validRequest);
                    }
                  });
                }
              }
            }
          } catch (Exception e) {
            withRequestId(rc, () -> log.error(e.getMessage(), e));
            endRequestWithError(rc, 400, true, messages.getMessage("en", MessageConsts.UnableToProcessRequest) + e.getMessage(),
              validRequest);
            return;
          }
        }
      }
      if (!validPath) {
        // invalid path
        endRequestWithError(rc, 400, true,
          messages.getMessage("en", MessageConsts.InvalidURLPath, rc.request().path()), validRequest);
      }
    } catch (Exception e) {
      withRequestId(rc, () -> log.error(e.getMessage(), e));
      endRequestWithError(rc, 500, true, "Server error", new boolean[] { true });
    }
  }

  private void handleStream(Method method2Run, RoutingContext rc, HttpServerRequest request,
      Object instance, String[] tenantId, Map<String, String> okapiHeaders,
      int[] uploadParamPosition, Object[] paramArray, boolean[] validRequest, long start){
    request.handler(new Handler<Buffer>() {
      @Override
      public void handle(Buffer buff) {
        try {
          StreamStatus stat = new StreamStatus();
          stat.setStatus(0);
          paramArray[uploadParamPosition[0]] =
              new ByteArrayInputStream( buff.getBytes() );
          invoke(method2Run, paramArray, instance, rc,  tenantId, okapiHeaders, stat, v -> {
            withRequestId(rc, () -> LogUtil.formatLogMessage(className, "start", " invoking " + method2Run));
          });
        } catch (Exception e1) {
          withRequestId(rc, () -> log.error(e1.getMessage(), e1));
          rc.response().end();
        }
      }
    });
    request.endHandler( e -> {
      StreamStatus stat = new StreamStatus();
      stat.setStatus(1);
      paramArray[uploadParamPosition[0]] = new ByteArrayInputStream(new byte [0]);
      invoke(method2Run, paramArray, instance, rc,  tenantId, okapiHeaders, stat, v -> {
        withRequestId(rc, () -> LogUtil.formatLogMessage(className, "start", " invoking " + method2Run));
        //all data has been stored in memory - not necessarily all processed
        sendResponse(rc, v, start, tenantId[0]);
      });
    });
    request.exceptionHandler(new Handler<Throwable>() {
      @Override
      public void handle(Throwable event) {
        StreamStatus stat = new StreamStatus();
        stat.setStatus(2);
        paramArray[uploadParamPosition[0]] = new ByteArrayInputStream(new byte[0]);
        invoke(method2Run, paramArray, instance, rc, tenantId, okapiHeaders, stat,
            v -> withRequestId(rc, () ->
              LogUtil.formatLogMessage(className, "start", " invoking " + method2Run))
        );
        endRequestWithError(rc, 400, true, "unable to upload file " + event.getMessage(), validRequest);
      }
    });
  }

  private void readInGitProps(){
    InputStream in = getClass().getClassLoader().getResourceAsStream("git.properties");
    if (in != null) {
      try {
        Properties prop = new Properties();
        prop.load(in);
        in.close();
        log.info("git: " + prop.getProperty("git.remote.origin.url")
                + " " + prop.getProperty("git.commit.id"));
      } catch (Exception e) {
        log.warn(e.getMessage());
      }
    }
  }

  /**
   * @param annotations
   * @return
   */
  private boolean isStreamed(Annotation[] annotations) {
    for (int i = 0; i < annotations.length; i++) {
      if(annotations[i].annotationType().equals(Stream.class)){
        return true;
      }
    }
    return false;
  }

  /**
   * @return a {@link Response} extracted from asyncResult, either from result(), or from
   *         cause().getResponse() if cause() is a {@link ResponseException}, or null otherwise
   */
  static Response getResponse(AsyncResult<Response> asyncResult) {
    if (asyncResult.succeeded()) {
      return asyncResult.result();
    }
    Throwable exception = asyncResult.cause();
    if (exception instanceof ResponseException) {
      return ((ResponseException) exception).getResponse();
    }
    return null;
  }

  /**
   * Send the result as response.
   *
   * @param rc
   *          - where to send the result
   * @param v
   *          - the result to send
   * @param start
   *          - request's start time, using JVM's high-resolution time source, in nanoseconds
   */
  private void sendResponse(RoutingContext rc, AsyncResult<Response> v, long start, String tenantId) {
    Response responseFromResult = getResponse(v);
    if (responseFromResult == null) {
      // catch all
      endRequestWithError(rc, 500, true, "Server error", new boolean[] { true });
      return;
    }
    Object entity = null;
    try {
      HttpServerResponse response = rc.response();
      int statusCode = responseFromResult.getStatus();
      // 204 means no content returned in the response, so passing
      // a chunked Transfer header is not allowed
      if (statusCode != 204) {
        response.setChunked(true);
      }

      response.setStatusCode(statusCode);

      // !!!!!!!!!!!!!!!!!!!!!! CORS commented OUT!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      // response.putHeader("Access-Control-Allow-Origin", "*");

      copyHeadersJoin(responseFromResult.getStringHeaders(), response.headers());

      entity = responseFromResult.getEntity();

      /* entity is of type OutStream - and will be written as a string */
      if (entity instanceof OutStream) {
        response.write(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(((OutStream) entity).getData()));
      }
      /* entity is of type BinaryOutStream - and will be written as a buffer */
      else if(entity instanceof BinaryOutStream){
        response.write(Buffer.buffer(((BinaryOutStream) entity).getData()));
      }
      /* data is a string so just push it out, no conversion needed */
      else if(entity instanceof String){
        response.write(Buffer.buffer((String)entity));
      }
      /* catch all - anything else will be assumed to be a pojo which needs converting to json */
      else if (entity != null) {
        response.write(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(entity));
      }
    } catch (Exception e) {
      withRequestId(rc, () -> log.error(e.getMessage(), e));
    } finally {
      rc.response().end();
    }

    long end = System.nanoTime();

    StringBuilder sb = new StringBuilder();
    if (log.isDebugEnabled()) {
      try {
        sb.append(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(entity));
      } catch (Exception e) {
        String name = entity == null ? "null" : entity.getClass().getName();
        withRequestId(rc, () -> log.error("writeValueAsString(" + name + ")", e));
      }
    }

    withRequestId(rc, () -> LogUtil.formatStatsLogMessage(rc, (end - start) / 1000000, tenantId, sb.toString()));
  }

  /**
   * Copy the headers from source to destination. Join several headers of same key using "; ".
   */
  private void copyHeadersJoin(MultivaluedMap<String,String> source, MultiMap destination) {
    for (Entry<String, List<String>> entry : source.entrySet()) {
      String jointValue = Joiner.on("; ").join(entry.getValue());
      try {
        destination.add(entry.getKey(), jointValue);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            e.getMessage() + ": " + entry.getKey() + " - " + jointValue, e);
      }
    }
  }

  void endRequestWithError(RoutingContext rc, int status, boolean chunked, String message, boolean[] isValid) {
    if (isValid[0]) {
      HttpServerResponse response = rc.response();
      if (!response.closed()) {
        response.setChunked(chunked);
        response.setStatusCode(status);
        if (status == 422) {
          response.putHeader("Content-type", SUPPORTED_CONTENT_TYPE_JSON_DEF);
        } else {
          response.putHeader("Content-type", SUPPORTED_CONTENT_TYPE_TEXT_DEF);
        }
        if (message != null) {
          response.write(message);
        }
        response.end();
      }
      withRequestId(rc, () ->
        LogUtil.formatStatsLogMessage(rc, -1, null, message == null ? "" : message));
    }
    // once we are here the call is not valid
    isValid[0] = false;
  }

  private void getOkapiHeaders(RoutingContext rc, Map<String, String> headers, String[] tenantId){
    MultiMap mm = rc.request().headers();
    Consumer<Map.Entry<String,String>> consumer = entry -> {
      String headerKey = entry.getKey().toLowerCase();
      if(headerKey.startsWith(OKAPI_HEADER_PREFIX)){
        if(headerKey.equalsIgnoreCase(ClientGenerator.OKAPI_HEADER_TENANT)){
          tenantId[0] = entry.getValue();
        }
        headers.put(headerKey, entry.getValue());
      }
    };
    mm.forEach(consumer);
  }

  private void invoke(Method method, Object[] params, Object o, RoutingContext rc, String[] tenantId,
      Map<String,String> headers, StreamStatus streamed, Handler<AsyncResult<Response>> resultHandler) {

    String generateRCforFunc = PomReader.INSTANCE.getProps().getProperty("generate_routing_context");
    boolean addRCParam = false;
    if(generateRCforFunc != null){
      String []addRC = generateRCforFunc.split(",");
      for (int i = 0; i < addRC.length; i++) {
        if(addRC[i].equals(rc.request().path())){
          addRCParam = true;
        }
      }
    }

    //if streaming is requested the status will be 0 (streaming started)
    //or 1 streaming data complete
    //or 2 streaming aborted
    //otherwise it will be -1 and flags wont be set
    if(streamed.status == 0){
      headers.put(STREAM_ID, String.valueOf(rc.hashCode()));
    }
    else if(streamed.status == 1){
      headers.put(STREAM_ID, String.valueOf(rc.hashCode()));
      headers.put(STREAM_COMPLETE, String.valueOf(rc.hashCode()));
    }
    else if (streamed.status == 2){
      headers.put(STREAM_ID, String.valueOf(rc.hashCode()));
      headers.put(STREAM_ABORT, String.valueOf(rc.hashCode()));
    }

    Object[] newArray = new Object[params.length];
    int size = 3;
    int pos = 0;

    //this endpoint indicated it wants to receive the routing context as a parameter
    if(addRCParam){
      //the amount of extra params added is 4 not 3
      size = 4;
      //the first param of the extra params is the injected RC
      newArray[params.length - size] = rc;
      pos = 1;
    }

    for (int i = 0; i < params.length - size; i++) {
      newArray[i] = params[i];
    }

    //inject call back handler into each function
    newArray[params.length - (size-(pos+1))] = resultHandler;

    //inject vertx context into each function
    newArray[params.length - (size-(pos+2))] = context;

    newArray[params.length - (size-pos)] = headers;

    headers.forEach(FolioLoggingContext::put);

    try {
      method.invoke(o, newArray);
      // response.setChunked(true);
      // response.setStatusCode(((Response)result).getStatus());
    } catch (Exception e) {
      withRequestId(rc, () -> log.error(e.getMessage(), e));
      String message;
      try {
        // catch exception for now in case of null point and show generic
        // message
        message = e.getCause().getMessage();
      } catch (Throwable ee) {
        message = messages.getMessage("en", MessageConsts.UnableToProcessRequest);
      }
      endRequestWithError(rc, 400, true, message, new boolean[]{true});
    }
  }

  public JsonObject loadConfig(String configFile) {
    try {
      byte[] jsonData = ByteStreams.toByteArray(getClass().getClassLoader().getResourceAsStream(configFile));
      return new JsonObject(new String(jsonData));
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }
    return new JsonObject();
  }

  private MappedClasses populateConfig() {
    MappedClasses mappedURLs = new MappedClasses();
    JsonObject jObjClasses = new JsonObject();
    try {
      jObjClasses.mergeIn(AnnotationGrabber.generateMappings());
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
    // loadConfig(JSON_URL_MAPPINGS);
    Set<String> classURLs = jObjClasses.fieldNames();
    classURLs.forEach(classURL -> {
      log.info(classURL);
      JsonObject jObjMethods = jObjClasses.getJsonObject(classURL);
      Set<String> methodURLs = jObjMethods.fieldNames();
      jObjMethods.fieldNames();
      methodURLs.forEach(methodURL -> {
        Object val = jObjMethods.getValue(methodURL);
        if (val instanceof JsonArray) {
          ((JsonArray) val).forEach(entry -> {
            String pathRegex = ((JsonObject) entry).getString("regex2method");
            ((JsonObject) entry).put(AnnotationGrabber.CLASS_NAME, jObjMethods.getString(AnnotationGrabber.CLASS_NAME));
            ((JsonObject) entry).put(AnnotationGrabber.INTERFACE_NAME, jObjMethods.getString(AnnotationGrabber.INTERFACE_NAME));
            mappedURLs.addPath(pathRegex, (JsonObject) entry);
          });
        }
      });
    });
    return mappedURLs;
  }

  @Override
  public void stop(Promise<Void> stopPromise) throws Exception {
    super.stop();
    // removes the .lck file associated with the log file
    LogUtil.closeLogger();
    runShutdownHook(v -> {
      if (v.succeeded()) {
        stopPromise.complete();
      } else {
        stopPromise.fail("shutdown hook failed....");
      }
    });
  }

  /**
   * ONLY 1 Impl is allowed currently!
   * implementors of the InitAPI interface must call back the handler in there init() implementation like this:
   * resultHandler.handle(io.vertx.core.Future.succeededFuture(true)); or this will hang
   */
  private void runHook(Handler<AsyncResult<Boolean>> resultHandler) throws Exception {
    try {
      ArrayList<Class<?>> aClass = InterfaceToImpl.convert2Impl(RTFConsts.PACKAGE_OF_IMPLEMENTATIONS, RTFConsts.PACKAGE_OF_HOOK_INTERFACES + ".InitAPI", false);
      for (int i = 0; i < aClass.size(); i++) {
        Class<?>[] paramArray = new Class[] { Vertx.class, Context.class, Handler.class };
        Method method = aClass.get(i).getMethod("init", paramArray);
        method.invoke(aClass.get(i).newInstance(), vertx, context, resultHandler);
        LogUtil.formatLogMessage(getClass().getName(), "runHook",
          "One time hook called with implemented class " + "named " + aClass.get(i).getName());
      }
    } catch (ClassNotFoundException e) {
      // no hook implemented, this is fine, just startup normally then
      resultHandler.handle(io.vertx.core.Future.succeededFuture(true));
    }
  }

  /**
   * multiple impl allowed
   * @throws Exception
   */
  private void runPeriodicHook() throws Exception {
    try {
      ArrayList<Class<?>> aClass = InterfaceToImpl.convert2Impl(RTFConsts.PACKAGE_OF_IMPLEMENTATIONS, RTFConsts.PACKAGE_OF_HOOK_INTERFACES + ".PeriodicAPI", true);
      for (int i = 0; i < aClass.size(); i++) {
        Class<?>[] paramArray = new Class[] {};
        Method method = aClass.get(i).getMethod("runEvery", paramArray);
        Object delay = method.invoke(aClass.get(i).newInstance());
        LogUtil.formatLogMessage(getClass().getName(), "runPeriodicHook",
          "Periodic hook called with implemented class " + "named " + aClass.get(i).getName());
        final int j = i;
        vertx.setPeriodic(((Long) delay).longValue(), new Handler<Long>() {
          @Override
          public void handle(Long aLong) {
            try {
              Class<?>[] paramArray1 = new Class[] { Vertx.class, Context.class };
              Method method1 = aClass.get(j).getMethod("run", paramArray1);
              method1.invoke(aClass.get(j).newInstance(), vertx, context);
            } catch (Exception e) {
              log.error(e.getMessage(), e);
            }
          }
        });
      }
    } catch (ClassNotFoundException e) {
      // no hook implemented, this is fine, just startup normally then
      LogUtil.formatLogMessage(getClass().getName(), "runPeriodicHook", "no periodic implementation found, continuing with deployment");
    }
  }

  /**
   * ONE impl allowed
   * @throws Exception
   */
  private void runPostDeployHook(Handler<AsyncResult<Boolean>> resultHandler) throws Exception {
    try {
      ArrayList<Class<?>> aClass = InterfaceToImpl.convert2Impl(RTFConsts.PACKAGE_OF_IMPLEMENTATIONS, RTFConsts.PACKAGE_OF_HOOK_INTERFACES + ".PostDeployVerticle", true);
      for (int i = 0; i < aClass.size(); i++) {
        Class<?>[] paramArray = new Class[] { Vertx.class, Context.class, Handler.class };
        Method method = aClass.get(i).getMethod("init", paramArray);
        method.invoke(aClass.get(i).newInstance(), vertx, context, resultHandler);
        LogUtil.formatLogMessage(getClass().getName(), "runHook",
          "Post Deploy Hook called with implemented class " + "named " + aClass.get(i).getName());
      }
    } catch (ClassNotFoundException e) {
      // no hook implemented, this is fine, just startup normally then
      LogUtil.formatLogMessage(getClass().getName(), "runPostDeployHook", "no Post Deploy Hook implementation found, continuing with deployment");
    }
  }

  /**
   * only one impl allowed
   * @param resultHandler
   * @throws Exception
   */
  private void runShutdownHook(Handler<AsyncResult<Void>> resultHandler) throws Exception {
    try {
      ArrayList<Class<?>> aClass = InterfaceToImpl.convert2Impl(RTFConsts.PACKAGE_OF_IMPLEMENTATIONS, RTFConsts.PACKAGE_OF_HOOK_INTERFACES + ".ShutdownAPI", false);
      for (int i = 0; i < aClass.size(); i++) {
        Class<?>[] paramArray = new Class[] { Vertx.class, Context.class, Handler.class };
        Method method = aClass.get(i).getMethod("shutdown", paramArray);
        method.invoke(aClass.get(i).newInstance(), vertx, context, resultHandler);
        LogUtil.formatLogMessage(getClass().getName(), "runShutdownHook",
          "shutdown hook called with implemented class " + "named " + aClass.get(i).getName());
      }
    } catch (ClassNotFoundException e) {
      // no hook implemented, this is fine, just startup normally then
      LogUtil.formatLogMessage(getClass().getName(), "runShutdownHook", "no shutdown hook implementation found, continuing with shutdown");
      resultHandler.handle(io.vertx.core.Future.succeededFuture());
    }
  }

  private void cmdProcessing() throws IOException {
    String importDataPath = null;
    // TODO need to add a normal command line parser
    List<String> cmdParams = processArgs();

    if (cmdParams != null) {
      for (String param : cmdParams) {

        if (param.startsWith("debug_log_package=")) {
          String debugPackage = param.split("=")[1];
          if(debugPackage != null && debugPackage.length() > 0){
            LogUtil.formatLogMessage(className, "cmdProcessing", "Setting package " + debugPackage + " to debug");
            LogUtil.updateLogConfiguration(debugPackage, "FINE");
          }
        }
        else if (param.startsWith("db_connection=")) {
          String dbconnection = param.split("=")[1];
          PostgresClient.setConfigFilePath(dbconnection);
          PostgresClient.setIsEmbedded(false);
          LogUtil.formatLogMessage(className, "cmdProcessing", "Setting path to db config file....  " + dbconnection);
        }
        else if (param.startsWith("embed_postgres=true")) {
          // allow setting config() from unit test mode which runs embedded

          LogUtil.formatLogMessage(className, "cmdProcessing", "Using embedded postgres... starting... ");

          // this blocks
          PostgresClient.setIsEmbedded(true);
          PostgresClient.setConfigFilePath(null);
        }
        else if (param != null && param.startsWith("postgres_import_path=")) {
          try {
            importDataPath = param.split("=")[1];
            System.out.println("Setting path to import DB file....  " + importDataPath);
          } catch (Exception e) {
            // any problems - print exception and continue
            log.error(e.getMessage(), e);
          }
        }
        else{
          //assume module specific cmd line args with '=' separator
          String []arg = param.split("=");
          if(arg.length == 2){
            MODULE_SPECIFIC_ARGS.put(arg[0], arg[1]);
            log.info("module specific argument added: " + arg[0] + " with value " + arg[1]);
          }
          else{
            log.warn("The following cmd line parameter was skipped, " + param + ". Expected format key=value\nIf this is a "
                + "JVM argument, pass it before the jar, not after");
          }
        }
      }

      if (PostgresClient.isEmbedded() || importDataPath != null) {
        PostgresClient.getInstance(vertx).startEmbeddedPostgres();
      }

      if (importDataPath != null) {
        // blocks as well for now
        System.out.println("Import DB file....  " + importDataPath);
        PostgresClient.getInstance(vertx).importFileEmbedded(importDataPath);
      }
    }
  }

  /**
   * look for the boundary and return just the multipart/form-data multipart/form-data boundary=----WebKitFormBoundaryP8wZiNAoFszXOXEt if
   * boundary doesnt exist that return original string
   */
  private String removeBoundry(String contenttype) {
    int idx = contenttype.indexOf("boundary");
    if (idx != -1) {
      return contenttype.substring(0, idx - 1);
    }
    return contenttype;
  }

  /**
   * check accept and content-type headers if no - set the request asa not valid and return error to user
   */
  private void checkAcceptContentType(JsonArray produces, JsonArray consumes, RoutingContext rc, boolean[] validRequest) {
    /*
     * NOTE that the content type and accept headers will accept a partial match - for example: if the raml indicates a text/plain and an
     * application/json content-type and only one is passed - it will accept it
     */
    // check allowed content types in the raml for this resource + method
    HttpServerRequest request = rc.request();
    if (consumes != null && validRequest[0]) {
      // get the content type passed in the request
      // if this was left out by the client they must add for request to return
      // clean up simple stuff from the clients header - trim the string and remove ';' in case
      // it was put there as a suffix
      String contentType = StringUtils.defaultString(request.getHeader("Content-type"), DEFAULT_CONTENT_TYPE)
          .replaceFirst(";.*", "").trim();
      if (!consumes.contains(removeBoundry(contentType))) {
        endRequestWithError(rc, 400, true, messages.getMessage("en", MessageConsts.ContentTypeError, consumes, contentType),
          validRequest);
      }
    }

    // type of data expected to be returned by the server
    if (produces != null && validRequest[0]) {
      String accept = StringUtils.defaultString(request.getHeader("Accept"), "*/*");
      if (acceptCheck(produces, accept) == null) {
        // use contains because multiple values may be passed here
        // for example json/application; text/plain mismatch of content type found
        endRequestWithError(rc, 400, true, messages.getMessage("en", MessageConsts.AcceptHeaderError, produces, accept),
          validRequest);
      }
    }
  }

  private void parseParams(RoutingContext rc, Buffer body, Iterator<Map.Entry<String, Object>> paramList, boolean[] validRequest, JsonArray consumes,
      Object[] paramArray, String[] pathParams, Map<String, String> okapiHeaders) {

    HttpServerRequest request = rc.request();
    MultiMap queryParams = request.params();
    int []pathParamsIndex = new int[] { 0 };

    paramList.forEachRemaining(entry -> {
      if (validRequest[0]) {
        String valueName = ((JsonObject) entry.getValue()).getString("value");
        String valueType = ((JsonObject) entry.getValue()).getString("type");
        String paramType = ((JsonObject) entry.getValue()).getString("param_type");
        int order = ((JsonObject) entry.getValue()).getInteger("order");
        Object defaultVal = ((JsonObject) entry.getValue()).getValue("default_value");

        boolean emptyNumericParam = false;
        // validation of query params (other then enums), object in body (not including drools),
        // and some header params validated by jsr311 (aspects) - the rest are handled in the code here
        // handle un-annotated parameters - this is assumed to be
        // entities in HTTP BODY for post and put requests or the 3 injected params
        // (okapi headers, vertx context and vertx handler) - file uploads are also not annotated but are not handled here due
        // to their async upload - so explicitly skip them
        if (AnnotationGrabber.NON_ANNOTATED_PARAM.equals(paramType) && !FILE_UPLOAD_PARAM.equals(valueType)) {
          try {
            // this will also validate the json against the pojo created from the schema
            Class<?> entityClazz = Class.forName(valueType);

            if (!valueType.equals("io.vertx.core.Handler") && !valueType.equals("io.vertx.core.Context") &&
                !valueType.equals("java.util.Map") && !valueType.equals("java.io.InputStream") && !valueType.equals("io.vertx.ext.web.RoutingContext")) {
              // we have special handling for the Result Handler and context, it is also assumed that
              //an inputsteam parameter occurs when application/octet is declared in the raml
              //in which case the content will be streamed to he function
              String bodyContent = body == null ? null : body.toString();
              withRequestId(rc, () -> log.debug(rc.request().path() + " -------- bodyContent -------- " + bodyContent));
              if(bodyContent != null){
                if("java.io.Reader".equals(valueType)){
                  paramArray[order] = new StringReader(bodyContent);
                }
                else if ("java.lang.String".equals(valueType)) {
                  paramArray[order] = bodyContent;
                }
                else if(bodyContent.length() > 0) {
                  try {
                    paramArray[order] = MAPPER.readValue(bodyContent, entityClazz);
                  } catch (UnrecognizedPropertyException e) {
                    withRequestId(rc, () -> log.error(e.getMessage(), e));
                    endRequestWithError(rc, RTFConsts.VALIDATION_ERROR_HTTP_CODE, true, JsonUtils.entity2String(
                      ValidationHelper.createValidationErrorMessage("", "", e.getMessage())) , validRequest);
                    return;
                  }
                }
              }

              Errors errorResp = new Errors();

              //is this request only to validate a field value and not an actual
              //request for additional processing
              List<String> field2validate = request.params().getAll("validate_field");
              Object[] resp = isValidRequest(rc, paramArray[order], errorResp, validRequest, field2validate, entityClazz);
              boolean isValid = (boolean) resp[0];
              paramArray[order] = resp[1];

              if (!isValid) {
                endRequestWithError(rc, RTFConsts.VALIDATION_ERROR_HTTP_CODE, true, JsonUtils.entity2String(errorResp), validRequest);
                return;
              }
              if (!field2validate.isEmpty()) {
                //valid request for the field to validate request made
                AsyncResponseResult arr = new AsyncResponseResult();
                ResponseImpl ri = new ResponseImpl();
                ri.setStatus(200);
                arr.setResult(ri);
                //right now this is the only flag available to stop
                //any additional respones for this request. to fix
                validRequest[0] = false;
                sendResponse(rc, arr, 0, null);
                return;
              }
              MetadataUtil.populateMetadata(paramArray[order], okapiHeaders);
            }
          } catch (Exception e) {
            withRequestId(rc, () -> log.error(e.getMessage(), e));
            endRequestWithError(rc, 400, true, "Json content error " + e.getMessage(), validRequest);

          }
        } else if (AnnotationGrabber.HEADER_PARAM.equals(paramType)) {
          // handle header params - read the header field from the
          // header (valueName) and get its value
          String value = request.getHeader(valueName);
          // set the value passed from the header as a param to the function
          paramArray[order] = value;
        } else if (AnnotationGrabber.PATH_PARAM.equals(paramType)) {
          // these are placeholder values in the path - for example
          // /patrons/{patronid} - this would be the patronid value
          paramArray[order] = pathParams[pathParamsIndex[0]++];
        } else if (AnnotationGrabber.QUERY_PARAM.equals(paramType)) {
          String param = queryParams.get(valueName);
          // support date, enum, numbers or strings as query parameters
          try {
            if (valueType.equals("java.lang.String")) {
              // regular string param in query string - just push value
              if (param == null && defaultVal != null) {
                // no value passed - check if there is a default value
                paramArray[order] = defaultVal;
              } else {
                paramArray[order] = param;
              }
            } else if (valueType.equals("java.util.Date")) {
              // regular string param in query string - just push value
              if (param == null) {
                // no value passed - check if there is a default value
                paramArray[order] = defaultVal;
              } else {
                paramArray[order] = DateUtils.parseDate(param, DATE_PATTERNS);
              }
            } else if (valueType.equals("int") || valueType.equals("java.lang.Integer")) {
              // cant pass null to an int type
              if (param == null) {
                if (defaultVal != null) {
                  paramArray[order] = Integer.valueOf((String) defaultVal);
                } else {
                  paramArray[order] = valueType.equals("int") ? 0 : null;
                }
              }
              else if("".equals(param)) {
                emptyNumericParam = true;
              }
              else {
                paramArray[order] = Integer.valueOf(param);
              }
            } else if (valueType.equals("boolean") || valueType.equals("java.lang.Boolean")) {
              if (param == null) {
                if (defaultVal != null) {
                  paramArray[order] = Boolean.valueOf((String)defaultVal);
                }
              } else {
                paramArray[order] = Boolean.valueOf(param);
              }
            } else if (valueType.contains("List")) {
              List<String> vals = queryParams.getAll(valueName);
              if (vals == null) {
                paramArray[order] = null;
              }
              else {
                paramArray[order] = vals;
              }
            } else if (valueType.equals("java.math.BigDecimal") || valueType.equals("java.lang.Number")) {
              if (param == null) {
                if (defaultVal != null) {
                  paramArray[order] = new BigDecimal((String) defaultVal);
                } else {
                  paramArray[order] = null;
                }
              }
              else if ("".equals(param)) {
                emptyNumericParam = true;
              }
              else {
                paramArray[order] = new BigDecimal(param.replaceAll(",", "")); // big decimal can contain ","
              }
            } else { // enum object type
              try {
                paramArray[order] = parseEnum(valueType, param, defaultVal);
              } catch (Exception ee) {
                withRequestId(rc, () -> log.error(ee.getMessage(), ee));
                endRequestWithError(rc, 400, true, ee.getMessage(), validRequest);
              }
            }
            if(emptyNumericParam){
              endRequestWithError(rc, 400, true, valueName + " does not have a default value in the RAML and has been passed empty",
                validRequest);
            }
          } catch (Exception e) {
            withRequestId(rc, () -> log.error(e.getMessage(), e));
            endRequestWithError(rc, 400, true, e.getMessage(), validRequest);
          }
        }
      }
    });
  }

  /**
   * @return the enum value of type valueType where value.name equals param (fall-back: equals defaultValue).
   *         Return null if the type neither has param nor defaultValue.
   * @throws ClassNotFoundException if valueType does not exist
   */
  @SuppressWarnings({
    "squid:S1523",  // Suppress warning "Make sure that this dynamic injection or execution of code is safe."
                    // This is safe because we accept an enum class only, and do not invoke any method.
    "squid:S3011"}) // Suppress "Make sure that this accessibility update is safe here."
                    // This is safe because we only read the field and it is a field of an enum.
  static Object parseEnum(String valueType, String param, Object defaultValue)
      throws ReflectiveOperationException {

    Class<?> enumClass = Class.forName(valueType);
    if (! enumClass.isEnum()) {
      return null;
    }
    String defaultString = null;
    if (defaultValue != null) {
      defaultString = defaultValue.toString();
    }
    Object defaultEnum = null;
    for (Object anEnum : enumClass.getEnumConstants()) {
      Field nameField = anEnum.getClass().getDeclaredField("name");
      nameField.setAccessible(true);  // access to private field
      String enumName = nameField.get(anEnum).toString();
      if (enumName.equals(param)) {
        return anEnum;
      }
      if (enumName.equals(defaultString)) {
        defaultEnum = anEnum;
        if (param == null) {
          return defaultEnum;
        }
      }
    }
    return defaultEnum;
  }

  /**
   * return whether the request is valid [0] and a cleaned up version of the object [1]
   * @param rc
   * @param content
   * @param errorResp
   * @param validRequest
   * @param singleField
   * @param entityClazz
   * @return
   */
  private Object[] isValidRequest(RoutingContext rc, Object content, Errors errorResp, boolean[] validRequest, List<String> singleField, Class<?> entityClazz) {
    Set<? extends ConstraintViolation<?>> validationErrors = validationFactory.getValidator().validate(content);
    boolean ret = true;
    if (validationErrors.size() > 0) {
      //StringBuffer sb = new StringBuffer();

      for (ConstraintViolation<?> cv : validationErrors) {

        if("must be null".equals(cv.getMessage())){
          /**
           * read only fields are marked with a 'must be null' annotation @null
           * so the client should not pass them in, if they were passed in, remove them here
           * so that they do not reach the implementing function
           */
          try {
            if(!(content instanceof JsonObject)){
              content = JsonObject.mapFrom(content);
            }
            ((JsonObject)content).remove(cv.getPropertyPath().toString());
            continue;
          } catch (Exception e) {
            withRequestId(rc, () -> log.warn("Failed to remove " + cv.getPropertyPath().toString()
                + " field from body when calling " + rc.request().absoluteURI(), e));
          }
        }
        Error error = new Error();
        Parameter p = new Parameter();
        String field = cv.getPropertyPath().toString();
        p.setKey(field);
        Object val = cv.getInvalidValue();
        if(val == null){
          p.setValue("null");
        }
        else{
          p.setValue(val.toString());

        }
        error.getParameters().add(p);
        error.setMessage(cv.getMessage());
        error.setCode("-1");
        error.setType(RTFConsts.VALIDATION_FIELD_ERROR);
        //return the error if the validation is requested on a specific field
        //and that field fails validation. if another field fails validation
        //that is ok as validation on that specific field wasnt requested
        //or there are validation errors and this is not a per field validation request
        if (singleField != null && (singleField.contains(field) || singleField.isEmpty())) {
          errorResp.getErrors().add(error);
          ret = false;
        }
        //sb.append("\n" + cv.getPropertyPath() + "  " + cv.getMessage() + ",");
      }
      if(content instanceof JsonObject){
        //we have sanitized the passed in object by removing read-only fields
        try {
          content = MAPPER.readValue(((JsonObject)content).encode(), entityClazz);
        } catch (IOException e) {
          withRequestId(rc, () -> log.error(
              "Failed to serialize body content after removing read-only fields when calling "
                  + rc.request().absoluteURI(), e));
        }
      }
    }

    return new Object[]{Boolean.valueOf(ret), content};
  }

  /**
   * Run logCommand with request id. Take request id value from headers in routingContext
   * and temporarily store it as reqId in ThreadContext. Run logCommand without reqId
   * if request id value is <code>null</code>.
   */
  private static void withRequestId(RoutingContext routingContext, Runnable logCommand) {
    if (routingContext == null) {
      logCommand.run();
      return;
    }
    String requestId = routingContext.request().headers().get(RestVerticle.OKAPI_REQUESTID_HEADER);
    if (requestId == null) {
      logCommand.run();
      return;
    }
    ThreadContext.put("reqId", "reqId=" + requestId);
    logCommand.run();
    // Multiple vertx async requests use the same thread therefore we must delete reqId afterwards.
    // For a better implementation see
    // https://issues.folio.org/browse/RMB-669 "Add default metrics to RMB: incoming API calls"
    ThreadContext.remove("reqId");
  }

  class StreamStatus {

    private int status = -1;

    public int getStatus() {
      return status;
    }
    public void setStatus(int status) {
      this.status = status;
    }
  }

  public static String getDeploymentId(){
    return deploymentId;
  }
}
