package org.folio.rest;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerFileUpload;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.dropwizard.MetricsService;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.StaticHandler;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.lang.annotation.Annotation;
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
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.mail.MessagingException;
import javax.mail.internet.InternetHeaders;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.ValidatorFactory;
import javax.ws.rs.core.Response;

import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.commons.lang3.StringUtils;
import org.folio.rest.annotations.Stream;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.AnnotationGrabber;
import org.folio.rest.tools.ClientGenerator;
import org.folio.rest.tools.RTFConsts;
import org.folio.rest.tools.codecs.PojoEventBusCodec;
import org.folio.rest.tools.messages.MessageConsts;
import org.folio.rest.tools.messages.Messages;
import org.folio.rest.tools.utils.BinaryOutStream;
import org.folio.rest.tools.utils.InterfaceToImpl;
import org.folio.rest.tools.utils.LogUtil;
import org.folio.rest.tools.utils.OutStream;
import org.folio.rulez.Rules;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.FactHandle;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.io.ByteStreams;

public class RestVerticle extends AbstractVerticle {

  public static final String        DEFAULT_UPLOAD_BUS_ADDRS        = "admin.uploaded.files";
  public static final String        DEFAULT_TEMP_DIR                = System.getProperty("java.io.tmpdir");
  public static final String        JSON_URL_MAPPINGS               = "API_PATH_MAPPINGS";
  public static final String        OKAPI_HEADER_TENANT             = ClientGenerator.OKAPI_HEADER_TENANT;
  public static final String        STREAM_ID                       =  "STREAMED_ID";
  public static final String        STREAM_COMPLETE                 =  "COMPLETE";
  public static final HashMap<String, String> MODULE_SPECIFIC_ARGS  = new HashMap<>();

  private static final String       UPLOAD_PATH_TO_HANDLE           = "/admin/upload";
  private static final String       CORS_ALLOW_HEADER               = "Access-Control-Allow-Origin";
  private static final String       CORS_ALLOW_ORIGIN               = "Access-Control-Allow-Headers";
  private static final String       CORS_ALLOW_HEADER_VALUE         = "*";
  private static final String       CORS_ALLOW_ORIGIN_VALUE         = "Origin, Authorization, X-Requested-With, Content-Type, Accept";
  private static final String       SUPPORTED_CONTENT_TYPE_FORMDATA = "multipart/form-data";
  private static final String       SUPPORTED_CONTENT_TYPE_STREAMIN = "application/octet-stream";
  private static final String       SUPPORTED_CONTENT_TYPE_JSON_DEF = "application/json";
  private static final String       SUPPORTED_CONTENT_TYPE_TEXT_DEF = "text/plain";
  private static final String       SUPPORTED_CONTENT_TYPE_XML_DEF  = "application/xml";
  private static final String       FILE_UPLOAD_PARAM               = "javax.mail.internet.MimeMultipart";
  private static MetricsService     serverMetrics                   = null;
  private static ValidatorFactory   validationFactory;
  private static KieSession         droolsSession;
  private static String             className                       = RestVerticle.class.getName();
  private static final Logger       log                             = LoggerFactory.getLogger(className);
  private static final ObjectMapper MAPPER                          = new ObjectMapper();
  private static final String       OKAPI_HEADER_PREFIX             = "x-okapi";
  private static final String       DEFAULT_SCHEMA                  = "public";

  private final Messages            messages                        = Messages.getInstance();
  private int                       port                            = -1;

  private EventBus eventBus;

  // this is only to run via IDE - otherwise see pom which runs the verticle and
  // requires passing -cluster and preferable -cluster-home args
  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(new RestVerticle());
  }

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
  public void start(Future<Void> startFuture) throws Exception {

    readInGitProps();

    //process cmd line arguments
    cmdProcessing();

    LogUtil.formatLogMessage(className, "start", "metrics enabled: " + vertx.isMetricsEnabled());

    serverMetrics = MetricsService.create(vertx);

    // maps paths found in raml to the generated functions to route to when the paths are requested
    MappedClasses mappedURLs = populateConfig();

    // set of exposed urls as declared in the raml
    Set<String> urlPaths = mappedURLs.getAvailURLs();

    // create a map of regular expression to url path
    Map<String, Pattern> regex2Pattern = mappedURLs.buildURLRegex();

    // Create a router object.
    Router router = Router.router(vertx);

    eventBus = vertx.eventBus();

    log.info(context.getInstanceCount() + " verticles deployed ");
    try {
      //register codec to be able to pass pojos on the event bus
      eventBus.registerCodec(new PojoEventBusCodec());
    } catch (Exception e3) {
      if(context.getInstanceCount() != 1){
        //needed in case we run multiple verticle instances
        //in this vertx instace - re-registering the same codec twice throws an
        //exception
        log.info("Attempt to register PojoEventBusCodec again... this is acceptable ");
      }
      else{
        throw e3;
      }
    }

    // needed so that we get the body content of the request - note that this
    // will read the entire body into memory
    final BodyHandler handler = BodyHandler.create();

    // IMPORTANT!!!
    // the body of the request will be read into memory for ALL PUT requests
    // and for POST requests with the content-types below ONLY!!!
    // multipart, for example will not be read by the body handler as vertx saves
    // multiparts and www-encoded to disk - hence multiparts will be handled differently
    // see uploadHandler further down
    router.put().handler(handler);
    router.post().consumes(SUPPORTED_CONTENT_TYPE_JSON_DEF).handler(handler);
    router.post().consumes(SUPPORTED_CONTENT_TYPE_TEXT_DEF).handler(handler);
    router.post().consumes(SUPPORTED_CONTENT_TYPE_XML_DEF).handler(handler);

    // run pluggable startup code in a class implementing the InitAPI interface
    // in the "org.folio.rest.impl" package
    runHook(vv -> {
      if (((Future<?>) vv).failed()) {
        String reason = ((Future<?>) vv).cause().getMessage();
        log.error( messages.getMessage("en", MessageConsts.InitializeVerticleFail, reason));
        startFuture.fail(reason);
        vertx.close();
        System.exit(-1);
      } else {
        log.info("init succeeded.......");
        try {
          // startup periodic impl if exists
          runPeriodicHook();
        } catch (Exception e2) {
          log.error(e2);
        }
        //single handler for all url calls other then documentation
        //which is handled separately
        router.routeWithRegex("^(?!.*apidocs).*$").handler(rc -> {
          long start = System.nanoTime();
          try {
            //list of regex urls created from urls declared in the raml
            Iterator<String> iter = urlPaths.iterator();
            boolean validPath = false;
            boolean[] validRequest = { true };
            // loop over regex patterns and try to match them against the requested
            // URL if no match is found, then the requested url is not supported by
            // the ramls and we return an error - this has positive security implications as well
            while (iter.hasNext()) {
              String regexURL = iter.next();
              //try to match the requested url to each regex pattern created from the urls in the raml
              Matcher m = regex2Pattern.get(regexURL).matcher(rc.request().path());
              if (m.find()) {
                validPath = true;
                // get the function that should be invoked for the requested
                // path + requested http_method pair
                JsonObject ret = mappedURLs.getMethodbyPath(regexURL, rc.request().method().toString());
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
                }
                Class<?> aClass;
                try {
                  if (validRequest[0]) {
                    int groups = m.groupCount();
                    //pathParams are the place holders in the raml query string
                    //for example /admin/{admin_id}/yyy/{yyy_id} - the content in between the {} are path params
                    //they are replaced with actual values and are passed to the function which the url is mapped to
                    String[] pathParams = new String[groups];
                    for (int i = 0; i < groups; i++) {
                      pathParams[i] = m.group(i + 1);
                    }

                    //create okapi headers map and inject into function
                    Map<String, String> okapiHeaders = new CaseInsensitiveMap<>();
                    String []tenantId = new String[]{null};
                    getOkapiHeaders(rc, okapiHeaders, tenantId);

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

                    // create the array and then populate it by parsing the url parameters which are needed to invoke the function mapped
                    //to the requested URL - array will be populated by parseParams() function
                    Iterator<Map.Entry<String, Object>> paramList = params.iterator();
                    Object[] paramArray = new Object[params.size()];
                    parseParams(rc, paramList, validRequest, consumes, paramArray, pathParams);

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

                    if (validRequest[0]) {

                      // check if we are dealing with a file upload , currently only multipart/form-data and application/octet
                      //in the raml definition for such a function
                      final boolean[] isContentUpload = new boolean[] { false };
                      final int[] uploadParamPosition = new int[] { -1 };
                      params.forEach(param -> {
                        String cType = request.getHeader("Content-type");
                        if (((JsonObject) param.getValue()).getString("type").equals(FILE_UPLOAD_PARAM)) {
                          isContentUpload[0] = true;
                          uploadParamPosition[0] = ((JsonObject) param.getValue()).getInteger("order");
                        }
                        else if(((JsonObject) param.getValue()).getString("type").equals("java.io.InputStream")){
                          //application/octet-stream passed - this is handled in a stream like manner
                          //and the corresponding function called must annotate with a @Stream - and be able
                          //to handle the function being called repeatedly on parts of the data
                          uploadParamPosition[0] = ((JsonObject) param.getValue()).getInteger("order");
                          isContentUpload[0] = true;
                        }
                      });

                      //
                      // file upload requested (multipart/form-data) but the url is not to the /admin/upload
                      // meaning, an implementing module is using its own upload handling, so read the content and
                      // pass to implementing function just like any other call
                      if (isContentUpload[0] && !streamData) {

                        //if file upload - set needed handlers
                        // looks something like -> multipart/form-data; boundary=----WebKitFormBoundaryzeZR8KqAYJyI2jPL
                        if (consumes != null && consumes.contains(SUPPORTED_CONTENT_TYPE_FORMDATA)) {
                          //multipart
                          handleMultipartUpload(rc, request, uploadParamPosition, paramArray, validRequest);
                          request.endHandler( a -> {
                            if (validRequest[0]) {
                              //if request is valid - invoke it
                              try {
                                invoke(method2Run[0], paramArray, instance, rc, tenantId, okapiHeaders, new StreamStatus(), v -> {
                                  LogUtil.formatLogMessage(className, "start", " invoking " + function);
                                  sendResponse(rc, v, start, tenantId[0]);
                                });
                              } catch (Exception e1) {
                                log.error(e1.getMessage(), e1);
                                rc.response().end();
                              }
                            }
                          });
                        }

                        else {
                          //assume input stream
                          handleInputStreamUpload(method2Run[0], rc, request, instance, tenantId, okapiHeaders,
                            uploadParamPosition, paramArray, validRequest, start);
                        }
                      }
                      else if(streamData){

                        handleStream(method2Run[0], rc, request, instance, tenantId, okapiHeaders,
                          uploadParamPosition, paramArray, validRequest, start);

                      }
                      else{
                        if (validRequest[0]) {
                          //if request is valid - invoke it
                          try {
                            invoke(method2Run[0], paramArray, instance, rc,  tenantId, okapiHeaders, new StreamStatus(), v -> {
                              LogUtil.formatLogMessage(className, "start", " invoking " + function);
                              sendResponse(rc, v, start, tenantId[0]);
                            });
                          } catch (Exception e1) {
                            log.error(e1.getMessage(), e1);
                            rc.response().end();
                          }
                        }
                      }
                    }
                    else{
                      endRequestWithError(rc, 400, true, messages.getMessage("en", MessageConsts.UnableToProcessRequest),
                        validRequest);
                      return;
                    }
                  }
                } catch (Exception e) {
                  log.error(e.getMessage(), e);
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
              log.error(e.getMessage(), e);
            }
        } );
        // routes requests on “/assets/*” to resources stored in the “assets”
        // directory.
        router.route("/assets/*").handler(StaticHandler.create("assets"));

        // In the following example all requests to paths starting with
        // /apidocs/ will get served from the directory resources/apidocs:
        // example:
        // http://localhost:8181/apidocs/index.html?raml=raml/_patrons.raml
        router.route("/apidocs/*").handler(StaticHandler.create("apidocs"));
        // startup http server on port 8181 to serve documentation
        if (port == -1) {
          // we are here if port was not passed via cmd line
          port = config().getInteger("http.port", 8081);
        }

        // in anycase set the port so it is available to others via the config()
        config().put("http.port", port);

        Integer p = port;

        //if client includes an Accept-Encoding header which includes
        //the supported compressions - deflate or gzip.
        HttpServerOptions serverOptions = new HttpServerOptions();
        serverOptions.setCompressionSupported(false);

        HttpServer server = vertx.createHttpServer(serverOptions);
        server.requestHandler(router::accept)
        // router object (declared in the beginning of the atrt function accepts request and will pass to next handler for
        // specified path

        .listen(p,
          // Retrieve the port from the configuration file - file needs to
          // be passed as arg to command line,
          // for example: -conf src/main/conf/my-application-conf.json
          // default to 8181.
          result -> {
            if (result.failed()) {
              startFuture.fail(result.cause());
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
              LogUtil.formatLogMessage(className, "start", "http server for apis and docs started on port " + p + ".");
              LogUtil.formatLogMessage(className, "start", "Documentation available at: " + "http://localhost:" + Integer.toString(p)
                + "/apidocs/");
              startFuture.complete();
            }
          });
      }
    });
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
            LogUtil.formatLogMessage(className, "start", " invoking " + method2Run);
          });
        } catch (Exception e1) {
          log.error(e1.getMessage(), e1);
          rc.response().end();
        }
      }
    });
    request.endHandler( e -> {

      StreamStatus stat = new StreamStatus();
      stat.setStatus(1);
      invoke(method2Run, paramArray, instance, rc,  tenantId, okapiHeaders, stat, v -> {
        LogUtil.formatLogMessage(className, "start", " invoking " + method2Run);
        //all data has been stored in memory - not necessarily all processed
        sendResponse(rc, v, start, tenantId[0]);
      });

    });
    request.exceptionHandler(new Handler<Throwable>(){
      @Override
      public void handle(Throwable event) {
        endRequestWithError(rc, 400, true, "unable to upload file " + event.getMessage(), validRequest);
      }});
  }
  /**
   * @param method2Run
   * @param rc
   * @param request
   * @param okapiHeaders
   * @param tenantId
   * @param instance
   * @param uploadParamPosition
   * @param paramArray
   * @param validRequest
   * @param start
   */
  private void handleInputStreamUpload(Method method2Run, RoutingContext rc, HttpServerRequest request,
      Object instance, String[] tenantId, Map<String, String> okapiHeaders,
      int[] uploadParamPosition, Object[] paramArray, boolean[] validRequest, long start) {

    final Buffer content = Buffer.buffer();

    request.handler(new Handler<Buffer>() {
      @Override
      public void handle(Buffer buff) {
        content.appendBuffer(buff);
      }
    });
    request.endHandler( e -> {
      paramArray[uploadParamPosition[0]] = new ByteArrayInputStream(content.getBytes());
      try {
        invoke(method2Run, paramArray, instance, rc, tenantId, okapiHeaders, new StreamStatus(), v -> {
          LogUtil.formatLogMessage(className, "start", " invoking " + method2Run);
          sendResponse(rc, v, start, tenantId[0]);
        });
      } catch (Exception e1) {
        log.error(e1.getMessage(), e1);
        rc.response().end();
      }
    });

    request.exceptionHandler(new Handler<Throwable>(){
      @Override
      public void handle(Throwable event) {
        endRequestWithError(rc, 400, true, event.getMessage(), validRequest);
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
   * @param request
   * @param uploadParamPosition
   * @param paramArray
   * @param validRequest
   */
  private void handleMultipartUpload(RoutingContext rc,
      HttpServerRequest request, int[] uploadParamPosition, Object[] paramArray, boolean[] validRequest) {
    request.setExpectMultipart(true);
    MimeMultipart mmp = new MimeMultipart();
    //place the mmp as an argument to the 'to be called' function - at the correct position
    paramArray[uploadParamPosition[0]] = mmp;
    request.uploadHandler(new MultiPartHandler(rc, mmp, validRequest));
  }

  class MultiPartHandler implements Handler<io.vertx.core.http.HttpServerFileUpload> {

    MimeMultipart mmp;
    RoutingContext rc;
    boolean[] validRequest;
    Buffer content = Buffer.buffer();

    public MultiPartHandler(RoutingContext rc, MimeMultipart mmp, boolean[] validRequest){
      this.rc = rc;
      this.mmp = mmp;
      this.validRequest = validRequest;
    }

    @Override
    public void handle(HttpServerFileUpload upload) {
      upload.handler(new Handler<Buffer>() {
        @Override
        public void handle(Buffer buff) { /** called as data comes in */
          if(content == null){
            content = Buffer.buffer();
          }
          content.appendBuffer(buff);
        }
      });
      upload.exceptionHandler(new Handler<Throwable>() {
        @Override
        public void handle(Throwable event) {
          endRequestWithError(rc, 400, true, "unable to upload file " + event.getMessage(), validRequest);
        }
      });
      /** endHandler called for each part in the multipart, so if uploading 2 files - will be called twice */
      upload.endHandler(new Handler<Void>() {
        @Override
        public void handle(Void event) {
          InternetHeaders headers = new InternetHeaders();
          MimeBodyPart mbp = null;
          try {
            mbp = new MimeBodyPart(headers, content.getBytes());
            mbp.setFileName(upload.filename());
            mmp.addBodyPart(mbp);
            content = null;
          } catch (MessagingException e) {
            log.error(e);
          }
        }
      });
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
    Response result = ((Response) ((AsyncResult<?>) v).result());
    if (result == null) {
      // catch all
      endRequestWithError(rc, 500, true, "Server error", new boolean[] { true });
      return;
    }

    try {
      HttpServerResponse response = rc.response();
      int statusCode = result.getStatus();
      // 204 means no content returned in the response, so passing
      // a chunked Transfer header is not allowed
      if (statusCode != 204) {
        response.setChunked(true);
      }
      response.setStatusCode(statusCode);

      // !!!!!!!!!!!!!!!!!!!!!! CORS commented OUT!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      // response.putHeader("Access-Control-Allow-Origin", "*");

      for (Entry<String, List<String>> entry : result.getStringHeaders().entrySet()) {
        String jointValue = Joiner.on("; ").join(entry.getValue());
        response.headers().add(entry.getKey(), jointValue);
      }

      //response.headers().add(HttpHeaders.ACCEPT_ENCODING, "gzip, deflate");
      //forward all headers except content-type that was passed in by the client
      //since this may cause problems when for example application/octet-stream is
      //sent as part of an upload. passing this back will confuse clients as they
      //will think they are getting back a stream of data which may not be the case
      rc.request().headers().remove("Content-type");

      mergeIntoResponseHeadersDistinct(response.headers(), rc.request().headers());

      Object entity = result.getEntity();

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
      log.error(e);
    } finally {
      rc.response().end();
    }

    long end = System.nanoTime();

    StringBuffer sb = new StringBuffer();
    if (log.isDebugEnabled()) {
      sb.append(rc.getBodyAsString());
    }
    LogUtil.formatStatsLogMessage(rc.request().remoteAddress().toString(), rc.request().method().toString(),
      rc.request().version().toString(), rc.response().getStatusCode(), (((end - start) / 1000000)), rc.response().bytesWritten(),
      rc.request().path(), rc.request().query(), rc.response().getStatusMessage(), tenantId, sb.toString());
  }

  private void mergeIntoResponseHeadersDistinct(MultiMap responseHeaders, MultiMap requestHeaders){
    Consumer<Map.Entry<String,String>> consumer = entry -> {
      String headerName = entry.getKey().toLowerCase();
      if(!responseHeaders.contains(headerName)){
        responseHeaders.add(headerName, entry.getValue());
      }
    };
    requestHeaders.forEach(consumer);
  }

  private void endRequestWithError(RoutingContext rc, int status, boolean chunked, String message,  boolean[] isValid) {
    if (isValid[0]) {
      log.error(rc.request().absoluteURI() + " [ERROR] " + message);
      rc.response().setChunked(chunked);
      rc.response().setStatusCode(status);
      if(message != null){
        rc.response().write(message);
      }
      rc.response().end();
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

  public void invoke(Method method, Object[] params, Object o, RoutingContext rc, String[] tenantId,
      Map<String,String> headers, StreamStatus streamed, Handler<AsyncResult<Response>> resultHandler) {

    Context context = vertx.getOrCreateContext();

    //if streaming is requested the status will be 0 (streaming started)
    //or 1 streaming data complete
    //otherwise it will be -1 and flags wont be set
    if(streamed.status == 0){
      headers.put(STREAM_ID, String.valueOf(rc.hashCode()));
    }
    else if(streamed.status == 1){
      headers.put(STREAM_ID, String.valueOf(rc.hashCode()));
      headers.put(STREAM_COMPLETE, String.valueOf(rc.hashCode()));
    }

    Object[] newArray = new Object[params.length];
    for (int i = 0; i < params.length - 3; i++) {
      newArray[i] = params[i];
    }

    //inject call back handler into each function
    newArray[params.length - 2] = resultHandler;

    //inject vertx context into each function
    newArray[params.length - 1] = getVertx().getOrCreateContext();

/*    if(tenantId[0] == null){
      headers.put(OKAPI_HEADER_TENANT, DEFAULT_SCHEMA);
    }*/
    newArray[params.length - 3] = headers;

    context.runOnContext(v -> {
      try {
        method.invoke(o, newArray);
        // response.setChunked(true);
        // response.setStatusCode(((Response)result).getStatus());
      } catch (Exception e) {
        log.error(e);
        String message;
        try {
          // catch exception for now in case of null point and show generic
          // message
          message = e.getCause().getMessage();
        } catch (Throwable ee) {
          message = messages.getMessage("en", MessageConsts.UnableToProcessRequest);
        }
        endRequestWithError(rc, 400, true, message, new boolean[] { true });
      }

    });
  }

  public JsonObject loadConfig(String configFile) {
    try {
      byte[] jsonData = ByteStreams.toByteArray(getClass().getClassLoader().getResourceAsStream(configFile));
      return new JsonObject(new String(jsonData));
    } catch (IOException e) {
      log.error(e);
    }
    return new JsonObject();
  }

  private static String replaceLast(String string, String substring, String replacement) {
    int index = string.lastIndexOf(substring);
    if (index == -1)
      return string;
    return string.substring(0, index) + replacement + string.substring(index + substring.length());
  }

  private MappedClasses populateConfig() {
    MappedClasses mappedURLs = new MappedClasses();
    JsonObject jObjClasses = new JsonObject();
    try {
      jObjClasses.mergeIn(AnnotationGrabber.generateMappings());
    } catch (Exception e) {
      log.error(e);
    }
    // loadConfig(JSON_URL_MAPPINGS);
    Set<String> classURLs = jObjClasses.fieldNames();
    classURLs.forEach(classURL -> {
      System.out.println(classURL);
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
  public void stop(Future<Void> stopFuture) throws Exception {
    super.stop();
    try {
      droolsSession.dispose();
    } catch (Exception e) {/*ignore*/}
    // removes the .lck file associated with the log file
    LogUtil.closeLogger();
    runShutdownHook(v -> {
      if (v.succeeded()) {
        stopFuture.complete();
      } else {
        stopFuture.fail("shutdown hook failed....");
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
        method.invoke(aClass.get(i).newInstance(), vertx, vertx.getOrCreateContext(), resultHandler);
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
              method1.invoke(aClass.get(j).newInstance(), vertx, vertx.getOrCreateContext());
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
        method.invoke(aClass.get(i).newInstance(), vertx, vertx.getOrCreateContext(), resultHandler);
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
        method.invoke(aClass.get(i).newInstance(), vertx, vertx.getOrCreateContext(), resultHandler);
        LogUtil.formatLogMessage(getClass().getName(), "runShutdownHook",
          "shutdown hook called with implemented class " + "named " + aClass.get(i).getName());
      }
    } catch (ClassNotFoundException e) {
      // no hook implemented, this is fine, just startup normally then
      LogUtil.formatLogMessage(getClass().getName(), "runShutdownHook", "no shutdown hook implementation found, continuing with shutdown");
      resultHandler.handle(io.vertx.core.Future.succeededFuture());
    }
  }

  private void cmdProcessing() throws Exception {
    String importDataPath = null;
    String droolsPath = null;
    // TODO need to add a normal command line parser
    List<String> cmdParams = processArgs();

    if (cmdParams != null) {
      for (Iterator iterator = cmdParams.iterator(); iterator.hasNext();) {
        String param = (String) iterator.next();

        if (param.startsWith("-Dhttp.port=")) {
          port = Integer.parseInt(param.split("=")[1]);
          LogUtil.formatLogMessage(className, "cmdProcessing", "port to listen on " + port);
        }
        else if (param.startsWith("drools_dir=")) {
          droolsPath = param.split("=")[1];
          LogUtil.formatLogMessage(className, "cmdProcessing", "Drools rules file dir set to " + droolsPath);
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
            log.error(e);
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

    try {
      droolsSession = new Rules(droolsPath).buildSession();
    } catch (Exception e) {
      log.error(e);
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
      String contentType = StringUtils.defaultString(request.getHeader("Content-type")).replaceFirst(";.*", "").trim();
      if (!consumes.contains(removeBoundry(contentType))) {
        endRequestWithError(rc, 400, true, messages.getMessage("en", MessageConsts.ContentTypeError, consumes, contentType),
          validRequest);
      }
    }

    // type of data expected to be returned by the server
    if (produces != null && validRequest[0]) {
      String accept = StringUtils.defaultString(request.getHeader("Accept"));
      if (acceptCheck(produces, accept) == null) {
        // use contains because multiple values may be passed here
        // for example json/application; text/plain mismatch of content type found
        endRequestWithError(rc, 400, true, messages.getMessage("en", MessageConsts.AcceptHeaderError, produces, accept),
          validRequest);
      }
    }
  }

  private void parseParams(RoutingContext rc, Iterator<Map.Entry<String, Object>> paramList, boolean[] validRequest, JsonArray consumes,
      Object[] paramArray, String[] pathParams) {

    HttpServerRequest request = rc.request();
    MultiMap queryParams = request.params();
    int []pathParamsIndex = new int[] { pathParams.length };

    paramList.forEachRemaining(entry -> {
      if (validRequest[0]) {
        String valueName = ((JsonObject) entry.getValue()).getString("value");
        String valueType = ((JsonObject) entry.getValue()).getString("type");
        String paramType = ((JsonObject) entry.getValue()).getString("param_type");
        int order = ((JsonObject) entry.getValue()).getInteger("order");
        Object defaultVal = ((JsonObject) entry.getValue()).getValue("default_value");

        boolean emptyNumeircParam = false;
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
                !valueType.equals("java.util.Map") && !valueType.equals("java.io.InputStream")) {
              // we have special handling for the Result Handler and context, it is also assumed that
              //an inputsteam parameter occurs when application/octet is declared in the raml
              //in which case the content will be streamed to he function
              String bodyContent = rc.getBodyAsString();
              if(bodyContent != null){
                if("java.io.Reader".equals(valueType)){
                  paramArray[order] = new StringReader(bodyContent);
                }
                else if(bodyContent.length() > 0) {
                  log.debug(" -------- bodyContent -------- " + bodyContent);
                  paramArray[order] = MAPPER.readValue(bodyContent, entityClazz);
                }
              }

              if(!allowEmptyObject(entityClazz, bodyContent)){
                //right now - because no way in raml to make body optional - do not validate
                //TenantAttributes object as it may be empty
                Set<? extends ConstraintViolation<?>> validationErrors = validationFactory.getValidator().validate(paramArray[order]);
                if (validationErrors.size() > 0) {
                  StringBuffer sb = new StringBuffer();
                  for (ConstraintViolation<?> cv : validationErrors) {
                    sb.append("\n" + cv.getPropertyPath() + "  " + cv.getMessage() + ",");
                  }
                  endRequestWithError(rc, 400, true, "Object validation errors " + sb.toString() , validRequest);
                }
              }
              // complex rules validation here (drools) - after simpler validation rules pass -
              try {
                // if no /rules exist then drools session will be null
                // TODO support adding .drl files dynamically to db / dir
                // and having them picked up
                if (droolsSession != null && paramArray[order] != null && validRequest[0]) {
                  // add object to validate to session
                  FactHandle handle = droolsSession.insert(paramArray[order]);
                  // run all rules in session on object
                  droolsSession.fireAllRules();
                  // remove the object from the session
                  droolsSession.delete(handle);
                }
              } catch (Exception e) {
                log.error(e);
                endRequestWithError(rc, 400, true, e.getCause().getMessage(), validRequest);
              }
              // }
            }
          } catch (Exception e) {
            log.error(e);
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
          paramArray[order] = pathParams[pathParamsIndex[0] - 1];
          pathParamsIndex[0] = pathParamsIndex[0] - 1;
        } else if (AnnotationGrabber.QUERY_PARAM.equals(paramType)) {
          String param = queryParams.get(valueName);
          // support enum, numbers or strings as query parameters
          try {
            if (valueType.contains("String")) {
              // regular string param in query string - just push value
              if (param == null && defaultVal != null) {
                // no value passed - check if there is a default value
                paramArray[order] = defaultVal;
              } else {
                paramArray[order] = param;
              }
            } else if (valueType.contains("int") || valueType.contains("Integer")) {
              // cant pass null to an int type
              if (param == null) {
                if (defaultVal != null) {
                  paramArray[order] = Integer.valueOf((String) defaultVal);
                } else {
                  paramArray[order] = 0;
                }
              }
              else if("".equals(param)) {
                emptyNumeircParam = true;
              }
              else {
                paramArray[order] = Integer.valueOf(param).intValue();
              }
            } else if (valueType.contains("boolean") || valueType.contains("Boolean")) {
              if (param == null) {
                if (defaultVal != null) {
                  paramArray[order] = Boolean.valueOf((String)defaultVal);
                }
              } else {
                paramArray[order] = Boolean.valueOf(param);
              }
            } else if (valueType.contains("BigDecimal")) {
              if (param == null) {
                if (defaultVal != null) {
                  paramArray[order] = new BigDecimal((String) defaultVal);
                } else {
                  paramArray[order] = null;
                }
              }
              else if ("".equals(param)) {
                emptyNumeircParam = true;
              }
              else {
                paramArray[order] = new BigDecimal(param.replaceAll(",", "")); // big decimal can contain ","
              }
            } else { // enum object type
              try {
                String enumClazz = replaceLast(valueType, ".", "$");
                Class<?> enumClazz1 = Class.forName(enumClazz);
                if (enumClazz1.isEnum()) {
                  Object defaultEnum = null;
                  Object[] vals = enumClazz1.getEnumConstants();
                  for (int i = 0; i < vals.length; i++) {
                    if (vals[i].toString().equals(defaultVal)) {
                      defaultEnum = vals[i];
                    }
                    // set default value (if there was one in the raml)
                    // in case no value was passed in the request
                    if (param == null && defaultEnum != null) {
                      paramArray[order] = defaultEnum;
                      break;
                    }
                    // make sure enum value is valid by converting the string to an enum
                    else if (vals[i].toString().equals(param)) {
                      paramArray[order] = vals[i];
                      break;
                    }
                    if (i == vals.length - 1) {
                      // if enum passed is not valid, replace with default value
                      paramArray[order] = defaultEnum;
                    }
                  }
                }
              } catch (Exception ee) {
                log.error(ee.getMessage(), ee);
                endRequestWithError(rc, 400, true, ee.getMessage(), validRequest);
              }
            }
            if(emptyNumeircParam){
              endRequestWithError(rc, 400, true, valueName + " does not have a default value in the RAML and has been passed empty",
                validRequest);
            }
          } catch (Exception e) {
            log.error(e.getMessage(), e);
            endRequestWithError(rc, 400, true, e.getMessage(), validRequest);
          }
        }
      }
    });
  }

  private boolean allowEmptyObject(Class clazz, String bodyContent){
    if(clazz.getName().equals(TenantAttributes.class.getName())){
      //right now - because no way in raml to make body optional - do not validate
      //TenantAttributes object if it is empty - since this is allowed
      if(bodyContent == null || bodyContent.length() == 0){
        return true;
      }
    }
    return false;
  }

  public static MetricsService getServerMetrics(){
    return serverMetrics;
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
}
