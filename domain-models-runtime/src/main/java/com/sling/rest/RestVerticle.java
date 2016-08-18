package com.sling.rest;

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
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.example.util.Runner;
import io.vertx.ext.dropwizard.MetricsService;
import io.vertx.ext.web.FileUpload;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.StaticHandler;

import java.io.IOException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.mail.internet.InternetHeaders;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.ValidatorFactory;
import javax.ws.rs.core.Response;

import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.FactHandle;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.folio.rulez.Rules;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import com.google.common.reflect.ClassPath;
import com.sling.rest.persist.MongoCRUD;
import com.sling.rest.persist.PostgresClient;
import com.sling.rest.resource.utils.LogUtil;
import com.sling.rest.resource.utils.OutStream;
import com.sling.rest.tools.AnnotationGrabber;
import com.sling.rest.tools.Messages;

public class RestVerticle extends AbstractVerticle {

  private final Logger log = LoggerFactory.getLogger(getClass());
  
  public static final String        API_BUS_ADDRESS                 = "bus.api.rest";
  public static final String        JSON_URL_MAPPINGS               = "API_PATH_MAPPINGS";

  private static final String       CORS_ALLOW_HEADER               = "Access-Control-Allow-Origin";
  private static final String       CORS_ALLOW_ORIGIN               = "Access-Control-Allow-Headers";

  private static final String       CORS_ALLOW_HEADER_VALUE         = "*";
  private static final String       CORS_ALLOW_ORIGIN_VALUE         = "Origin, Authorization, X-Requested-With, Content-Type, Accept";

  private static final String       PACKAGE_OF_IMPLEMENTATIONS      = "com.sling.rest.impl";
  private static final String       PACKAGE_OF_HOOK_INTERFACES      = "com.sling.rest.resource.interfaces";

  private static final String       SUPPORTED_CONTENT_TYPE_FORMDATA = "multipart/form-data";
  private static final String       SUPPORTED_CONTENT_TYPE_STREAMIN = "application/octet-stream";
  private static final String       SUPPORTED_CONTENT_TYPE_JSON_DEF = "application/json";

  private static ValidatorFactory   factory;
  private static KieSession         droolsSession;
  private final Messages            messages                        = Messages.getInstance();
  private HttpServer                server;
  private static final ObjectMapper mapper                          = new ObjectMapper();

  private static int                port                            = -1;                                                             // read

  // this is only to run via IDE - otherwise see pom which runs the verticle and
  // requires passing -cluster and preferable -cluster-home args
  public static void main(String[] args) {
    Runner.runExample(RestVerticle.class);
  }

  static {
    factory = Validation.buildDefaultValidatorFactory();

  }

  @Override
  public void start(Future<Void> startFuture) throws Exception {

    // event bus to use for communicating between REST verticle and persistence
    // layer

    // EVENTBUS = getVertx().eventBus();
    
    cmdProcessing();
    
    System.out.println("metrics enabled: " + vertx.isMetricsEnabled());

    MetricsService metricsService = MetricsService.create(vertx);

    // maps API_PATH_MAPPINGS file into java object for quick access
    MappedClasses mappedURLs = populateConfig();

    // set of exposed urls as declared in the raml
    Set<String> urlPaths = mappedURLs.getAvailURLs();

    // create a map of regular expression to url path
    Map<String, Pattern> regex2Pattern = mappedURLs.buildURLRegex();

    // Create a router object.
    Router router = Router.router(vertx);

    // needed so that we get the body content of the request - note that this
    // will read the entire body into memory for every request - if this is not
    // needed - this line can be removed and can use something like this
    // https://github.com/vert-x3/vertx-examples/blob/master/core-examples/src/main/java/io/vertx/example/core/http/simpleformupload/SimpleFormUploadServer.java
    final BodyHandler handler = BodyHandler.create();

    router.route().handler(handler);

    // run pluggable startup code in a class implementing the InitAPI interface
    // in the "com.sling.rest.impl" package
    runHook(vv -> {
      if (((Future) vv).failed()) {
        System.out.println("init failed, exiting.......");
        startFuture.fail(((Future) vv).cause().getMessage());
        vertx.close();
        System.exit(-1);
        // startFuture.fail(res.cause().toString());
      } else {
        System.out.println("init succeeded.......");

        // startup periodic if exists
        try {
          runPeriodicHook();
        } catch (Exception e2) {
          e2.printStackTrace();
        }

        router.route("/apis/*").handler(rc -> {
          long start = System.nanoTime();
          try {
            Iterator<String> iter = urlPaths.iterator();
            boolean validPath = false;
            boolean[] validRequest = { true };
            // loop over regex patterns and try to match them against the requested
            // URL if no match is found, then the requested url is not supported by
            // the ramls and we return an error - this has positive security implications as well
          while (iter.hasNext()) {
            String regexURL = iter.next();
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
                  // assume cors and return header of preflight
                  // Access-Control-Allow-Origin

                  // REMOVE CORS SUPPORT FOR
                  // NOW!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

                  // rc.response().putHeader(CORS_ALLOW_ORIGIN,
                  // CORS_ALLOW_ORIGIN_VALUE);
                  // rc.response().putHeader(CORS_ALLOW_HEADER,
                  // CORS_ALLOW_HEADER_VALUE);

                  // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

                  rc.response().end();

                  return;
                }

                // the url exists but the http method requested does not match a function
                // meaning url+http method != a function
                endRequestWithError(rc, 400, true, messages.getMessage("en", "10005"), start);
                validRequest[0] = false;
              }
              Class<?> aClass;
              try {
                if (validRequest[0]) {
                  int groups = m.groupCount();
                  String[] pathParams = new String[groups];
                  for (int i = 0; i < groups; i++) {
                    pathParams[i] = m.group(i + 1);
                  }
                  String iClazz = ret.getString(AnnotationGrabber.CLASS_NAME);
                  // convert from interface to an actual class implementing it, which appears in the impl package
                  aClass = convert2Impl(PACKAGE_OF_IMPLEMENTATIONS, iClazz);
                  Object o = null;
                  // call back the constructor of the class - gives a hook into the class not based on the apis
                  // passing the vertx and context objects in to it.
                  try {
                    o = aClass.getConstructor(Vertx.class, Context.class).newInstance(vertx, vertx.getOrCreateContext());
                  } catch (Exception e) {
                    // if no such constructor was implemented call the
                    // default no param constructor to create the object to be used to call functions on
                    o = aClass.newInstance();
                  }
                  // function to invoke for the requested url
                  String function = ret.getString(AnnotationGrabber.FUNCTION_NAME);
                  // parameters for the function to invoke
                  JsonObject params = ret.getJsonObject(AnnotationGrabber.METHOD_PARAMS);
                  // create the parameters needed to invoke the function mapped to the requested URL
                  Iterator<Map.Entry<String, Object>> paramList = params.iterator();
                  Object[] paramArray = new Object[params.size()];
                  HttpServerRequest request = rc.request();
                  MultiMap queryParams = request.params();
                  int pathParamsIndex[] = new int[] { pathParams.length };

                  /*
                   * NOTE that the content type and accept headers will accept a partial match - for example: if the raml indicates a
                   * text/plain and an application/json content-type and only one is passed - it will accept it
                   */
                  JsonArray produces = ret.getJsonArray(AnnotationGrabber.PRODUCES);
                  // what the api will return as output (Accept)
                  JsonArray consumes = ret.getJsonArray(AnnotationGrabber.CONSUMES);
                  // what the api expects to get (content-type)

                  // get the content type passed in the request
                  String contentTypeRequested = request.getHeader("Content-type");
                  // check allowed content types in the raml for this resource + method
                  if (consumes != null && validRequest[0]) {
                    // if this was left out by the client they must add for request to return
                    // clean up simple stuff from the clients header - trim the string and remove ';' in case
                    // it was put there as a suffix
                    if (contentTypeRequested == null || !consumes.contains(contentTypeRequested.trim().replace(";", ""))) {
                      endRequestWithError(rc, 400, true, messages.getMessage("en", "10006"), start);
                      validRequest[0] = false;
                    }
                  }
                  String acceptableContentRequested = request.getHeader("Accept");
                  // type of data expected to be returned by the server
                  if (produces != null && validRequest[0]) {
                    if (acceptableContentRequested == null || !doesContain(produces.getList(), acceptableContentRequested)) {
                      // use contains because multiple values may be passed here
                      // for example json/application; text/plain mismatch of content type found
                      endRequestWithError(rc, 400, true, messages.getMessage("en", "10007"), start);
                      validRequest[0] = false;
                    }
                  }

                  paramList.forEachRemaining(entry -> {
                    if (validRequest[0]) {
                      String valueName = ((JsonObject) entry.getValue()).getString("value");
                      String valueType = ((JsonObject) entry.getValue()).getString("type");
                      String paramType = ((JsonObject) entry.getValue()).getString("param_type");
                      int order = ((JsonObject) entry.getValue()).getInteger("order");
                      Object defaultVal = ((JsonObject) entry.getValue()).getValue("default_value");

                      // validation of query params (other then enums), object in body,
                      // and some header params validated by jsr311 (aspects) -the rest are handled in the code here
                    if (AnnotationGrabber.NON_ANNOTATED_PARAM.equals(paramType)) {
                      // handle un-annotated parameters - this is assumed to be
                      // entities in HTTP BODY for post and put requests or the 2 injected params
                      // (vertx context and vertx handler)
                      try {
                        // this will also validate the json against the
                        // pojo created from the schema
                        Class<?> entityClazz = Class.forName(valueType);

                        if (!valueType.equals("io.vertx.core.Handler") && !valueType.equals("io.vertx.core.Context")) {
                          // we have special handling for the Result Handler check the data that is accepted by the api
                          // - it should be json or multipart content type of multipart should
                          // look something like -> multipart/form-data; boundary=----WebKitFormBoundaryzeZR8KqAYJyI2jPL
                          if (consumes != null && consumes.contains(SUPPORTED_CONTENT_TYPE_FORMDATA)) {
                            Buffer content = rc.getBody();
                            // MimeMultipart mmp1 =
                            // FileUploadsUtil.MultiPartFormData(content);
                            if (content != null) {
                              MimeMultipart mmp = new MimeMultipart();
                              InternetHeaders headers = new InternetHeaders();
                              Set<FileUpload> fileUploadSet = rc.fileUploads();
                              Iterator<FileUpload> fileUploadIterator = fileUploadSet.iterator();
                              while (fileUploadIterator.hasNext()) {
                                FileUpload fileUpload = fileUploadIterator.next();
                                // To get the uploaded file - to optimize - read first line of content
                                // - that is the multi-part form data delimiter - delimit the content based on that
                                // into the parts right now, reading from filesystem since vertx saves uploaded files
                                // there automatically
                                Buffer uploadedFile = vertx.fileSystem().readFileBlocking(fileUpload.uploadedFileName());
                                MimeBodyPart mbp = new MimeBodyPart(headers, uploadedFile.getBytes());
                                mmp.addBodyPart(mbp);
                                // consider not deleting for debug purposes
                                vertx.fileSystem().deleteBlocking(fileUpload.uploadedFileName());
                              }
                              paramArray[order] = mmp;
                            } else {
                              paramArray[order] = null;
                            }
                          }
                          // TODO Stream file uploads - by pushing into a temp object in memory and
                          // passing this to the function - and populate object and then end it to
                          // indicate all date read - then it can be async - so in the handler, will
                          // need to call invoke of the function and the function can then call the
                          // handler once it finished reading all content from temp object
                          /*
                           * else if (consumes != null && consumes.contains (SUPPORTED_CONTENT_TYPE_FORMDATA)) {
                           * 
                           * rc.request().handler( buf -> { System.out.println( new String( buf.getByteBuf().array() )); }); }
                           */
                          else {
                            paramArray[order] = mapper.readValue(rc.getBodyAsString(), entityClazz);

                            Set<? extends ConstraintViolation<?>> validationErrors = factory.getValidator().validate(paramArray[order]);
                            if (validationErrors.size() > 0) {
                              StringBuffer sb = new StringBuffer();
                              for (ConstraintViolation<?> cv : validationErrors) {
                                sb.append("\n" + cv.getPropertyPath() + "  " + cv.getMessage() + ",");
                              }
                              endRequestWithError(rc, 400, true, "Object validation errors " + sb.toString(), start);
                              validRequest[0] = false;
                            }

                            // complex rules validation here (drools) - after simpler validation rules pass -
                            try {
                              // if no /rules exist then drools session will be null
                              // TODO support adding .drl files dynamically to db / dir
                              // and having them picked up
                              if (droolsSession != null) {
                                // add object to validate to session
                                FactHandle handle = droolsSession.insert(paramArray[order]);
                                // run all rules in session on object
                                droolsSession.fireAllRules();
                                // remove the object from the session
                                droolsSession.delete(handle);
                              }
                            } catch (Exception e) {
                              e.printStackTrace();
                              endRequestWithError(rc, 400, true, e.getCause().getMessage(), start);
                              validRequest[0] = false;
                            }
                          }
                        }
                      } catch (Exception e) {
                        e.printStackTrace();
                        endRequestWithError(rc, 400, true, "Json content error " + e.getMessage(), start);
                        validRequest[0] = false;
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
                            paramArray[order] = (String) defaultVal;
                          } else {
                            paramArray[order] = param;
                          }
                        } else if (valueType.contains("int")) {
                          // cant pass null to an int type - replace with zero
                          if (param == null) {
                            if (defaultVal != null) {
                              paramArray[order] = Integer.valueOf((String) defaultVal);
                            } else {
                              paramArray[order] = 0;
                            }
                          } else {
                            paramArray[order] = Integer.valueOf(param).intValue();
                          }
                        } 
                        else if (valueType.contains("BigDecimal")) {
                          // cant pass null to an int type - replace with zero
                          if (param == null) {
                            if (defaultVal != null) {
                              paramArray[order] = new BigDecimal((String) defaultVal);
                            } else {
                              paramArray[order] = new BigDecimal(0);
                            }
                          } else {
                            paramArray[order] = new BigDecimal(param.replaceAll(",", "")); //big decimal can contain ","
                          }
                        }                         
                        else { // enum object type
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
                            ee.printStackTrace();
                            validRequest[0] = false;
                          }
                        }

                      } catch (Exception e) {
                        e.printStackTrace();
                        validRequest[0] = false;
                      }
                    }
                  }
                });

                  if (validRequest[0]) {
                    Method[] methods = aClass.getMethods();
                    for (int i = 0; i < methods.length; i++) {
                      if (methods[i].getName().equals(function)) {

                        // Object result = null;
                        try {
                          // result = methods[i].invoke(o, paramArray);
                          invoke(methods[i], paramArray, o, rc, v -> {
                            System.out.println(" invoking " + function + " response id " + rc.response().hashCode());
                            sendResponse(rc, v, start);
                          });
                        } catch (Exception e1) {
                          e1.printStackTrace();
                          rc.response().end();
                        }
                      }
                    }
                  }
                }
              } catch (Exception e) {
                e.printStackTrace();
                endRequestWithError(rc, 400, true, messages.getMessage("en", "10003") + e.getMessage(), start);
              }
            }
          }
          if (!validPath) {
            // invalid path
            endRequestWithError(rc, 400, true, "invalid path", start);
          }
        } finally {
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

        // CHANGED FOR NOW _ REMOVE CORS
        // SUPPORT!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

        /*
         * Set<HttpMethod> corsAllowedMethods = new HashSet<HttpMethod>(); corsAllowedMethods.add(HttpMethod.GET);
         * corsAllowedMethods.add(HttpMethod.OPTIONS); corsAllowedMethods.add(HttpMethod.PUT); corsAllowedMethods.add(HttpMethod.POST);
         * corsAllowedMethods.add(HttpMethod.DELETE);
         * 
         * router.route().handler( CorsHandler.create("*").allowedMethods(corsAllowedMethods
         * ).allowedHeader("Authorization").allowedHeader("Content-Type") .allowedHeader("Access-Control-Request-Method").allowedHeader(
         * "Access-Control-Allow-Credentials") .allowedHeader("Access-Control-Allow-Origin"
         * ).allowedHeader("Access-Control-Allow-Headers"));
         */

        // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

        if (port == -1) {
          port = config().getInteger("http.port", 8081);
        }
        Integer p = port;
        server = vertx
          .createHttpServer()
          .requestHandler(router::accept)
          // router accepts request and will pass to next handler for
          // specified path

          .listen(
            // Retrieve the port from the configuration file - file needs to
            // be passed as arg to command line,
            // for example: -conf src/main/conf/my-application-conf.json
            // default to 8181.
            p,
            result -> {
              System.out.println("http server for apis and docs started on port " + p
                  + ", \ndocumentation availble at:     /apidocs/index.html?raml=raml/<name_of_raml>.raml");
            });
        startFuture.complete();
      }
    });

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
  private void sendResponse(RoutingContext rc, AsyncResult<Response> v, long start) {
    Response result = ((Response) ((AsyncResult<?>) v).result());
    if (result == null) {
      // catch all
      endRequestWithError(rc, 500, true, "Server error", start);
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

      Object entity = result.getEntity();
      if (entity instanceof OutStream) {
        entity = ((OutStream) entity).getData();
      }
      if (entity != null) {
        response.write(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(entity));
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      rc.response().end();
    }

    long end = System.nanoTime();
    LogUtil.formatStatsLogMessage(rc.request().remoteAddress().toString(), rc.request().method().toString(), rc.request().version()
      .toString(), rc.response().getStatusCode(), (((end - start) / 1000000)), rc.response().bytesWritten(), rc.request().path(), rc
      .request().query(), rc.response().getStatusMessage());
  }

  private void endRequestWithError(RoutingContext rc, int status, boolean chunked, String message, long beginTime) {
    log.error(rc.request().absoluteURI() + " [ERROR] " + message);
    rc.response().setChunked(chunked);
    rc.response().setStatusCode(status);
    rc.response().write(message);
    rc.response().end();
  }


  public void invoke(Method method, Object[] params, Object o, RoutingContext rc, Handler<AsyncResult<Response>> resultHandler) {
    Context context = vertx.getOrCreateContext();
    Object[] newArray = new Object[params.length];
    for (int i = 0; i < params.length - 2; i++) {
      newArray[i] = params[i];
    }
    newArray[params.length - 2] = resultHandler;
    newArray[params.length - 1] = getVertx().getOrCreateContext();
    // newArray[params.length+1] = response;
    // params =
    context.runOnContext(v -> {
      try {
        method.invoke(o, newArray);
        // response.setChunked(true);
        // response.setStatusCode(((Response)result).getStatus());
    } catch (Exception e) {
      e.printStackTrace();
      String message;
      try {
        // catch exception for now in case of null point and show generic
        // message
        message = e.getCause().getMessage();
      } catch (Throwable ee) {
        message = messages.getMessage("en", "10003");
      }
      endRequestWithError(rc, 400, true, message, 0L);
    }

  });
  }

  public JsonObject loadConfig(String configFile) {
    try {
      byte[] jsonData = ByteStreams.toByteArray(getClass().getClassLoader().getResourceAsStream(configFile));
      return new JsonObject(new String(jsonData));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return new JsonObject();
  }

  private static String replaceLast(String string, String substring, String replacement) {
    int index = string.lastIndexOf(substring);
    if (index == -1)
      return string;
    return string.substring(0, index) + replacement + string.substring(index + substring.length());
  }

  /**
   * Return the implementing class.
   *
   * @param implDir
   *          - package name where to search
   * @param interface2check
   *          - class name of the required interface
   * @return implementing class
   * @throws IOException
   *           - if the attempt to read class path resources (jar files or directories) failed.
   * @throws ClassNotFoundException
   *           - if no class in implDir implements the interface
   */
  private static Class<?> convert2Impl(String implDir, String interface2check) throws IOException, ClassNotFoundException {
    ClassPath classPath = ClassPath.from(Thread.currentThread().getContextClassLoader());
    ImmutableSet<ClassPath.ClassInfo> classes = classPath.getTopLevelClasses(implDir);
    Class<?> impl = null;
    for (ClassPath.ClassInfo info : classes) {
      try {
        Class<?> clazz = Class.forName(info.getName());
        for (Class<?> anInterface : clazz.getInterfaces()) {
          if (!anInterface.getName().equals(interface2check)) {
            continue;
          }
          if (impl != null) {
            throw new RuntimeException("Duplicate implementation of " + interface2check + " in " + implDir + ": " + impl.getName() + ", "
                + clazz.getName());
          }
          impl = clazz;
        }
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }
    }
    if (impl == null) {
      throw new ClassNotFoundException("Implementation of " + interface2check + " not found in " + implDir);
    }
    return impl;
  }

  private MappedClasses populateConfig() {
    MappedClasses mappedURLs = new MappedClasses();
    JsonObject jObjClasses = loadConfig(JSON_URL_MAPPINGS);
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
    // TODO Auto-generated method stub
    super.stop();
    MongoCRUD.stopEmbeddedMongo();
    PostgresClient.stopEmbeddedPostgres();
    try {
      droolsSession.dispose();
    } catch (Exception e) {
    }
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

  /*
   * implementors of the InitAPI interface must call back the handler in there init() implementation like this:
   * resultHandler.handle(io.vertx.core.Future.succeededFuture(true)); or this will hang
   */
  private void runHook(Handler<AsyncResult<Boolean>> resultHandler) throws Exception {
    try {
      Class<?> aClass = convert2Impl(PACKAGE_OF_IMPLEMENTATIONS, PACKAGE_OF_HOOK_INTERFACES + ".InitAPI");
      Class<?>[] paramArray = new Class[] { Vertx.class, Context.class, Handler.class };
      Method method = aClass.getMethod("init", paramArray);
      method.invoke(aClass.newInstance(), vertx, vertx.getOrCreateContext(), resultHandler);
      LogUtil.formatLogMessage(getClass().getName(), "runHook",
        "One time hook called with implemented class " + "named " + aClass.getName());
    } catch (ClassNotFoundException e) {
      // no hook implemented, this is fine, just startup normally then
      resultHandler.handle(io.vertx.core.Future.succeededFuture(true));
    }
  }

  private void runPeriodicHook() throws Exception {
    try {
      Class<?> aClass = convert2Impl(PACKAGE_OF_IMPLEMENTATIONS, PACKAGE_OF_HOOK_INTERFACES + ".PeriodicAPI");
      Class<?>[] paramArray = new Class[] {};
      Method method = aClass.getMethod("runEvery", paramArray);
      Object delay = method.invoke(aClass.newInstance());
      LogUtil.formatLogMessage(getClass().getName(), "runPeriodicHook",
        "Periodic hook called with implemented class " + "named " + aClass.getName());
      vertx.setPeriodic(((Long) delay).longValue(), new Handler<Long>() {
        @Override
        public void handle(Long aLong) {
          try {
            Class<?>[] paramArray1 = new Class[] { Vertx.class, Context.class };
            Method method1 = aClass.getMethod("run", paramArray1);
            method1.invoke(aClass.newInstance(), vertx, vertx.getOrCreateContext());
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      });
    } catch (ClassNotFoundException e) {
      // no hook implemented, this is fine, just startup normally then
      LogUtil.formatLogMessage(getClass().getName(), "runPeriodicHook", "no periodic implementation found, continuing with deployment");
    }
  }

  private void runShutdownHook(Handler<AsyncResult<Void>> resultHandler) throws Exception {
    try {
      Class<?> aClass = convert2Impl(PACKAGE_OF_IMPLEMENTATIONS, PACKAGE_OF_HOOK_INTERFACES + ".ShutdownAPI");
      Class<?>[] paramArray = new Class[] { Vertx.class, Context.class, Handler.class };
      Method method = aClass.getMethod("shutdown", paramArray);
      method.invoke(aClass.newInstance(), vertx, vertx.getOrCreateContext(), resultHandler);
      LogUtil.formatLogMessage(getClass().getName(), "runShutdownHook",
        "shutdown hook called with implemented class " + "named " + aClass.getName());
    } catch (ClassNotFoundException e) {
      // no hook implemented, this is fine, just startup normally then
      LogUtil.formatLogMessage(getClass().getName(), "runShutdownHook", "no shutdown hook implementation found, continuing with shutdown");
      resultHandler.handle(io.vertx.core.Future.succeededFuture());
    }
  }

  /**
   * return true if at least one value in the source appears in the target
   * 
   * @param source
   * @param target
   * @return
   */
  private boolean doesContain(List source, String target) {
    if (target == null) {
      return false;
    }
    String targets[] = target.trim().replace(";", " ").split(" ");
    for (int i = 0; i < targets.length; i++) {
      if (source.contains(targets[i])) {
        return true;
      }
    }
    return false;
  }
  
  private void cmdProcessing() throws Exception {
    String importDataPath = null;
    String droolsPath = null;
    // TODO need to add a normal command line parser
    List<String> cmdParams = processArgs();
    
    if (cmdParams != null) {
      for (Iterator iterator = cmdParams.iterator(); iterator.hasNext();) {
        String param = (String) iterator.next();
        if ("embed_mongo=true".equals(param)) {
          MongoCRUD.setIsEmbedded(true);
        }
        else if (param.startsWith("http.port=")) {
          port = Integer.parseInt(param.split("=")[1]);
          System.out.println("port to listen on " + port);
        } 
        else if (param.startsWith("drools_dir=")) {
          droolsPath = param.split("=")[1];
          System.out.println("Drools rules file dir set to " + droolsPath);
        } 
        else if (param.startsWith("db_connection=")) {
          String dbconnection = param.split("=")[1];
          PostgresClient.setConfigFilePath(dbconnection);
          PostgresClient.setIsEmbedded(false);
          System.out.println("Setting path to db config file....  " + dbconnection);
        }
        else if ("embed_postgres=true".contains(param)) {
          //allow setting config() from unit test mode which runs embedded
          
          System.out.println("Using embedded postgres... starting... ");
          //this blocks
          PostgresClient.setIsEmbedded(true);
          PostgresClient.setConfigFilePath(null);
        }
        else if (param.startsWith("mongo_connection=")) {
          String dbconnection = param.split("=")[1];
          MongoCRUD.setConfigFilePath(dbconnection);
          MongoCRUD.setIsEmbedded(false);
          System.out.println("Setting path to mongo config file....  " + dbconnection);
        }
        else if (param != null && param.startsWith("postgres_import_path=")) {
          try{
            importDataPath = param.split("=")[1];
            System.out.println("Setting path to import DB file....  " + importDataPath);
          }
          catch(Exception e){
            //any problems - print exception and continue
            e.printStackTrace();
          }
        }
      }
      
      if(PostgresClient.isEmbedded()){
        PostgresClient.getInstance(vertx).startEmbeddedPostgres();
      }
      
      if(MongoCRUD.isEmbedded()){
        MongoCRUD.getInstance(vertx).startEmbeddedMongo();
      }
      
      if(importDataPath != null){
        //blocks as well for now
        System.out.println("Import DB file....  " + importDataPath);
        PostgresClient.getInstance(vertx).importFile(importDataPath);
      }
    }
    
    try {
      droolsSession = new Rules(droolsPath).buildSession();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
