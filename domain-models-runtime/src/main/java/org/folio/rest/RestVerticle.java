package org.folio.rest;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import io.vertx.core.Future;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.StaticHandler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.resource.DomainModelConsts;
import org.folio.rest.tools.client.test.HttpClientMock2;
import org.folio.rest.tools.utils.InterfaceToImpl;
import org.folio.rest.tools.utils.LogUtil;

public class RestVerticle extends AbstractVerticle {

  public static final String        OKAPI_HEADER_TENANT             = "x-okapi-tenant";
  public static final String        OKAPI_HEADER_TOKEN              = "x-okapi-token";
  public static final String        OKAPI_HEADER_PREFIX             = "x-okapi";
  public static final String        OKAPI_USERID_HEADER             = "X-Okapi-User-Id";
  public static final String        OKAPI_REQUESTID_HEADER          = "X-Okapi-Request-Id";
  public static final String        STREAM_ID                       =  "STREAMED_ID";
  public static final String        STREAM_COMPLETE                 =  "COMPLETE";
  public static final String        STREAM_ABORT                    =  "STREAMED_ABORT";

  public static final Map<String, String> MODULE_SPECIFIC_ARGS  = new HashMap<>(); //NOSONAR

  private static final String       HTTP_PORT_SETTING               = "http.port";
  private static final Logger       log                             = LogManager.getLogger(RestVerticle.class);
  private static String             deploymentId                     = "";

  private String packageOfImplementations;

  @Override
  public void start(Promise<Void> startPromise) throws Exception {

    readInGitProps();

    //process cmd line arguments
    cmdProcessing(processArgs());

    deploymentId = UUID.randomUUID().toString();

    log.info("metrics enabled: {}", vertx.isMetricsEnabled());

    packageOfImplementations = config().getString("packageOfImplementations", DomainModelConsts.PACKAGE_OF_IMPLEMENTATIONS);

    // Create a router object.
    Router router = Router.router(vertx);

    //single handler for all url calls other then documentation
    //which is handled separately
    //router.routeWithRegex("^(?!.*apidocs).*$").handler(rc -> route(mappedURLs, urlPaths, regex2Pattern, rc));
    // routes requests on “/assets/*” to resources stored in the “assets”
    // directory.
    router.route("/assets/*").handler(StaticHandler.create("assets"));

    // In the following example all requests to paths starting with
    // /apidocs/ will get served from the directory resources/apidocs:
    // example:
    // http://localhost:8181/apidocs/index.html?raml=raml/_patrons.raml
    router.route("/apidocs/*").handler(StaticHandler.create("apidocs"));
    // startup http server on port 8181 to serve documentation

    //if client includes an Accept-Encoding header which includes
    //the supported compressions - deflate or gzip.
    HttpServerOptions serverOptions = new HttpServerOptions();
    serverOptions.setCompressionSupported(true);

    HttpServer server = vertx.createHttpServer(serverOptions);
    String portS = System.getProperty(HTTP_PORT_SETTING);
    int port;
    if (portS != null) {
      port = Integer.parseInt(portS);
      config().put(HTTP_PORT_SETTING, port);
    } else {
      // we are here if port was not passed via cmd line
      port = config().getInteger(HTTP_PORT_SETTING, 8081);
    }
    RestRouting.populateRoutes(router, packageOfImplementations)
        .compose(x -> runHook())
        .compose(x -> {
          log.info("Listening port {}", port);
          return server.requestHandler(router).listen(port);
        })
        .<Void>compose(ret -> {
          try {
            // startup periodic impl if exists
            runPeriodicHook();
          } catch (Exception e) {
            log.error(e.getMessage(), e);
            return Future.failedFuture(e);
          }
          //check if mock mode requested and set sys param so that http client factory
          //can config itself accordingly
          String mockMode = config().getString(HttpClientMock2.MOCK_MODE);
          if (mockMode != null) {
            System.setProperty(HttpClientMock2.MOCK_MODE, mockMode);
          }
          runPostDeployHook(res2 -> {
            if (!res2.succeeded()) {
              log.error(res2.cause().getMessage(), res2.cause());
            }
          });
          return Future.succeededFuture();
        })
        .onFailure(cause -> log.error(cause.getMessage(), cause))
        .onComplete(startPromise);
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

  private ArrayList<Class<?>> convert2impl(String clazz, boolean allowMultiple)
      throws ClassNotFoundException, IOException {
    return InterfaceToImpl.convert2Impl(
        packageOfImplementations,
        DomainModelConsts.PACKAGE_OF_HOOK_INTERFACES + "." + clazz,
        allowMultiple);
  }

  /**
   * ONLY 1 Impl is allowed currently!
   * implementors of the InitAPI interface must call back the handler in there init() implementation like this:
   * resultHandler.handle(io.vertx.core.Future.succeededFuture(true)); or this will hang
   */
  private Future<Boolean> runHook() {
    Promise<Boolean> promise = Promise.promise();
    try {
      ArrayList<Class<?>> aClass = convert2impl("InitAPI", false);
      for (Class<?> value : aClass) {
        Class<?>[] paramArray = new Class[]{Vertx.class, Context.class, Handler.class};
        Method method = value.getMethod("init", paramArray);
        method.invoke(value.newInstance(), vertx, context, promise);
        log.info("Init hook called with implemented class named {}", value.getName());
      }
      return promise.future();
    } catch (ClassNotFoundException|NoSuchMethodException e) {
      // no hook implemented, this is fine, just startup normally then
      return Future.succeededFuture(Boolean.TRUE);
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  /**
   * multiple impl allowed
   * @throws Exception
   */
  private void runPeriodicHook() throws Exception {
    try {
      ArrayList<Class<?>> aClass = convert2impl("PeriodicAPI", true);
      for (int i = 0; i < aClass.size(); i++) {
        Class<?>[] paramArray = new Class[] {};
        Method method = aClass.get(i).getMethod("runEvery", paramArray);
        Object delay = method.invoke(aClass.get(i).newInstance());
        log.info("Periodic hook called with implemented class named {}", aClass.get(i).getName());
        final int j = i;
        vertx.setPeriodic((Long) delay, aLong -> {
          try {
            Class<?>[] paramArray1 = new Class[] { Vertx.class, Context.class };
            Method method1 = aClass.get(j).getMethod("run", paramArray1);
            method1.invoke(aClass.get(j).newInstance(), vertx, context);
          } catch (Exception e) {
            log.error(e.getMessage(), e);
          }
        });
      }
    } catch (ClassNotFoundException e) {
      // no hook implemented, this is fine, just startup normally then
      log.info("No periodic implementation found, continuing with deployment");
    }
  }

  /**
   * ONE impl allowed
   */
  private void runPostDeployHook(Handler<AsyncResult<Boolean>> resultHandler) {
    try {
      ArrayList<Class<?>> aClass = convert2impl("PostDeployVerticle", true);
      for (Class<?> value : aClass) {
        Class<?>[] paramArray = new Class[]{Vertx.class, Context.class, Handler.class};
        Method method = value.getMethod("init", paramArray);
        method.invoke(value.newInstance(), vertx, context, resultHandler);
        log.info("Post Deploy Hook called with implemented class named {}", value.getName());
      }
    } catch (ClassNotFoundException e) {
      // no hook implemented, this is fine, just startup normally then
      log.info("No Post Deploy Hook implementation found, continuing with deployment");
    } catch (IOException|ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * only one impl allowed
   * @param resultHandler
   * @throws Exception
   */
  private void runShutdownHook(Handler<AsyncResult<Void>> resultHandler) throws Exception {
    try {
      ArrayList<Class<?>> aClass = convert2impl("ShutdownAPI", false);
      for (Class<?> value : aClass) {
        Class<?>[] paramArray = new Class[]{Vertx.class, Context.class, Handler.class};
        Method method = value.getMethod("shutdown", paramArray);
        method.invoke(value.newInstance(), vertx, context, resultHandler);
        log.info("shutdown hook called with implemented class named {}", value.getName());
      }
    } catch (ClassNotFoundException e) {
      // no hook implemented, this is fine, just startup normally then
      log.info("No shutdown hook implementation found, continuing with shutdown");
      resultHandler.handle(io.vertx.core.Future.succeededFuture());
    }
  }

  private static void cmdProcessing(List<String> cmdParams) {
    // TODO need to add a normal command line parser

    if (cmdParams != null) {
      for (String param : cmdParams) {

        if (param.startsWith("debug_log_package=")) {
          String debugPackage = param.split("=")[1];
          if(debugPackage != null && debugPackage.length() > 0){
            log.info("Setting package {} to debug", debugPackage);
            LogUtil.updateLogConfiguration(debugPackage, "FINE");
          }
        }
        else if (param.startsWith("db_connection=")) {
          String dbconnection = param.split("=")[1];
          PostgresClient.setConfigFilePath(dbconnection);
          log.info("Setting path to db config file....  " + dbconnection);
        }
        else{
          //assume module specific cmd line args with '=' separator
          String []arg = param.split("=");
          if(arg.length == 2){
            MODULE_SPECIFIC_ARGS.put(arg[0], arg[1]);
            log.info("module specific argument added: {} with value {}", arg[0], arg[1]);
          }
          else{
            log.warn("The following cmd line parameter was skipped, {}. Expected format key=value\nIf this is a "
                + "JVM argument, pass it before the jar, not after", param);
          }
        }
      }
    }
  }

  public static String getDeploymentId(){
    return deploymentId;
  }
}
