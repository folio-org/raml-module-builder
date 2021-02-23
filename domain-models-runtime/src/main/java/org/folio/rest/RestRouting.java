package org.folio.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.google.common.base.Joiner;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.folio.HttpStatus;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.okapi.common.logging.FolioLoggingContext;
import org.folio.rest.annotations.Stream;
import org.folio.rest.jaxrs.model.Error;
import org.folio.rest.jaxrs.model.Errors;
import org.folio.rest.jaxrs.model.Parameter;
import org.folio.rest.resource.DomainModelConsts;
import org.folio.rest.tools.AnnotationGrabber;
import org.folio.rest.tools.client.exceptions.ResponseException;
import org.folio.rest.tools.messages.MessageConsts;
import org.folio.rest.tools.messages.Messages;
import org.folio.rest.tools.utils.AsyncResponseResult;
import org.folio.rest.tools.utils.BinaryOutStream;
import org.folio.rest.tools.utils.InterfaceToImpl;
import org.folio.rest.tools.utils.JsonUtils;
import org.folio.rest.tools.utils.LogUtil;
import org.folio.rest.tools.utils.MetadataUtil;
import org.folio.rest.tools.utils.OutStream;
import org.folio.rest.tools.utils.ResponseImpl;
import org.folio.rest.tools.utils.ValidationHelper;
import org.folio.util.StringUtil;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.ValidatorFactory;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class RestRouting {
  private static final String CONTENT_TYPE = "Content-Type";
  private static final String SUPPORTED_CONTENT_TYPE_JSON_DEF = "application/json";
  private static final String DEFAULT_CONTENT_TYPE = "application/json";
  private static final String SUPPORTED_CONTENT_TYPE_TEXT_DEF = "text/plain";
  private static final String[] DATE_PATTERNS = {
      "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
      "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
      "yyyy-MM-dd'T'HH:mm:ss'Z'",
      "yyyy-MM-dd'T'HH:mm:ss",
      "yyyy-MM-dd",
      "yyyy-MM",
      "yyyy"
  };
  private static final Logger LOGGER = LogManager.getLogger(RestRouting.class);
  private static final Messages MESSAGES = Messages.getInstance();
  private static final ObjectMapper MAPPER = ObjectMapperTool.getMapper();
  private static ValidatorFactory validationFactory = Validation.buildDefaultValidatorFactory();

  private RestRouting() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  private static void endRequestWithError(RoutingContext rc, int status, boolean chunked, String message) {
    HttpServerResponse response = rc.response();
    if (!response.closed()) {
      response.setChunked(chunked);
      response.setStatusCode(status);
      if (status == 422) {
        response.putHeader(CONTENT_TYPE, SUPPORTED_CONTENT_TYPE_JSON_DEF);
      } else {
        response.putHeader(CONTENT_TYPE, SUPPORTED_CONTENT_TYPE_TEXT_DEF);
      }
      if (message != null) {
        response.write(message);
      }
      response.end();
    }
    withRequestId(rc, () ->
        LogUtil.formatStatsLogMessage(rc, -1, null, message == null ? "" : message));
  }

  /**
   * Copy the headers from source to destination. Join several headers of same key using "; ".
   */
  private static void copyHeadersJoin(MultivaluedMap<String, String> source, MultiMap destination) {
    for (Map.Entry<String, List<String>> entry : source.entrySet()) {
      String jointValue = Joiner.on("; ").join(entry.getValue());
      try {
        destination.add(entry.getKey(), jointValue);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            e.getMessage() + ": " + entry.getKey() + " - " + jointValue, e);
      }
    }
  }

  /**
   * return whether the request is valid [0] and a cleaned up version of the object [1]
   *
   * @param rc
   * @param content
   * @param errorResp
   * @param singleField
   * @param entityClazz
   * @return
   */
  static Object[] isValidRequest(RoutingContext rc, Object content, Errors errorResp, List<String> singleField, Class<?> entityClazz) {
    Set<? extends ConstraintViolation<?>> validationErrors = validationFactory.getValidator().validate(content);
    if (validationErrors.isEmpty()) {
      return new Object[]{Boolean.TRUE, content};
    }
    boolean ret = true;
    for (ConstraintViolation<?> cv : validationErrors) {
      if ("must be null".equals(cv.getMessage())) {
        /**
         * read only fields are marked with a 'must be null' annotation @null
         * so the client should not pass them in, if they were passed in, remove them here
         * so that they do not reach the implementing function
         */
        try {
          if (!(content instanceof JsonObject)) {
            content = JsonObject.mapFrom(content);
          }
          ((JsonObject) content).remove(cv.getPropertyPath().toString());
          continue;
        } catch (Exception e) {
          withRequestId(rc, () -> LOGGER.warn("Failed to remove {} field from body when calling {}",
              cv.getPropertyPath(), rc.request().absoluteURI(), e));
        }
      }
      Error error = new Error();
      Parameter p = new Parameter();
      String field = cv.getPropertyPath().toString();
      p.setKey(field);
      Object val = cv.getInvalidValue();
      p.setValue(val == null ? "null" : val.toString());
      error.getParameters().add(p);
      error.setMessage(cv.getMessage());
      error.setCode("-1");
      error.setType(DomainModelConsts.VALIDATION_FIELD_ERROR);
      //return the error if the validation is requested on a specific field
      //and that field fails validation. if another field fails validation
      //that is ok as validation on that specific field wasnt requested
      //or there are validation errors and this is not a per field validation request
      if (singleField != null && (singleField.contains(field) || singleField.isEmpty())) {
        errorResp.getErrors().add(error);
        ret = false;
      }
    }
    if (content instanceof JsonObject) {
      //we have sanitized the passed in object by removing read-only fields
      try {
        content = MAPPER.readValue(((JsonObject) content).encode(), entityClazz);
      } catch (IOException e) {
        withRequestId(rc, () -> LOGGER.error(
            "Failed to serialize body content after removing read-only fields when calling {}",
            rc.request().absoluteURI(), e));
      }
    }
    return new Object[]{Boolean.valueOf(ret), content};
  }

  /**
   * @return the enum value of type valueType where value.name equals param (fall-back: equals defaultValue).
   * Return null if the type neither has param nor defaultValue.
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
    if (!enumClass.isEnum()) {
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

  private static void parseParams(RoutingContext rc, Buffer body, Iterator<Map.Entry<String, Object>> paramList,
                                  Object[] paramArray, String[] pathParams, Map<String, String> okapiHeaders, boolean[] useRC) {

    HttpServerRequest request = rc.request();
    MultiMap queryParams = request.params();
    int[] pathParamsIndex = new int[]{0};

    paramList.forEachRemaining(entry -> {
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
      if (AnnotationGrabber.NON_ANNOTATED_PARAM.equals(paramType)) {
        try {
          // this will also validate the json against the pojo created from the schema
          Class<?> entityClazz = Class.forName(valueType);

          if (valueType.equals("io.vertx.ext.web.RoutingContext")) {
            useRC[0] = true;
          } else if (!valueType.equals("io.vertx.core.Handler") && !valueType.equals("io.vertx.core.Context") &&
              !valueType.equals("java.util.Map") && !valueType.equals("java.io.InputStream")) {
            // we have special handling for the Result Handler and context, it is also assumed that
            //an inputsteam parameter occurs when application/octet is declared in the raml
            //in which case the content will be streamed to he function
            String bodyContent = body == null ? null : body.toString();
            withRequestId(rc, () -> LOGGER.debug("{} -------- bodyContent -------- {}",
                rc.request().path(), bodyContent));
            if (bodyContent != null) {
              if ("java.io.Reader".equals(valueType)) {
                paramArray[order] = new StringReader(bodyContent);
              } else if ("java.lang.String".equals(valueType)) {
                paramArray[order] = bodyContent;
              } else if (bodyContent.length() > 0) {
                try {
                  paramArray[order] = MAPPER.readValue(bodyContent, entityClazz);
                } catch (UnrecognizedPropertyException e) {
                  withRequestId(rc, () -> LOGGER.error(e.getMessage(), e));
                  endRequestWithError(rc, HttpStatus.HTTP_UNPROCESSABLE_ENTITY.toInt(), true, JsonUtils.entity2String(
                      ValidationHelper.createValidationErrorMessage("", "", e.getMessage())));
                  return;
                }
              }
            }
            Errors errorResp = new Errors();

            //is this request only to validate a field value and not an actual
            //request for additional processing
            List<String> field2validate = request.params().getAll("validate_field");
            Object[] resp = isValidRequest(rc, paramArray[order], errorResp, field2validate, entityClazz);
            boolean isValid = (boolean) resp[0];
            paramArray[order] = resp[1];

            if (!isValid) {
              endRequestWithError(rc, HttpStatus.HTTP_UNPROCESSABLE_ENTITY.toInt(), true,
                  JsonUtils.entity2String(errorResp));
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
              sendResponse(rc, arr, 0, null);
              return;
            }
            MetadataUtil.populateMetadata(paramArray[order], okapiHeaders);
          }
        } catch (Exception e) {
          withRequestId(rc, () -> LOGGER.error(e.getMessage(), e));
          endRequestWithError(rc, 400, true, "Json content error " + e.getMessage());
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
            if (param == null) {
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
            } else if ("".equals(param)) {
              emptyNumericParam = true;
            } else {
              paramArray[order] = Integer.valueOf(param);
            }
          } else if (valueType.equals("boolean") || valueType.equals("java.lang.Boolean")) {
            if (param == null) {
              if (defaultVal != null) {
                paramArray[order] = Boolean.valueOf((String) defaultVal);
              }
            } else {
              paramArray[order] = Boolean.valueOf(param);
            }
          } else if (valueType.contains("List")) {
            List<String> vals = queryParams.getAll(valueName);
            paramArray[order] = vals;
          } else if (valueType.equals("java.math.BigDecimal") || valueType.equals("java.lang.Number")) {
            if (param == null) {
              if (defaultVal != null) {
                paramArray[order] = new BigDecimal((String) defaultVal);
              } else {
                paramArray[order] = null;
              }
            } else if ("".equals(param)) {
              emptyNumericParam = true;
            } else {
              paramArray[order] = new BigDecimal(param.replace(",", "")); // big decimal can contain ","
            }
          } else { // enum object type
            try {
              paramArray[order] = parseEnum(valueType, param, defaultVal);
            } catch (Exception ee) {
              withRequestId(rc, () -> LOGGER.error(ee.getMessage(), ee));
              endRequestWithError(rc, 400, true, ee.getMessage());
            }
          }
          if (emptyNumericParam) {
            endRequestWithError(rc, 400, true, valueName + " does not have a default value in the RAML and has been passed empty");
          }
        } catch (Exception e) {
          withRequestId(rc, () -> LOGGER.error(e.getMessage(), e));
          endRequestWithError(rc, 400, true, e.getMessage());
        }
      }
    });
  }

  static void invoke(Method method, Object[] params, Object o, RoutingContext rc, boolean addRCParam,
                     Map<String, String> headers, StreamStatus streamed, Handler<AsyncResult<Response>> resultHandler) {

    //if streaming is requested the status will be 0 (streaming started)
    //or 1 streaming data complete
    //or 2 streaming aborted
    //otherwise it will be -1 and flags wont be set
    if (streamed.getStatus() == 0) {
      headers.put(RestVerticle.STREAM_ID, String.valueOf(rc.hashCode()));
    } else if (streamed.getStatus() == 1) {
      headers.put(RestVerticle.STREAM_ID, String.valueOf(rc.hashCode()));
      headers.put(RestVerticle.STREAM_COMPLETE, String.valueOf(rc.hashCode()));
    } else if (streamed.getStatus() == 2) {
      headers.put(RestVerticle.STREAM_ID, String.valueOf(rc.hashCode()));
      headers.put(RestVerticle.STREAM_ABORT, String.valueOf(rc.hashCode()));
    }

    Object[] newArray = new Object[params.length];
    int size = 3;
    int pos = 0;

    //this endpoint indicated it wants to receive the routing context as a parameter
    if (addRCParam) {
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
    newArray[params.length - (size - (pos + 1))] = resultHandler;

    //inject vertx context into each function
    newArray[params.length - (size - (pos + 2))] = rc.vertx().getOrCreateContext();

    newArray[params.length - (size - pos)] = headers;

    headers.forEach(FolioLoggingContext::put);

    try {
      method.invoke(o, newArray);
    } catch (Exception e) {
      withRequestId(rc, () -> LOGGER.error(e.getMessage(), e));
      String message;
      try {
        // catch exception for now in case of null point and show generic
        // message
        message = e.getCause().getMessage();
      } catch (Exception ee) {
        message = MESSAGES.getMessage("en", MessageConsts.UnableToProcessRequest);
      }
      endRequestWithError(rc, 400, true, message);
    }
  }

  private static void handleStream(Method method2Run, RoutingContext rc, boolean useRoutingContext,
                                   Object instance, String[] tenantId, Map<String, String> okapiHeaders,
                                   JsonObject params, Object[] paramArray, long start) {
    final int[] uploadParamPosition = new int[]{-1};
    params.forEach(param -> {
      if (((JsonObject) param.getValue()).getString("type").equals("java.io.InputStream")) {
        //application/octet-stream passed - this is handled in a stream like manner
        //and the corresponding function called must annotate with a @Stream - and be able
        //to handle the function being called repeatedly on parts of the data
        uploadParamPosition[0] = ((JsonObject) param.getValue()).getInteger("order");
      }
    });
    HttpServerRequest request = rc.request();
    request.handler(buff -> {
      try {
        StreamStatus stat = new StreamStatus();
        stat.setStatus(0);
        paramArray[uploadParamPosition[0]] = new ByteArrayInputStream(buff.getBytes());
        invoke(method2Run, paramArray, instance, rc, useRoutingContext, okapiHeaders, stat, v -> {
          withRequestId(rc, () -> LogUtil.formatLogMessage(method2Run.getName(), method2Run.getName(), " invoking " + method2Run));
        });
      } catch (Exception e1) {
        withRequestId(rc, () -> LOGGER.error(e1.getMessage(), e1));
        rc.response().end();
      }
    });
    request.endHandler(e -> {
      StreamStatus stat = new StreamStatus();
      stat.setStatus(1);
      paramArray[uploadParamPosition[0]] = new ByteArrayInputStream(new byte[0]);
      invoke(method2Run, paramArray, instance, rc, useRoutingContext, okapiHeaders, stat, v -> {
        withRequestId(rc, () -> LogUtil.formatLogMessage(method2Run.getName(), method2Run.getName(), " invoking " + method2Run));
        //all data has been stored in memory - not necessarily all processed
        sendResponse(rc, v, start, tenantId[0]);
      });
    });
    request.exceptionHandler(event -> {
      StreamStatus stat = new StreamStatus();
      stat.setStatus(2);
      paramArray[uploadParamPosition[0]] = new ByteArrayInputStream(new byte[0]);
      invoke(method2Run, paramArray, instance, rc, useRoutingContext, okapiHeaders, stat,
          v -> withRequestId(rc, () ->
              LogUtil.formatLogMessage(method2Run.getName(), method2Run.getName(), " invoking " + method2Run))
      );
      endRequestWithError(rc, 400, true, "unable to upload file " + event.getMessage());
    });
  }

  static void getOkapiHeaders(RoutingContext rc, Map<String, String> headers, String[] tenantId) {
    MultiMap mm = rc.request().headers();
    Consumer<Map.Entry<String, String>> consumer = entry -> {
      String headerKey = entry.getKey().toLowerCase(); // should be changed and headers should be multiMap and not Map
      if (headerKey.startsWith(RestVerticle.OKAPI_HEADER_PREFIX)) {
        if (headerKey.equalsIgnoreCase(RestVerticle.OKAPI_HEADER_TENANT)) {
          tenantId[0] = entry.getValue();
        }
        headers.put(headerKey, entry.getValue());
      }
    };
    mm.forEach(consumer);
  }

  /**
   * Run logCommand with request id. Take request id value from headers in routingContext
   * and temporarily store it as reqId in ThreadContext. Run logCommand without reqId
   * if request id value is <code>null</code>.
   */
  static void withRequestId(RoutingContext routingContext, Runnable logCommand) {
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
   * @param rc    - where to send the result
   * @param v     - the result to send
   * @param start - request's start time, using JVM's high-resolution time source, in nanoseconds
   */
  static void sendResponse(RoutingContext rc, AsyncResult<Response> v, long start, String tenantId) {
    Response responseFromResult = getResponse(v);
    if (responseFromResult == null) {
      endRequestWithError(rc, 500, true, "null response from handler");
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

      copyHeadersJoin(responseFromResult.getStringHeaders(), response.headers());

      entity = responseFromResult.getEntity();

      /* entity is of type OutStream - and will be written as a string */
      if (entity instanceof OutStream) {
        response.write(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(((OutStream) entity).getData()));
      }
      /* entity is of type BinaryOutStream - and will be written as a buffer */
      else if (entity instanceof BinaryOutStream) {
        response.write(Buffer.buffer(((BinaryOutStream) entity).getData()));
      }
      /* data is a string so just push it out, no conversion needed */
      else if (entity instanceof String) {
        response.write(Buffer.buffer((String) entity));
      }
      /* catch all - anything else will be assumed to be a pojo which needs converting to json */
      else if (entity != null) {
        response.write(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(entity));
      }
    } catch (Exception e) {
      withRequestId(rc, () -> LOGGER.error(e.getMessage(), e));
    } finally {
      rc.response().end();
    }

    long end = System.nanoTime();

    StringBuilder sb = new StringBuilder();
    if (LOGGER.isDebugEnabled()) {
      try {
        sb.append(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(entity));
      } catch (Exception e) {
        String name = entity == null ? "null" : entity.getClass().getName();
        withRequestId(rc, () -> LOGGER.error("writeValueAsString({})", name, e));
      }
    }
    withRequestId(rc, () -> LogUtil.formatStatsLogMessage(rc, (end - start) / 1000000, tenantId, sb.toString()));
  }

  /**
   * @param annotations
   * @return
   */
  static boolean isStreamed(Annotation[] annotations) {
    for (int i = 0; i < annotations.length; i++) {
      if (annotations[i].annotationType().equals(Stream.class)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Match the path agaist pattern.
   *
   * @return the matching groups urldecoded, may be an empty array, or null if the pattern doesn't match
   */
  static String[] matchPath(String path, Pattern pattern) {
    Matcher m = pattern.matcher(path);
    if (!m.find()) {
      return new String[0];
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

  static Object construct(Vertx vertx, String tenantId, Class<?> aClass)
      throws IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException {
    try {
      Constructor<?> constructorWithArgs = aClass.getConstructor(Vertx.class, String.class);
      return constructorWithArgs.newInstance(vertx, tenantId);
    } catch (NoSuchMethodException e1) {
      Constructor<?> constructorWithNoArgs = aClass.getConstructor();
      return constructorWithNoArgs.newInstance();
    }
  }

  static void handleRequest(RoutingContext rc, Class<?> aClass, JsonObject ret, Method method, Pattern pattern) {
    long start = System.nanoTime();
    Map<String, String> okapiHeaders = new CaseInsensitiveMap<>();
    String []tenantId = new String[]{null};
    getOkapiHeaders(rc, okapiHeaders, tenantId);
    if (tenantId[0] == null && !rc.request().path().startsWith("/admin")) {
      endRequestWithError(rc, 400, true,
          MESSAGES.getMessage("en", MessageConsts.UnableToProcessRequest) + " Tenant must be set");
      return;
    }
    Object instanceTmp;
    try {
      instanceTmp = construct(rc.vertx(), tenantId[0], aClass);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      endRequestWithError(rc, 500, true, "Server error");
      return;
    }
    final Object instance = instanceTmp;

    JsonObject params = ret.getJsonObject(AnnotationGrabber.METHOD_PARAMS);

    Iterator<Map.Entry<String, Object>> paramList = params.iterator();
    Object[] paramArray = new Object[params.size()];

    // what the api will return as output (Content-Type)
    JsonArray produces = ret.getJsonArray(AnnotationGrabber.PRODUCES);
    // what the api expects to get (Accept)
    JsonArray consumes = ret.getJsonArray(AnnotationGrabber.CONSUMES);

    checkAcceptContentType(produces, consumes, rc);
    if (rc.response().ended()) {
      return;
    }
    final boolean[] useRoutingContext = { false };

    String[] pathParams = matchPath(rc.request().path(), pattern);

    boolean streamData = isStreamed(method.getAnnotations());
    if (streamData) {
      parseParams(rc, null, paramList, paramArray, pathParams, okapiHeaders, useRoutingContext);
      if (rc.response().ended()) {
        return;
      }
      handleStream(method, rc, useRoutingContext[0], instance, tenantId, okapiHeaders,
          params, paramArray, start);
    } else {
      // regular request (no streaming).. Read the request body before checking params + body
      Buffer body = Buffer.buffer();
      rc.request().handler(body::appendBuffer);
      rc.request().endHandler(endRes -> {
        parseParams(rc, body, paramList, paramArray, pathParams, okapiHeaders, useRoutingContext);
        if (rc.response().ended()) {
          return;
        }
        try {
          invoke(method, paramArray, instance, rc, useRoutingContext[0], okapiHeaders, new StreamStatus(), v -> {
            withRequestId(rc, () -> LogUtil.formatLogMessage(method.getName(), "start", " invoking " + method.getName()));
            sendResponse(rc, v, start, tenantId[0]);
          });
        } catch (Exception e1) {
          withRequestId(rc, () -> LOGGER.error(e1.getMessage(), e1));
          rc.response().end();
        }
      });
    }
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

  /**
   * look for the boundary and return just the multipart/form-data multipart/form-data boundary=----WebKitFormBoundaryP8wZiNAoFszXOXEt if
   * boundary doesnt exist that return original string
   */
  static String removeBoundry(String contenttype) {
    int idx = contenttype.indexOf("boundary");
    if (idx != -1) {
      return contenttype.substring(0, idx - 1);
    }
    return contenttype;
  }

  /**
   * check accept and content-type headers if no - set the request asa not valid and return error to user
   */
  private static void checkAcceptContentType(JsonArray produces, JsonArray consumes, RoutingContext rc) {
    /*
     * NOTE that the content type and accept headers will accept a partial match - for example: if the raml indicates a text/plain and an
     * application/json content-type and only one is passed - it will accept it
     */
    // check allowed content types in the raml for this resource + method
    HttpServerRequest request = rc.request();
    if (consumes != null) {
      // get the content type passed in the request
      // if this was left out by the client they must add for request to return
      // clean up simple stuff from the clients header - trim the string and remove ';' in case
      // it was put there as a suffix
      String contentType = StringUtils.defaultString(request.getHeader(CONTENT_TYPE), DEFAULT_CONTENT_TYPE)
          .replaceFirst(";.*", "").trim();
      if (!consumes.contains(removeBoundry(contentType))) {
        endRequestWithError(rc, 400, true, MESSAGES.getMessage("en", MessageConsts.ContentTypeError, consumes, contentType));
      }
    }

    // type of data expected to be returned by the server
    if (produces != null) {
      String accept = StringUtils.defaultString(request.getHeader("Accept"), "*/*");
      if (acceptCheck(produces, accept) == null) {
        // use contains because multiple values may be passed here
        // for example json/application; text/plain mismatch of content type found
        endRequestWithError(rc, 400, true, MESSAGES.getMessage("en", MessageConsts.AcceptHeaderError, produces, accept));
      }
    }
  }

  static Future<Void> populateRoutes(Router router) {
    JsonObject jObjClasses;
    try {
      jObjClasses = AnnotationGrabber.generateMappings(null);
    } catch (IOException e) {
      LOGGER.info(e.getMessage(), e);
      return Future.failedFuture(e);
    }
    for (String classURL : jObjClasses.fieldNames()) {
      JsonObject ret = jObjClasses.getJsonObject(classURL);
      String iClazz = ret.getString(AnnotationGrabber.CLASS_NAME);
      try {
        Class<?> aClass = InterfaceToImpl.convert2Impl(
            DomainModelConsts.PACKAGE_OF_IMPLEMENTATIONS, iClazz, false).get(0);
        // there is an implementation
        for (String classPaths : ret.fieldNames()) {
          Object value = ret.getValue(classPaths);
          if (value instanceof JsonArray) {
            JsonArray methodsForPath = (JsonArray) value;
            for (int i = 0; i < methodsForPath.size(); i++) {
              JsonObject methodInfo = methodsForPath.getJsonObject(i);
              String function = methodInfo.getString(AnnotationGrabber.FUNCTION_NAME);
              String httpMethodS = methodInfo.getString(AnnotationGrabber.HTTP_METHOD);
              httpMethodS = httpMethodS.substring(httpMethodS.lastIndexOf('.') + 1);
              HttpMethod httpMethod = HttpMethod.valueOf(httpMethodS);
              String regex = methodInfo.getString(AnnotationGrabber.REGEX_URL);
              String ramlPath = methodInfo.getString(AnnotationGrabber.METHOD_URL);

              Method[] classMethods = aClass.getMethods();
              for (Method classMethod : classMethods) {
                if (classMethod.getName().equals(function)) {
                  LOGGER.info("Adding route {} {} -> {}", httpMethod::name, () -> ramlPath, () -> function);
                  router.routeWithRegex(httpMethod, regex).handler(ctx -> handleRequest(ctx, aClass,
                      methodInfo, classMethod, Pattern.compile(regex)));
                }
              }
            }
          }
        }
      } catch (IOException e) {
        LOGGER.warn(e.getMessage(), e);
        return Future.failedFuture(e);
      } catch (ClassNotFoundException e) {
        LOGGER.info("Looks like {} is not implemented", iClazz);
      }
    }
    return Future.succeededFuture();
  }

  static class StreamStatus {
    private int status = -1;

    public int getStatus() {
      return status;
    }
    public void setStatus(int status) {
      this.status = status;
    }
  }

}
