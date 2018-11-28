package org.folio.rest.persist;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.folio.rest.jaxrs.resource.support.ResponseDelegate;
import org.folio.rest.tools.utils.OutStream;
import org.folio.rest.tools.utils.TenantTool;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Helper methods for using PostgresClient.
 */
public final class PgUtil {
  private static final Logger logger = LoggerFactory.getLogger(PgUtil.class);

  private static final String RESPOND_200_WITH_APPLICATION_JSON = "respond200WithApplicationJson";
  private static final String RESPOND_201_WITH_APPLICATION_JSON = "respond201WithApplicationJson";
  private static final String RESPOND_204                       = "respond204";
  private static final String RESPOND_400_WITH_TEXT_PLAIN       = "respond400WithTextPlain";
  private static final String RESPOND_404_WITH_TEXT_PLAIN       = "respond404WithTextPlain";
  private static final String RESPOND_500_WITH_TEXT_PLAIN       = "respond500WithTextPlain";
  private static final String NOT_FOUND = "Not found";

  private PgUtil() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  /**
   * Create a Response using okMethod(entity, headersMethod().withLocationMethod(location)).
   * On exception create a Response using failResponseMethod.
   *
   * <p>All exceptions are caught and reported via the returned Future.
   */
  static <T> Future<Response> response(T entity, String location,
      Method headersMethod, Method withLocationMethod,
      Method okResponseMethod, Method failResponseMethod) {
    try {
      OutStream stream = new OutStream();
      stream.setData(entity);
      Object headers = headersMethod.invoke(null);
      withLocationMethod.invoke(headers, location);
      Response response = (Response) okResponseMethod.invoke(null, entity, headers);
      return Future.succeededFuture(response);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      try {
        Response response = (Response) failResponseMethod.invoke(null, e.getMessage());
        return Future.succeededFuture(response);
      } catch (Exception innerException) {
        logger.error(innerException.getMessage(), innerException);
        return Future.failedFuture(innerException);
      }
    }
  }

  /**
   * Message of t or t.getCause(). Useful for InvocationTargetException.
   * @return  The first non-null value of these: t.getMessage(), t.getCause().getMessage().
   *          Null if both are null.
   */
  static String message(Throwable t) {
    String message = t.getMessage();
    if (message != null) {
      return message;
    }
    Throwable inner = t.getCause();
    if (inner == null) {
      return null;
    }
    return inner.getMessage();
  }

  /**
   * Create a Response for the Exception e using failResponseMethod.
   */
  private static Future<Response> response(Exception e, Method failResponseMethod) {
    String message = message(e);
    logger.error(message, e);
    try {
      if (failResponseMethod == null) {
        return Future.failedFuture(e);
      }
      Response response = (Response) failResponseMethod.invoke(null, message);
      return Future.succeededFuture(response);
    } catch (Exception innerException) {
      message = message(innerException);
      logger.error(message, innerException);
      return Future.failedFuture(innerException);
    }
  }

  /**
   * Create a Response using valueMethod(T).
   * On exception create a Response using failResponseMethod(String exceptionMessage).
   * If that also throws an exception create a failed future.
   *
   * <p>All exceptions are caught and reported via the returned Future.
   */
  static <T> Future<Response> response(T value, Method valueMethod, Method failResponseMethod) {
    try {
      if (valueMethod == null) {
        throw new NullPointerException("valueMethod must not be null (" + value + ")");
      }
      // this null check is redundant but avoids several sonarlint warnings
      if (failResponseMethod == null) {
        throw new NullPointerException("failResponseMethod must not be null (" + value + ")");
      }
      Response response = (Response) valueMethod.invoke(null, value);
      return Future.succeededFuture(response);
    } catch (Exception e) {
      return response(e, failResponseMethod);
    }
  }

  /**
   * Return a Response using valueMethod and a PgExceptionUtil message of throwable. If that is null
   * use failResponseMethod of throwable.getMessage().
   * @param throwable  where to get the text from
   * @param valueMethod  how to report the PgException
   * @param failResponseMethod  how to report other Exceptions/Throwables
   */
  static Future<Response> response(Throwable throwable, Method valueMethod, Method failResponseMethod) {
    String message = PgExceptionUtil.badRequestMessage(throwable);
    if (message != null) {
      return response(message,                valueMethod,        failResponseMethod);
    } else {
      return response(throwable.getMessage(), failResponseMethod, failResponseMethod);
    }
  }

  /**
   * Return a Response using responseMethod() wrapped in a succeeded future.
   *
   * <p>On exception create a Response using failResponseMethod(String exceptionMessage)
   * wrapped in a succeeded future.
   *
   * <p>If that also throws an exception create a failed future.
   *
   * <p>All exceptions are caught and reported via the returned Future.
   */
  static Future<Response> response(Method responseMethod, Method failResponseMethod) {
    try {
      // the null check is redundant but avoids several sonarlint warnings
      if (responseMethod == null) {
        throw new NullPointerException("responseMethod must not be null");
      }
      if (failResponseMethod == null) {
        throw new NullPointerException("failResponseMethod must not be null");
      }
      Response response = (Response) responseMethod.invoke(null);
      return Future.succeededFuture(response);
    } catch (Exception e) {
      return response(e, failResponseMethod);
    }
  }

  /**
   * Delete a record from a table.
   *
   * <p>All exceptions are caught and reported via the asyncResultHandler.
   *
   * @param table  where to delete
   * @param id  the primary key of the record to delete
   * @param okapiHeaders  http headers provided by okapi
   * @param vertxContext  the current context
   * @param clazz  the ResponseDelegate class created from the RAML file with these methods:
   *               respond204(), respond500WithTextPlain(Object).
   * @param asyncResultHandler  where to return the result created by clazz
   */
  public static void deleteById(String table, String id,
      Map<String, String> okapiHeaders, Context vertxContext,
      Class<? extends ResponseDelegate> clazz,
      Handler<AsyncResult<Response>> asyncResultHandler) {

    final Method respond500;
    try {
      respond500 = clazz.getMethod(RESPOND_500_WITH_TEXT_PLAIN, Object.class);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      asyncResultHandler.handle(response(e.getMessage(), null, null));
      return;
    }

    try {
      Method respond204 = clazz.getMethod(RESPOND_204);
      Method respond404 = clazz.getMethod(RESPOND_404_WITH_TEXT_PLAIN, Object.class);
      PostgresClient postgresClient = PgUtil.postgresClient(vertxContext, okapiHeaders);
      postgresClient.delete(table, id, reply -> {
        if (reply.failed()) {
          String message = PgExceptionUtil.badRequestMessage(reply.cause());
          if (message == null) {
            message = reply.cause().getMessage();
          }
          asyncResultHandler.handle(response(message, respond500, respond500));
          return;
        }
        int deleted = reply.result().getUpdated();
        if (deleted == 0) {
          asyncResultHandler.handle(response(NOT_FOUND, respond404, respond500));
          return;
        }
        if (deleted != 1) {
          String message = "Deleted " + deleted + " records in " + table + " for id: " + id;
          logger.fatal(message);
          asyncResultHandler.handle(response(message, respond500, respond500));
          return;
        }
        asyncResultHandler.handle(response(respond204, respond500));
      });
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      asyncResultHandler.handle(response(e.getMessage(), respond500, respond500));
    }
  }

  /**
   * Get a record by id.
   *
   * <p>All exceptions are caught and reported via the asyncResultHandler.
   *
   * @param table  the table that contains the record
   * @param clazz  the class of the response type T
   * @param id  the primary key of the record to get
   * @param okapiHeaders  http headers provided by okapi
   * @param vertxContext  the current context
   * @param responseDelegateClass  the ResponseDelegate class generated as defined by the RAML file,
   *    must have these methods: respond200(T), respond500WithTextPlain(Object).
   * @param asyncResultHandler  where to return the result created by the responseDelegateClass
   */
  public static <T> void getById(String table, Class<T> clazz, String id,
      Map<String, String> okapiHeaders, Context vertxContext,
      Class<? extends ResponseDelegate> responseDelegateClass,
      Handler<AsyncResult<Response>> asyncResultHandler) {

    final Method respond500;
    try {
      respond500 = responseDelegateClass.getMethod(RESPOND_500_WITH_TEXT_PLAIN, Object.class);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      asyncResultHandler.handle(response(e.getMessage(), null, null));
      return;
    }
    try {
      Method respond200 = responseDelegateClass.getMethod(RESPOND_200_WITH_APPLICATION_JSON, clazz);
      Method respond404 = responseDelegateClass.getMethod(RESPOND_404_WITH_TEXT_PLAIN, Object.class);
      PostgresClient postgresClient = postgresClient(vertxContext, okapiHeaders);
      postgresClient.getById(table, id, clazz, reply -> {
        if (reply.failed()) {
          asyncResultHandler.handle(response(reply.cause().getMessage(), respond500, respond500));
          return;
        }
        if (reply.result() == null) {
          asyncResultHandler.handle(response(NOT_FOUND, respond404, respond500));
          return;
        }
        asyncResultHandler.handle(response(reply.result(), respond200, respond500));
      });
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      asyncResultHandler.handle(response(e.getMessage(), respond500, respond500));
    }
  }

  /**
   * Return entity's id.
   *
   * <p>Use reflection, the POJOs don't have a interface/superclass in common.
   */
  private static <T> Object getId(T entity) throws ReflectiveOperationException {
    return entity.getClass().getDeclaredMethod("getId").invoke(entity);
  }

  /**
   * Set entity's id.
   *
   * <p>Use reflection, the POJOs don't have a interface/superclass in common.
   * @param entity  where to set the id field
   * @param id  the new id value
   */
  private static <T> void setId(T entity, String id) throws ReflectiveOperationException {
    entity.getClass().getDeclaredMethod("setId", String.class).invoke(entity, id);
  }

  /**
   * If entity's id field is null then initialize it with a random UUID.
   * @param entity  entity with id field
   * @return the value of the id field at the end
   * @throws ReflectiveOperationException if entity.getId() or entity.setId(String) fails.
   */
  private static <T> String initId(T entity) throws ReflectiveOperationException {
    Object id = getId(entity);
    if (id != null) {
      return id.toString();
    }
    String idString = UUID.randomUUID().toString();
    setId(entity, idString);
    return idString;
  }

  /**
   * Post entity to table.
   *
   * <p>All exceptions are caught and reported via the asyncResultHandler.
   *
   * @param table  table name
   * @param entity  the entity to post. If the id field is missing or null it is set to a random UUID.
   * @param okapiHeaders  http headers provided by okapi
   * @param vertxContext  the current context
   * @param clazz  the ResponseDelegate class generated as defined by the RAML file, must have these methods:
   *               headersFor201(), respond201WithApplicationJson(Object, HeadersFor201),
   *               respond400WithTextPlain(Object), respond500WithTextPlain(Object).
   * @param asyncResultHandler  where to return the result created by clazz
   */
  @SuppressWarnings("squid:S1523")  // suppress "Dynamically executing code is security-sensitive"
                                    // we use only hard-coded names
  public static <T> void post(String table, T entity,
      Map<String, String> okapiHeaders, Context vertxContext,
      Class<? extends ResponseDelegate> clazz,
      Handler<AsyncResult<Response>> asyncResultHandler) {

    final Method respond500;

    try {
      respond500 = clazz.getMethod(RESPOND_500_WITH_TEXT_PLAIN, Object.class);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      asyncResultHandler.handle(Future.failedFuture(e));
      return;
    }

    try {
      Method headersFor201Method = clazz.getMethod("headersFor201");
      String headersFor201ClassName = clazz.getName() + "$HeadersFor201";
      Class<?> headersFor201Class = null;
      for (Class<?> declaredClass : clazz.getDeclaredClasses()) {
        if (declaredClass.getName().equals(headersFor201ClassName)) {
          headersFor201Class = declaredClass;
          break;
        }
      }
      if (headersFor201Class == null) {
        throw new ClassNotFoundException(headersFor201ClassName + " not found in " + clazz.getCanonicalName());
      }
      Method withLocation = headersFor201Class.getMethod("withLocation", String.class);
      Method respond201 = clazz.getMethod(RESPOND_201_WITH_APPLICATION_JSON, Object.class, headersFor201Class);
      Method respond400 = clazz.getMethod(RESPOND_400_WITH_TEXT_PLAIN, Object.class);

      String id = initId(entity);
      PostgresClient postgresClient = postgresClient(vertxContext, okapiHeaders);
      postgresClient.save(table, id, entity, reply -> {
        if (reply.failed()) {
          asyncResultHandler.handle(response(reply.cause(), respond400, respond500));
          return;
        }
        asyncResultHandler.handle(response(entity, reply.result(), headersFor201Method, withLocation,
            respond201, respond500));
      });
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      asyncResultHandler.handle(response(e.getMessage(), respond500, respond500));
    }
  }

  /**
   * Put entity to table.
   *
   * <p>All exceptions are caught and reported via the asyncResultHandler.
   *
   * @param table  table name
   * @param entity  the new entity to store. The id field is set to the id value.
   * @param id  the id value to use for entity
   * @param okapiHeaders  http headers provided by okapi
   * @param vertxContext  the current context
   * @param clazz  the ResponseDelegate class created from the RAML file with these methods:
   *               respond204(), respond400WithTextPlain(Object), respond500WithTextPlain(Object).
   * @param asyncResultHandler  where to return the result created by clazz
   */
  public static <T> void put(String table, T entity, String id,
      Map<String, String> okapiHeaders, Context vertxContext,
      Class<? extends ResponseDelegate> clazz,
      Handler<AsyncResult<Response>> asyncResultHandler) {

    final Method respond500;

    try {
      respond500 = clazz.getMethod(RESPOND_500_WITH_TEXT_PLAIN, Object.class);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      asyncResultHandler.handle(response(e.getMessage(), null, null));
      return;
    }

    try {
      Method respond204 = clazz.getMethod(RESPOND_204);
      Method respond400 = clazz.getMethod(RESPOND_400_WITH_TEXT_PLAIN, Object.class);
      Method respond404 = clazz.getMethod(RESPOND_404_WITH_TEXT_PLAIN, Object.class);
      setId(entity, id);
      PostgresClient postgresClient = postgresClient(vertxContext, okapiHeaders);
      postgresClient.update(table, entity, id, reply -> {
        if (reply.failed()) {
          asyncResultHandler.handle(response(reply.cause(), respond400, respond500));
          return;
        }
        int updated = reply.result().getUpdated();
        if (updated == 0) {
          asyncResultHandler.handle(response(NOT_FOUND, respond404, respond500));
          return;
        }
        if (updated != 1) {
          String message = "Updated " + updated + " records in " + table + " for id: " + id;
          logger.fatal(message);
          asyncResultHandler.handle(response(message, respond500, respond500));
          return;
        }
        asyncResultHandler.handle(response(respond204, respond500));
      });
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      asyncResultHandler.handle(response(e.getMessage(), respond500, respond500));
    }
  }

  /**
   * Return a PostgresClient.
   * @param vertxContext  Where to get a Vertx from.
   * @param okapiHeaders  Where to get the tenantId from.
   * @return the PostgresClient for the vertx and the tenantId
   */
  public static PostgresClient postgresClient(Context vertxContext, Map<String, String> okapiHeaders) {
    return PostgresClient.getInstance(vertxContext.owner(), TenantTool.tenantId(okapiHeaders));
  }
}
