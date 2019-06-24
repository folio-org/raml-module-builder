package org.folio.rest.persist;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.io.IOException;

import javax.ws.rs.core.Response;

import org.folio.rest.tools.utils.ObjectMapperTool;
import org.folio.rest.tools.utils.OutStream;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.util.UuidUtil;
import org.folio.cql2pgjson.CQL2PgJSON;
import org.folio.cql2pgjson.exception.FieldException;
import org.folio.cql2pgjson.exception.QueryValidationException;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.jaxrs.resource.support.ResponseDelegate;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.ResultSet;

import org.z3950.zing.cql.CQLDefaultNodeVisitor;
import org.z3950.zing.cql.CQLNode;
import org.z3950.zing.cql.CQLParseException;
import org.z3950.zing.cql.CQLParser;
import org.z3950.zing.cql.CQLSortNode;
import org.z3950.zing.cql.Modifier;
import org.z3950.zing.cql.ModifierSet;

import com.fasterxml.jackson.databind.ObjectMapper;

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
  private static final String INVALID_UUID = "Invalid UUID format of id, should be "
      + "xxxxxxxx-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx where M is 1-5 and N is 8, 9, a, b, A or B and "
      + "x is 0-9, a-f or A-F.";
  /** This is the name of the column used by all modules to store actual data */
  private static final String JSON_COLUMN = "jsonb";
  /** mapper between JSON and Java instance (POJO) */
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperTool.getMapper();
  /** Number of records to read from the sort index in getWithOptimizedSql and generateOptimizedSql method */
  private static int optimizedSqlSize = 10000;

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
   *               respond204(), respond400WithTextPlain(Object), respond404WithTextPlain(Object),
   *               respond500WithTextPlain(Object).
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
      Method respond400 = clazz.getMethod(RESPOND_400_WITH_TEXT_PLAIN, Object.class);
      Method respond404 = clazz.getMethod(RESPOND_404_WITH_TEXT_PLAIN, Object.class);
      if (! UuidUtil.isUuid(id)) {
        asyncResultHandler.handle(response(INVALID_UUID, respond400, respond500));
        return;
      }
      PostgresClient postgresClient = PgUtil.postgresClient(vertxContext, okapiHeaders);
      postgresClient.delete(table, id, reply -> {
        if (reply.failed()) {
          if (PgExceptionUtil.isForeignKeyViolation(reply.cause())) {
            String message = "Cannot delete record " + id + " in table " + table +
                ", it is still in use in table " + PgExceptionUtil.get(reply.cause(), 't');
            asyncResultHandler.handle(response(message, respond400, respond500));
            return;
          }
          asyncResultHandler.handle(response(reply.cause().getMessage(), respond500, respond500));
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
   * Return the first method whose name starts with <code>set</code> and that takes a List as parameter,
   * for example {@code setUser(List<User>)}.
   * @param collectionClass  where to search for the method
   * @return the method
   * @throws NoSuchMethodException if not found
   */
  static <C> Method getListSetter(Class<C> collectionClass) throws NoSuchMethodException {
    for (Method method : collectionClass.getMethods()) {
      Class<?> [] parameterTypes = method.getParameterTypes();

      if (method.getName().startsWith("set")
          && parameterTypes.length == 1
          && parameterTypes[0].equals(List.class)) {
        return method;
      }
    }

    throw new NoSuchMethodException(collectionClass.getName() + " must have a set...(java.util.List<>) method.");
  }

  private static <T, C> C collection(Class<C> collectionClazz, List<T> list, int totalRecords)
      throws ReflectiveOperationException {

    Method setList = getListSetter(collectionClazz);
    Method setTotalRecords = collectionClazz.getMethod("setTotalRecords", Integer.class);
    C collection = collectionClazz.newInstance();
    setList.invoke(collection, list);
    setTotalRecords.invoke(collection, totalRecords);
    return collection;
  }
  /**
   * Get records by CQL.
   * @param table  the table that contains the records
   * @param clazz  the class of the record type T
   * @param collectionClazz  the class of the collection type C containing records of type T
   * @param cql  the CQL query for filtering and sorting the records
   * @param okapiHeaders  http headers provided by okapi
   * @param vertxContext  the current context
   * @param responseDelegateClass  the ResponseDelegate class generated as defined by the RAML file,
   *    must have these methods: respond200(C), respond400WithTextPlain(Object), respond500WithTextPlain(Object).
   * @param asyncResultHandler  where to return the result created by the responseDelegateClass
   */
  public static <T, C> void get(String table, Class<T> clazz, Class<C> collectionClazz,
      String cql, int offset, int limit,
      Map<String, String> okapiHeaders, Context vertxContext,
      Class<? extends ResponseDelegate> responseDelegateClass,
      Handler<AsyncResult<Response>> asyncResultHandler) {

    final Method respond500;
    final Method respond400;
    try {
      respond500 = responseDelegateClass.getMethod(RESPOND_500_WITH_TEXT_PLAIN, Object.class);
      respond400 = responseDelegateClass.getMethod(RESPOND_400_WITH_TEXT_PLAIN, Object.class);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      asyncResultHandler.handle(response(e.getMessage(), null, null));
      return;
    }

    try {
      CQL2PgJSON cql2pgJson = new CQL2PgJSON(JSON_COLUMN);
      CQLWrapper cqlWrapper = new CQLWrapper(cql2pgJson, cql, limit, offset);
      PreparedCQL preparedCql = new PreparedCQL(table, cqlWrapper, okapiHeaders);
      get(preparedCql, clazz, collectionClazz, okapiHeaders, vertxContext, responseDelegateClass, asyncResultHandler);
    } catch (FieldException e) {
      logger.error(e.getMessage(), e);
      asyncResultHandler.handle(response(e.getMessage(), respond400, respond500));
    }
  }

  static <T, C> void get(PreparedCQL preparedCql, Class<T> clazz, Class<C> collectionClazz,
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
      Method respond200 = responseDelegateClass.getMethod(RESPOND_200_WITH_APPLICATION_JSON, collectionClazz);
      Method respond400 = responseDelegateClass.getMethod(RESPOND_400_WITH_TEXT_PLAIN, Object.class);
      PostgresClient postgresClient = PgUtil.postgresClient(vertxContext, okapiHeaders);
      postgresClient.get(preparedCql.getTableName(), clazz, preparedCql.getCqlWrapper(), true, reply -> {
        try {
          if (reply.failed()) {
            String message = PgExceptionUtil.badRequestMessage(reply.cause());
            if (message == null) {
              message = reply.cause().getMessage();
            }
            logger.error(message, reply.cause());
            asyncResultHandler.handle(response(message, respond400, respond500));
            return;
          }
          List<T> list = reply.result().getResults();
          C collection = collection(collectionClazz, list, reply.result().getResultInfo().getTotalRecords());
          asyncResultHandler.handle(response(collection, respond200, respond500));
        } catch (Exception e) {
          logger.error(e.getMessage(), e);
          asyncResultHandler.handle(response(e.getMessage(), respond500, respond500));
        }
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
   *    must have these methods: respond200(T), respond404WithTextPlain(Object), respond500WithTextPlain(Object).
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
      if (! UuidUtil.isUuid(id)) {
        asyncResultHandler.handle(response(INVALID_UUID, respond404, respond500));
        return;
      }
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
   * Return the method respond201WithApplicationJson(entity, headers) where the type of entity
   * is assignable from entityClass and the type of headers is assignable from headersFor201Class.
   *
   * <p>Depending on the .raml file entity is either of type Object or of the POJO type (for example of type User).
   *
   * @throws NoSuchMethodException if not found
   */
  private static <T> Method getResponse201Method(Class<? extends ResponseDelegate> clazz, Class<T> entityClass,
      Class<?> headersFor201Class) throws NoSuchMethodException {

    for (Method method : clazz.getMethods()) {
      if (! method.getName().equals(RESPOND_201_WITH_APPLICATION_JSON)) {
        continue;
      }
      Class<?> [] parameterType = method.getParameterTypes();
      if (parameterType.length == 2
          && parameterType[0].isAssignableFrom(entityClass)
          && parameterType[1].isAssignableFrom(headersFor201Class)) {
        return method;
      }
    }
    throw new NoSuchMethodException(RESPOND_201_WITH_APPLICATION_JSON
        + "(" + entityClass.getName() + ", " + headersFor201Class.getName() + ") not found in "
        + clazz.getCanonicalName());
  }

  /**
   * Post entity to table.
   *
   * <p>Create a random UUID for id if entity doesn't contain one.
   *
   * <p>All exceptions are caught and reported via the asyncResultHandler.
   *
   * @param table  table name
   * @param entity  the entity to post. If the id field is missing or null it is set to a random UUID.
   * @param okapiHeaders  http headers provided by okapi
   * @param vertxContext  the current context
   * @param clazz  the ResponseDelegate class generated as defined by the RAML file, must have these methods:
   *               headersFor201(), respond201WithApplicationJson(T, HeadersFor201),
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
      Method respond201 = getResponse201Method(clazz, entity.getClass(), headersFor201Class);
      Method respond400 = clazz.getMethod(RESPOND_400_WITH_TEXT_PLAIN, Object.class);

      String id = initId(entity);
      if (! UuidUtil.isUuid(id)) {
        asyncResultHandler.handle(response(INVALID_UUID, respond400, respond500));
        return;
      }
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
   *               respond204(), respond400WithTextPlain(Object), respond404WithTextPlain(Object),
   *               respond500WithTextPlain(Object).
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
      if (! UuidUtil.isUuid(id)) {
        asyncResultHandler.handle(response(INVALID_UUID, respond400, respond500));
        return;
      }
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
   * Return the sort node from the sortBy clause of the cql query, or null if no
   * sortBy clause exists or cql is invalid.
   * @param cql  the CQL query to parse
   * @return sort node, or null
   */
  static CQLSortNode getSortNode(String cql) {
    try {
      CQLParser parser = new CQLParser();
      CQLNode node = parser.parse(cql);
      return getSortNode(node);
    } catch (IOException|CQLParseException|NullPointerException e) {
      return null;
    }
  }

  private static CQLSortNode getSortNode(CQLNode node) {
    CqlSortNodeVisitor visitor = new CqlSortNodeVisitor();
    node.traverse(visitor);
    return visitor.sortNode;
  }

  private static class CqlSortNodeVisitor extends CQLDefaultNodeVisitor {
    CQLSortNode sortNode = null;

    @Override
    public void onSortNode(CQLSortNode cqlSortNode) {
      sortNode = cqlSortNode;
    }
  }

  private static String getAscDesc(ModifierSet modifierSet) {
    String ascDesc = "";
    for (Modifier modifier : modifierSet.getModifiers()) {
      switch (modifier.getType()) {
      case "sort.ascending":
        ascDesc = "ASC";
        break;
      case "sort.descending":
        ascDesc = "DESC";
        break;
      default:
        // ignore
      }
    }
    return ascDesc;
  }

  /**
   * Return a PostgresClient.
   * @param vertxContext  Where to get a Vertx from.
   * @param okapiHeaders  Where to get the tenantId from.
   * @return the PostgresClient for the vertx and the tenantId
   */
  public static PostgresClient postgresClient(Context vertxContext, Map<String, String> okapiHeaders) {
    String tenantId = TenantTool.tenantId(okapiHeaders);
    if (PostgresClient.DEFAULT_SCHEMA.equals(tenantId)) {
      return PostgresClient.getInstance(vertxContext.owner());
    }
    return PostgresClient.getInstance(vertxContext.owner(), TenantTool.tenantId(okapiHeaders));
  }

  /** Number of records to read from the sort index in getWithOptimizedSql method */
  public static int getOptimizedSqlSize() {
    return optimizedSqlSize;
  }

  /**
   * Set the number of records the getWithOptimizedSql methode uses from the sort index.
   * @param size the new size
   */
  public static void setOptimizedSqlSize(int size) {
    optimizedSqlSize = size;
  }

  /**
   * Run the cql query using optimized SQL (if possible) or standard SQL.
   * <p>
   * PostgreSQL has no statistics about a field within a JSONB resulting in bad performance.
   * <p>
   * This method requires both a b-tree index and a full text index for the field used for sorting.
   * <p>
   * This method starts a full table scan until getOptimizedSqlSize() records have been scanned.
   * Then it assumes that there are only a few result records and uses the full text match.
   * If the requested number of records have been found it stops immediately.
   *
   * @param table
   * @param clazz
   * @param cql
   * @param okapiHeaders
   * @param vertxContext
   * @param responseDelegateClass
   * @param asyncResultHandler
   */
  public static <T, C> void getWithOptimizedSql(String table, Class<T> clazz, Class<C> collectionClazz,
      String sortField, String cql, int offset, int limit,
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

    final Method respond200;
    final Method respond400;
    try {
      respond200 = responseDelegateClass.getMethod(RESPOND_200_WITH_APPLICATION_JSON, collectionClazz);
      respond400 = responseDelegateClass.getMethod(RESPOND_400_WITH_TEXT_PLAIN, Object.class);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      asyncResultHandler.handle(response(e.getMessage(), respond500, respond500));
      return;
    }

    try {
      CQL2PgJSON cql2pgJson = new CQL2PgJSON(JSON_COLUMN);
      CQLWrapper cqlWrapper = new CQLWrapper(cql2pgJson, cql, limit, offset);
      PreparedCQL preparedCql = new PreparedCQL(table, cqlWrapper, okapiHeaders);
      String sql = generateOptimizedSql(sortField, preparedCql, offset, limit);
      if (sql == null) {
        // the cql is not suitable for optimization, generate simple sql
        get(preparedCql, clazz, collectionClazz,
            okapiHeaders, vertxContext, responseDelegateClass, asyncResultHandler);
        return;
      }

      logger.info("Optimized SQL generated. Source CQL: " + cql);

      PostgresClient postgresClient = postgresClient(vertxContext, okapiHeaders);
      postgresClient.select(sql, reply -> {
        try {
          if (reply.failed()) {
            Throwable cause = reply.cause();
            logger.error("Optimized SQL failed: " + cause.getMessage() + ": " + sql, cause);
            asyncResultHandler.handle(response(cause.getMessage(), respond500, respond500));
            return;
          }
          C collection = collection(clazz, collectionClazz, reply.result(), limit);
          asyncResultHandler.handle(response(collection, respond200, respond500));
        } catch (Exception e) {
          logger.error(e.getMessage(), e);
          asyncResultHandler.handle(response(e.getMessage(), respond500, respond500));
          return;
        }
      });
    } catch (FieldException | QueryValidationException e) {
      logger.error(e.getMessage(), e);
      asyncResultHandler.handle(response(e.getMessage(), respond400, respond500));
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      asyncResultHandler.handle(response(e.getMessage(), respond500, respond500));
    }
  }

  private static <T, C> C collection(Class<T> clazz, Class<C> collectionClazz, ResultSet resultSet, int limit)
      throws ReflectiveOperationException, IOException {

    List<JsonObject> jsonList = resultSet.getRows();
    List<T> recordList = new ArrayList<>(jsonList.size());
    int totalRecords = 0;
    for (JsonObject object : jsonList) {
      String jsonb = object.getString(JSON_COLUMN);
      recordList.add(OBJECT_MAPPER.readValue(jsonb, clazz));
      totalRecords = object.getInteger("count");
    }

    // full table scan was stopped without total records calculation.
    if (totalRecords == 0 && jsonList.size() == limit) {
      totalRecords = 999999999;  // unknown total
    }

    return collection(collectionClazz, recordList, totalRecords);
  }

  /**
   * Generate optimized sql given a specific cql query, tenant, index column name hint and configurable size to hinge the optimization on.
   *
   * @param column the column that has an index to be used for sorting
   * @param preparedCql the cql query
   * @param tenantId the tenant used to generate schema location
   * @param offset start index of objects to return
   * @param limit max number of objects to return
   * @param size the number of rows that determines which method will be used to generate the ultimate result
   * @throws QueryValidationException
   * @return the generated SQL string, or null if the CQL query is not suitable for optimization.
   */
  static String generateOptimizedSql(String column, PreparedCQL preparedCql,
      int offset, int limit) throws QueryValidationException {

    String cql = preparedCql.getCqlWrapper().getQuery();
    CQLSortNode cqlSortNode = getSortNode(cql);
    if (cqlSortNode == null) {
      return null;
    }
    List<ModifierSet> sortIndexes = cqlSortNode.getSortIndexes();
    if (sortIndexes.size() != 1) {
      return null;
    }
    ModifierSet modifierSet = sortIndexes.get(0);
    if (! modifierSet.getBase().equals(column)) {
      return null;
    }
    String ascDesc = getAscDesc(modifierSet);
    cql = cqlSortNode.getSubtree().toCQL();
    String lessGreater = "";
    if (ascDesc.equals("DESC")) {
      lessGreater = ">" ;
    } else {
      lessGreater = "<";
    }
    String tableName = preparedCql.getFullTableName();
    String where = preparedCql.getCqlWrapper().getField().toSql(cql).getWhere();
    // If there are many matches use a full table scan in title sort order
    // using the title index, but stop this scan after OPTIMIZED_SQL_SIZE index entries.
    // Otherwise use full text matching because there are only a few matches.
    //
    // "headrecords" are the matching records found within the first OPTIMIZED_SQL_SIZE records
    // by stopping at the title from "OFFSET OPTIMIZED_SQL_SIZE LIMIT 1".
    // If "headrecords" are enough to return the requested "LIMIT" number of records we are done.
    // Otherwise use the full text index to create "allrecords" with all matching
    // records and do sorting and LIMIT afterwards.
    String sql =
        " WITH "
      + " headrecords AS ("
      + "   SELECT jsonb, lower(f_unaccent(jsonb->>'" + column + "')) AS title FROM " + tableName
      + "   WHERE (" + where + ")"
      + "     AND lower(f_unaccent(jsonb->>'" + column + "'))" + lessGreater
      + "             ( SELECT lower(f_unaccent(jsonb->>'" + column + "'))"
      + "               FROM " + tableName
      + "               ORDER BY lower(f_unaccent(jsonb->>'" + column + "')) " + ascDesc
      + "               OFFSET " + optimizedSqlSize + " LIMIT 1"
      + "             )"
      + "   ORDER BY lower(f_unaccent(jsonb->>'" + column + "')) " + ascDesc
      + "   LIMIT " + limit + " OFFSET " + offset
      + " ), "
      + " allrecords AS ("
      + "   SELECT jsonb, lower(f_unaccent(jsonb->>'" + column + "')) AS title FROM " + tableName
      + "   WHERE (" + where + ")"
      + "     AND (SELECT COUNT(*) FROM headrecords) < " + limit
      + " )"
      + " SELECT jsonb, title,  0                                 AS count"
      + "   FROM headrecords"
      + "   WHERE (SELECT COUNT(*) FROM headrecords) >= " + limit
      + " UNION"
      + " (SELECT jsonb, title, (SELECT COUNT(*) FROM allrecords) AS count"
      + "   FROM allrecords"
      + "   ORDER BY title " + ascDesc
      + "   LIMIT " + limit + " OFFSET " + offset
      + " )"
      + " ORDER BY title " + ascDesc;

    logger.info("optimized SQL generated from CQL: " + sql);
    return sql;
  }

  static class PreparedCQL {
    private final String tableName;
    private final String fullTableName;
    private final CQLWrapper cqlWrapper;

    public PreparedCQL(String tableName, CQLWrapper cqlWrapper, Map<String, String> okapiHeaders) {
      String tenantId = TenantTool.tenantId(okapiHeaders);
      this.tableName = tableName;
      this.fullTableName = PostgresClient.convertToPsqlStandard(tenantId) + "." + tableName;
      this.cqlWrapper = cqlWrapper;
    }

    public String getTableName() {
      return tableName;
    }

    /** @return full table name including schema, for example tenant_mymodule.users */
    public String getFullTableName() {
      return fullTableName;
    }

    public CQLWrapper getCqlWrapper() {
      return cqlWrapper;
    }
  }
}
