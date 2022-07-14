package org.folio.rest.persist;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.RowStream;
import io.vertx.sqlclient.Tuple;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dbschema.util.SqlUtil;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.Criteria.UpdateSection;
import org.folio.rest.persist.PostgresClient.FunctionWithException;
import org.folio.rest.persist.PostgresClient.QueryHelper;
import org.folio.rest.persist.PostgresClient.TotaledResults;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.persist.facets.FacetField;
import org.folio.rest.persist.helpers.LocalRowSet;
import org.folio.rest.persist.interfaces.Results;
import org.folio.rest.tools.utils.MetadataUtil;

/**
 * A connection of a PostgresClient.
 */
public class Conn {

  private static final Logger log = LogManager.getLogger(Conn.class);

  private final PostgresClient postgresClient;
  private final PgConnection pgConnection;

  public Conn(PostgresClient postgresClient, PgConnection conn) {
    this.postgresClient = postgresClient;
    this.pgConnection = conn;
  }

  public PgConnection getPgConnection() {
    return pgConnection;
  }

  /**
   * A debug message showing the duration from startNanoTime until now.
   * @param descriptionKey  key for StatsTracker and text for the log entry
   * @param sql  additional text for the log entry
   * @param startNanoTime  start time as returned by System.nanoTime()
   */
  static String durationMsg(String descriptionKey, String sql, long startNanoTime) {
    long milliseconds = (System.nanoTime() - startNanoTime) / 1000000;
    return descriptionKey + " timer: " + sql + " took " + milliseconds + " ms";
  }

  /**
   * Get the jsonb by id.
   * @param lock  whether to use SELECT FOR UPDATE to lock the selected row
   * @param table  the table to search in
   * @param id  the value of the id field
   * @param function  how to convert the (String encoded) JSON
   * @return the jsonb after applying the function
   */
  <R> Future<R> getById(boolean lock, String table, String id,
      FunctionWithException<String, R, Exception> function) {

    try {
      String sql = "SELECT jsonb FROM " + postgresClient.getSchemaName() + "." + table
          + " WHERE id = $1" + (lock ? " FOR UPDATE" : "");
      return pgConnection
          .preparedQuery(sql)
          .execute(Tuple.of(UUID.fromString(id)))
          .map(rowSet -> {
            if (rowSet.size() == 0) {
              return null;
            }
            String entity = rowSet.iterator().next().getValue(0).toString();
            try {
              return function.apply(entity);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  /**
   * Get the jsonb by id and return it as a String.
   * @param table  the table to search in
   * @param id  the value of the id field
   * @return the JSON encoded as a String
   */
  public Future<String> getByIdAsString(String table, String id) {
    return getById(false, table, id, string -> string);
  }

  /**
   * Lock the row using {@code SELECT ... FOR UPDATE} and return jsonb as String.
   * @param table  the table to search in
   * @param id  the value of the id field
   * @return the JSON encoded as a String
   */
  public Future<String> getByIdAsStringForUpdate(String table, String id) {
    return getById(true, table, id, string -> string);
  }

  /**
   * Get the jsonb by id and return it as a JsonObject.
   * @param table  the table to search in
   * @param id  the value of the id field
   * @return the JSON encoded as a JsonObject
   */
  public Future<JsonObject> getById(String table, String id) {
    return getById(false, table, id, JsonObject::new);
  }

  /**
   * Lock the row using {@code SELECT ... FOR UPDATE} and return jsonb as a JsonObject.
   * @param table  the table to search in
   * @param id  the value of the id field
   * @return the JSON encoded as a JsonObject
   */
  public Future<JsonObject> getByIdForUpdate(String table, String id) {
    return getById(true, table, id, JsonObject::new);
  }

  /**
   * Get the jsonb by id and return it as a pojo of type T.
   * @param table  the table to search in
   * @param id  the value of the id field
   * @param clazz  the type of the pojo
   * @return the JSON converted into a T pojo.
   */
  public <T> Future<T> getById(String table, String id, Class<T> clazz) {
    return getById(false, table, id, json -> PostgresClient.MAPPER.readValue(json, clazz));
  }

  /**
   * Lock the row using {@code SELECT ... FOR UPDATE} and return jsonb as a pojo of type T.
   * @param table  the table to search in
   * @param id  the value of the id field
   * @param clazz  the type of the pojo
   * @return the JSON converted into a T pojo.
   */
  public <T> Future<T> getByIdForUpdate(String table, String id, Class<T> clazz) {
    return getById(true, table, id, json -> PostgresClient.MAPPER.readValue(json, clazz));
  }

  /**
   * Save entity in table. Return the id field (primary key), if id (primary key) and
   * the id of entity (jsonb field) are different you may need a trigger in the
   * database to sync them.
   *
   * @param table where to insert the entity record
   * @param id  the value for the id field (primary key); if null a new random UUID is created for it.
   * @param entity  the record to insert, either a POJO or a JsonArray, see convertEntity
   * @param returnId true to return the id of the inserted record, false to return an empty string
   * @param upsert whether to update if the record with that id already exists (INSERT or UPDATE)
   * @param convertEntity true if entity is a POJO, false if entity is a JsonArray
   * @return the final id of the id field after triggers have run
   */
  public Future<String> save(String table, String id, Object entity,
      boolean returnId, boolean upsert, boolean convertEntity) {

    try {
      long start = log.isDebugEnabled() ? System.nanoTime() : 0;
      String sql;
      if (upsert) {
        sql = (returnId ? "" : "SELECT '' FROM (")
            + "SELECT upsert('" + table + "', $1::uuid, " + (convertEntity ? "$2::jsonb" : "$2::text") + ")"
            + (returnId ? "" : ") x");
      } else {
        sql = "INSERT INTO " + postgresClient.getSchemaName() + "." + table
            + " (id, jsonb) VALUES ($1, " + (convertEntity ? "$2" : "$2::text") + ")"
            + " RETURNING " + (returnId ? "id" : "''");
      }
      return pgConnection.preparedQuery(sql).execute(Tuple.of(
          id == null ? UUID.randomUUID() : UUID.fromString(id),
          convertEntity ? PostgresClient.pojo2JsonObject(entity) : ((JsonArray)entity).getString(0)
      )).map(rowSet -> {
        log.debug(() -> durationMsg("save", table, start));
        return rowSet.iterator().next().getValue(0).toString();
      });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return Future.failedFuture(e);
    }
  }

  /**
   * Insert entity into table. Create a new id UUID if entity's id is null.
   * @param table database table (without schema)
   * @param entity a POJO (plain old java object)
   * @return the final id after applying triggers
   */
  public Future<String> save(String table, Object entity) {
    return save(table, /* id */ null, entity,
        /* returnId */ true, /* upsert */ false, /* convertEntity */ true);
  }

  /**
   * Insert entity into table.
   * @param table database table (without schema)
   * @param entity a POJO (plain old java object)
   * @param returnId true to return the id of the inserted record, false to return an empty string
   * @return the final id after applying triggers
   */
  public Future<String> save(String table, Object entity, boolean returnId) {
    return save(table, /* id */ null, entity,
        returnId, /* upsert */ false, /* convertEntity */ true);
  }

  /**
   * Insert entity into table.
   * @param table database table (without schema)
   * @param id primary key for the record, or null if one should be created
   * @param entity a POJO (plain old java object)
   * @return the final id after applying triggers
   */
  public Future<String> save(String table, String id, Object entity) {
    return save(table, id, entity,
        /* returnId */ true, /* upsert */ false, /* convertEntity */ true);
  }

  /**
   * Insert entity into table.
   * @param table database table (without schema)
   * @param id primary key for the record, or null if one should be created
   * @param entity a POJO (plain old java object)
   * @param returnId true to return the id of the inserted record, false to return an empty string
   * @return the final id after applying triggers or an empty string, see returnId
   */
  public Future<String> save(String table, String id, Object entity, boolean returnId) {
    return save(table, id, entity, returnId, /* upsert */ false, /* convertEntity */ true);
  }

  /**
   * Insert entity into table.
   * @param table database table (without schema)
   * @param id primary key for the record, or null if one should be created
   * @param entity a POJO (plain old java object)
   * @param returnId true to return the id of the inserted record, false to return an empty string
   * @param upsert whether to update if the record with that id already exists (INSERT or UPDATE)
   * @return the final id after applying triggers or an empty string, see returnId
   */
  public Future<String> save(String table, String id, Object entity, boolean returnId, boolean upsert) {
    return save(table, id, entity, returnId, upsert, /* convertEntity */ true);
  }

  /**
   * Insert entity into table, or update it if it already exists.
   * @param table database table (without schema)
   * @param id primary key for the record, or null if one should be created
   * @param entity a POJO (plain old java object)
   * @return the final id after applying triggers
   */
  public Future<String> upsert(String table, String id, Object entity) {
    return save(table, id, entity, /* returnId */ true, /* upsert */ true, /* convertEntity */ true);
  }

  /**
   * Insert or update.
   *
   * <p>Needed if upserting binary data as base64 where converting it to a json will corrupt the data
   * otherwise this function is not needed as the default is true
   * example:
   *     byte[] data = ......;
   *     JsonArray jsonArray = new JsonArray().add(data);
   *     .upsert(TABLE_NAME, id, jsonArray, false, replyHandler -> {

   * @param table database table (without schema)
   * @param id primary key for the record, or null if one should be created
   * @param entity either a POJO, or a JsonArray containing a byte[] element, see convertEntity
   * @param convertEntity true if entity is a POJO, false if entity is a JsonArray
   * @return the final id after applying triggers
   */
  public Future<String> upsert(String table, String id, Object entity, boolean convertEntity) {
    return save(table, id, entity, /* returnId */ true, /* upsert */ true, /* convertEntity */ convertEntity);
  }

  /**
   * Save entity in table and return the updated entity.
   *
   * @param table where to insert the entity record
   * @param id  the value for the id field (primary key); if null a new random UUID is created for it.
   * @param entity  the record to insert, a POJO
   * @return the entity after applying any database INSERT triggers
   */
  public <T> Future<T> saveAndReturnUpdatedEntity(String table, String id, T entity) {
    try {
      long start = log.isDebugEnabled() ? System.nanoTime() : 0;
      String sql = "INSERT INTO " + postgresClient.getSchemaName() + "." + table
          + " (id, jsonb) VALUES ($1, $2) RETURNING jsonb";
      return pgConnection.preparedQuery(sql).execute(Tuple.of(
          id == null ? UUID.randomUUID() : UUID.fromString(id),
          PostgresClient.pojo2JsonObject(entity)
      )).map(rowSet -> {
        log.debug(() -> durationMsg("save", table, start));
        String updatedEntityString = rowSet.iterator().next().getValue(0).toString();
        try {
          @SuppressWarnings("unchecked")
          T updatedEntity = (T) PostgresClient.MAPPER.readValue(updatedEntityString, entity.getClass());
          return updatedEntity;
        } catch (JsonProcessingException e) {
          throw new UncheckedIOException(e);
        }
      });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return Future.failedFuture(e);
    }
  }

  /**
   * Insert or upsert the entities into table.
   *
   * <p>A transaction must be open on this {@link #Conn} so that SELECT ... FOR UPDATE works.
   *
   * @param upsert  true for upsert, false for insert with fail on duplicate id
   * @param table  destination table to insert into
   * @param entities  each array element is a String with the content for the JSONB field of table; if id is missing a random id is generated
   * @return one result row per inserted row, containing the id field
   */
  Future<RowSet<Row>> saveBatch(boolean upsert, String table, JsonArray entities) {

    try {
      List<Tuple> list = new ArrayList<>();
      if (entities != null) {
        for (int i = 0; i < entities.size(); i++) {
          String json = entities.getString(i);
          JsonObject jsonObject = new JsonObject(json);
          String id = jsonObject.getString("id");
          list.add(Tuple.of(
              id == null ? UUID.randomUUID() : UUID.fromString(id),
              jsonObject));
        }
      }
      return saveBatchInternal(upsert, table, list);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return Future.failedFuture(e);
    }
  }

  private RowSet<Row> emptyRowSetOfId() {
    return new LocalRowSet(0).withColumns(Collections.singletonList("id"));
  }

  /**
   * A transaction must be open on this {@link #Conn} so that SELECT ... FOR UPDATE works.
   *
   * @throws Throwable the caller needs to catch and convert into a failed Future
   */
  private Future<RowSet<Row>> saveBatchInternal(boolean upsert, String table, List<Tuple> batch) {

    if (batch.isEmpty()) {
      // vertx-pg-client fails with "Can not execute batch query with 0 sets of batch parameters."
      return Future.succeededFuture(emptyRowSetOfId());
    }
    long start = log.isDebugEnabled() ? System.nanoTime() : 0;
    log.info("starting: saveBatch size=" + batch.size());

    StringBuilder selectForUpdate = new StringBuilder(batch.size() * 39 + 50 + table.length());
    selectForUpdate.append("SELECT 1 FROM ").append(table).append(" WHERE id in (");
    batch.forEach(tuple -> selectForUpdate.append('\'').append(tuple.getUUID(0)).append('\'').append(","));
    selectForUpdate.deleteCharAt(selectForUpdate.length() - 1);  // delete last comma
    selectForUpdate.append(") FOR UPDATE");

    String sql;
    if (upsert) {
      sql = "SELECT upsert('" + table + "', $1::uuid, $2::jsonb)";
    } else {
      sql = "INSERT INTO " + postgresClient.getSchemaName() + "." + table + " (id, jsonb) VALUES ($1, $2)"
        + " RETURNING id";
    }

    return pgConnection.query(selectForUpdate.toString()).execute()
        .compose(x -> pgConnection.preparedQuery(sql).executeBatch(batch))
        .map(rowSet -> {
          log.debug(() -> durationMsg("saveBatch", table, start));
          if (rowSet == null) {
            return emptyRowSetOfId();
          }
          return rowSet;
        })
        .onFailure(e -> {
          log.error("saveBatch size=" + batch.size() + " " + e.getMessage(), e);
          log.debug(() -> durationMsg("saveBatchFailed", table, start));
        });
  }

  /**
   * Insert the entities into table.
   *
   * <p>A transaction must be open on this {@link #Conn} so that SELECT ... FOR UPDATE works.
   *
   * @param table  destination table to insert into
   * @param entities  each array element is a String with the content for the JSONB field of table; if id is missing a random id is generated
   * @return one result row per inserted row, containing the id field
   */
  public Future<RowSet<Row>> saveBatch(String table, JsonArray entities) {
    return saveBatch(false, table, entities);
  }

  /**
   * Upsert the entities into table.
   *
   * <p>A transaction must be open on this {@link #Conn} so that SELECT ... FOR UPDATE works.
   *
   * @param table  destination table to insert into
   * @param entities  each array element is a String with the content for the JSONB field of table; if id is missing a random id is generated
   * @return one result row per inserted row, containing the id field
   */
  public Future<RowSet<Row>> upsertBatch(String table, JsonArray entities) {
    return saveBatch(true, table, entities);
  }

  /**
   * A transaction must be open on this {@link #Conn} so that SELECT ... FOR UPDATE works.
   */
  <T> Future<RowSet<Row>> saveBatch(boolean upsert, String table, List<T> entities) {

    try {
      if (entities == null || entities.isEmpty()) {
        return Future.succeededFuture(emptyRowSetOfId());
      }
      List<Tuple> batch = new ArrayList<>(entities.size());
      // We must use reflection, the POJOs don't have an interface/superclass in common.
      Method getIdMethod = entities.get(0).getClass().getDeclaredMethod("getId");
      for (Object entity : entities) {
        Object obj = getIdMethod.invoke(entity);
        UUID id = obj == null ? UUID.randomUUID() : UUID.fromString((String) obj);
        batch.add(Tuple.of(id, PostgresClient.pojo2JsonObject(entity)));
      }
      return saveBatchInternal(upsert, table, batch);
    } catch (Exception e) {
      log.error("saveBatch error " + e.getMessage(), e);
      return Future.failedFuture(e);
    }
  }

  /**
   * Save a list of POJOs.
   *
   * <p>POJOs are converted to a JSON String.
   *
   * <p>A random id is generated if POJO's id is null.
   *
   * <p>Call {@link MetadataUtil#populateMetadata(List, Map)} before if applicable.
   *
   * <p>A transaction must be open on this {@link #Conn} so that SELECT ... FOR UPDATE works.
   *
   * @param table  destination table to insert into
   * @param entities  each list element is a POJO
   * @return one result row per inserted row, containing the id field
   */
  public <T> Future<RowSet<Row>> saveBatch(String table, List<T> entities) {
    return saveBatch(false, table, entities);
  }

  /**
   * Upsert a list of POJOs.
   *
   * <p>POJOs are converted to a JSON String.
   *
   * <p>A random id is generated if POJO's id is null.
   *
   * <p>If a record with the id already exists it is updated (upsert).
   *
   * <p>Call {@link MetadataUtil#populateMetadata(List, Map)} before if applicable.
   *
   * <p>A transaction must be open on this {@link #Conn} so that SELECT ... FOR UPDATE works.
   *
   * @param table  destination table to insert into
   * @param entities  each list element is a POJO
   * @return one result row per inserted row, containing the id field
   */
  public <T> Future<RowSet<Row>> upsertBatch(String table, List<T> entities) {
    return saveBatch(true, table, entities);
  }

  private Future<RowSet<Row>> updateBatchInternal(String table, List<Tuple> batch) {
    long start = log.isDebugEnabled() ? System.nanoTime() : 0;
    log.info("starting: updateBatchInternal size=" + batch.size());
    String sql = "UPDATE " + postgresClient.getSchemaName() + "." + table
        + " SET jsonb = $1 WHERE id = ($1::jsonb->>'id')::uuid";
    return pgConnection.preparedQuery(sql).executeBatch(batch)
    .onSuccess(x -> log.debug(() -> durationMsg("updateBatch", table, start)))
    .onFailure(e -> {
      log.error("updateBatch size=" + batch.size() + ", " + e.getMessage(), e);
      log.debug(() -> durationMsg("updateBatchFailed", table, start));
    });
  }

  /**
   * Update the entities in the table , match using the id property.
   *
   * <p>A transaction must be open on this {@link #Conn} so that SELECT ... FOR UPDATE works.
   *
   * @param entities  each array element is a String with the content for the JSONB field of table
   * @return one {@link RowSet} per array element with {@link RowSet#rowCount()} information
   */
  public Future<RowSet<Row>> updateBatch(String table, JsonArray entities) {
    try {
      if (entities == null || entities.size() == 0) {
        return Future.succeededFuture();
      }
      List<Tuple> list = new ArrayList<>(entities.size());
      for (int i = 0; i < entities.size(); i++) {
        Object o = entities.getValue(i);
        list.add(Tuple.of(o instanceof JsonObject ? o : new JsonObject(o.toString())));
      }
      return updateBatchInternal(table, list);
    } catch (Throwable t) {
      log.error("updateBatch error " + t.getMessage(), t);
      return Future.failedFuture(t);
    }
  }

  /**
   * Update a list of POJOs.
   *
   * <p>POJOs are converted to a JSON String and matched using the id property.
   *
   * <p>Call {@link MetadataUtil#populateMetadata(List, Map)} before if applicable.
   *
   * <p>A transaction must be open on this {@link #Conn} so that SELECT ... FOR UPDATE works.
   *
   * @param table  table to update
   * @param entities  each list element is a POJO
   * @return one {@link RowSet} per array element with {@link RowSet#rowCount()} information
   */
  public <T> Future<RowSet<Row>> updateBatch(String table, List<T> entities) {
    try {
      if (entities == null || entities.size() == 0) {
        return Future.succeededFuture();
      }
      List<Tuple> list = new ArrayList<>(entities.size());
      for (T entity : entities) {
        list.add(Tuple.of(PostgresClient.pojo2JsonObject(entity)));
      }
      return updateBatchInternal(table, list);
    } catch (Throwable t) {
      log.error("updateBatch error " + t.getMessage(), t);
      return Future.failedFuture(t);
    }
  }

  /**
   * Update a specific record associated with the key passed in the id arg
   * @param table - table to update
   * @param entity - new pojo to save
   * @param id - key of the entity being updated
   * @return empty {@link RowSet} with {@link RowSet#rowCount()} information
   */
  public Future<RowSet<Row>> update(String table, Object entity, String id) {
    StringBuilder where = new StringBuilder("WHERE id=");
    SqlUtil.Cql2PgUtil.appendQuoted(id, where);  // proper masking prevents SQL injection
    return update(table, entity, PostgresClient.DEFAULT_JSONB_FIELD_NAME, where.toString(), false);
  }

  /**
   * Update records selected by WHERE clause.
   *
   * <p>Danger: The {@code whereClause} is prone to SQL injection. Consider using
   * an {@code update} method that takes {@link CQLWrapper} or {@link Criterion}.
   *
   * @param table - table to update
   * @param entity - pojo to set for matching records
   * @param whereClause - an SQL WHERE clause including the WHERE keyword,
   *                      or empty string to update all records
   * @param returnUpdatedIds - whether to return id of updated records
   * @return one result row per updated row, containing the id field
   */
  public Future<RowSet<Row>> update(String table, Object entity, String jsonbField,
      String whereClause, boolean returnUpdatedIds) {

    try {
      long start = log.isDebugEnabled() ? System.nanoTime() : 0;
      String sql = "UPDATE " + postgresClient.getSchemaName() + "." + table
          + " SET " + jsonbField + " = $1::jsonb " + whereClause
          + (returnUpdatedIds ? " RETURNING id" : "");
      log.debug("update query = {}", sql);
      return pgConnection.preparedQuery(sql).execute(Tuple.of(PostgresClient.pojo2JsonObject(entity)))
      .onComplete(query -> log.debug(() -> durationMsg("update", table, start)))
      .onFailure(e -> log.error(e.getMessage(), e));
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return Future.failedFuture(e);
    }
  }

  /**
   * Update 1...n records matching the {@code Criterion filter}
   * <br>
   * Criterion Examples:
   * <br>
   * 1. can be mapped from a string in the following format [{"field":"''","value":"","op":""}]
   * <pre>
   *    Criterion a = json2Criterion("[{\"field\":\"'fund_distributions'->[]->'amount'->>'sum'\",\"value\":120,\"op\":\"<\"}]"); //denotes funds_distribution is an array of objects
   *    Criterion a = json2Criterion("[{"field":"'po_line_status'->>'value'","value":"SENT","op":"like"},{"field":"'owner'->>'value'","value":"MITLIBMATH","op":"="}, {"op":"AND"}]");
   *    (see postgres query syntax for more examples in the read.me
   * </pre>
   * 2. Simple Criterion
   * <pre>
   *    Criteria b = new Criteria();
   *    b.field.add("'note'");
   *    b.operation = "=";
   *    b.value = "a";
   *    b.isArray = true; //denotes that the queried field is an array with multiple values
   *    Criterion a = new Criterion(b);
   * </pre>
   * 3. For a boolean field called rush = false OR note[] contains 'a'
   * <pre>
   *    Criteria d = new Criteria();
   *    d.field.add("'rush'");
   *    d.operation = Criteria.OP_IS_FALSE;
   *    d.value = null;
   *    Criterion a = new Criterion();
   *    a.addCriterion(d, Criteria.OP_OR, b);
   * </pre>
   * 4. for the following json:
   * <pre>
   *      "price": {
   *        "sum": "150.0",
   *         "po_currency": {
   *           "value": "USD",
   *           "desc": "US Dollar"
   *         }
   *       },
   *
   *    Criteria c = new Criteria();
   *    c.addField("'price'").addField("'po_currency'").addField("'value'");
   *    c.operation = Criteria.OP_LIKE;
   *    c.value = "USD";
   *
   * </pre>
   * @param table - table to update
   * @param entity - pojo to set for matching records
   * @param filter - see example below
   * @param returnUpdatedIds - return ids of updated records
   * @return ids of updated records if {@code returnUpdatedIds} is true
   *
   */
  public Future<RowSet<Row>> update(String table, Object entity, Criterion filter, boolean returnUpdatedIds) {
    String where = null;
    if (filter != null) {
      where = filter.toString();
    }
    return update(table, entity, PostgresClient.DEFAULT_JSONB_FIELD_NAME, where, returnUpdatedIds);
  }

  /**
   * Update all records in {@code table} that match the {@code CQLWrapper} query.
   * @param entity new content for the matched records
   * @return one row with the id for each updated record if returnUpdatedIds is true
   */
  public Future<RowSet<Row>> update(String table, Object entity, CQLWrapper filter, boolean returnUpdatedIds) {
    String where = "";
    if (filter != null) {
      where = filter.toString();
    }
    return update(table, entity, PostgresClient.DEFAULT_JSONB_FIELD_NAME, where, returnUpdatedIds);
  }

  /**
   * update a section / field / object in the pojo -
   * <br>
   * for example:
   * <br> if a json called po_line contains the following field
   * <pre>
   *     "po_line_status": {
   *       "value": "SENT",
   *       "desc": "sent to vendor"
   *     },
   * </pre>
   *  this translates into a po_line_status object within the po_line object - to update the entire object / section
   *  create an updateSection object pushing into the section the po line status as the field and the value (string / json / etc...) to replace it with
   *  <pre>
   *  a = new UpdateSection();
   *  a.addField("po_line_status");
   *  a.setValue(new JsonObject("{\"value\":\"SOMETHING_NEW4\",\"desc\":\"sent to vendor again\"}"));
   *  </pre>
   * Note that postgres does not update inplace the json but rather will create a new json with the
   * updated section and then reference the id to that newly created json
   * <br>
   * Queries generated will look something like this:
   * <pre>
   *
   * update test.po_line set jsonb = jsonb_set(jsonb, '{po_line_status}', '{"value":"SOMETHING_NEW4","desc":"sent to vendor"}') where _id = 19;
   * update test.po_line set jsonb = jsonb_set(jsonb, '{po_line_status, value}', '"SOMETHING_NEW5"', false) where _id = 15;
   * </pre>
   *
   * @param table - table to update
   * @param section - the field within the JSONB and the new field value
   * @param when - records to update
   * @return one row with the id for each updated record if returnUpdatedIds is true
   */
  public Future<RowSet<Row>> update(String table, UpdateSection section, Criterion when, boolean returnUpdatedIds) {
    try {
      long start = log.isDebugEnabled() ? System.nanoTime() : 0;
      String value = section.getValue().replace("'", "''");
      String where = when == null ? "" : when.toString();
      String returning = returnUpdatedIds ? " RETURNING id" : "";
      String sql = "UPDATE " + postgresClient.getSchemaName() + "." + table
          + " SET " + PostgresClient.DEFAULT_JSONB_FIELD_NAME
          + " = jsonb_set(" + PostgresClient.DEFAULT_JSONB_FIELD_NAME + ","
          + section.getFieldsString() + ", '" + value + "', false) "
          + where + returning;
      log.debug("update query = {}", sql);
      return pgConnection.preparedQuery(sql).execute()
      .onComplete(query -> log.debug(() -> durationMsg("update", table, start)))
      .onFailure(e -> log.error(e.getMessage(), e));
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return Future.failedFuture(e);
    }
  }

  private Future<RowSet<Row>> doDelete(String table, String where) {
    try {
      long start = log.isDebugEnabled() ? System.nanoTime() : 0;
      String sql = "DELETE FROM " + postgresClient.getSchemaName() + "." + table + " " + where;
      log.debug("doDelete query = {}", sql);
      return pgConnection.preparedQuery(sql).execute()
          .onFailure(e -> log.error(e.getMessage(), e))
          .onComplete(done -> log.debug(() -> durationMsg("delete", table, start)));
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return Future.failedFuture(e);
    }
  }

  /**
   * Delete by id.
   * @param table table name without schema
   * @param id primary key value of the record to delete
   * @return empty {@link RowSet} with {@link RowSet#rowCount()} information
   */
  public Future<RowSet<Row>> delete(String table, String id) {
    try {
      return pgConnection.preparedQuery(
          "DELETE FROM " + postgresClient.getSchemaName() + "." + table + " WHERE id=$1")
          .execute(Tuple.of(UUID.fromString(id)));
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return Future.failedFuture(e);
    }
  }

  /**
   * Delete by POJO.
   * @param table table name without schema
   * @param entity a POJO of the record to delete
   * @return empty {@link RowSet} with {@link RowSet#rowCount()} information
   */
  public Future<RowSet<Row>> delete(String table, Object entity) {
    try {
      long start = log.isDebugEnabled() ? System.nanoTime() : 0;
      String sql = "DELETE FROM " + postgresClient.getSchemaName() + "." + table
          + " WHERE jsonb @> $1";
      log.debug("delete by entity, query = {}; $1 = {}", sql, entity);
      return pgConnection.preparedQuery(sql).execute(Tuple.of(PostgresClient.pojo2JsonObject(entity)))
          .onFailure(e -> log.error(e.getMessage(), e))
          .onComplete(done -> log.debug(() -> durationMsg("delete", table, start)));
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return Future.failedFuture(e);
    }
  }

  /**
   * Delete by {@code CQLWrapper}.
   * @param table table name without schema
   * @param cql which records to delete
   * @return empty {@link RowSet} with {@link RowSet#rowCount()} information
   */
  public Future<RowSet<Row>> delete(String table, CQLWrapper cql) {
    try {
      String where = cql == null ? "" : cql.getWhereClause();
      return doDelete(table, where);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return Future.failedFuture(e);
    }
  }

  /**
   * Delete by Criterion.
   * @param table table name without schema
   * @param filter which records to delete
   * @return empty {@link RowSet} with {@link RowSet#rowCount()} information
   */
  public Future<RowSet<Row>> delete(String table, Criterion filter) {
    try {
      String where = filter == null ? "" : filter.toString();
      return doDelete(table, where);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return Future.failedFuture(e);
    }
  }

  /**
   * Return records selected by {@link CQLWrapper} filter.
   *
   * @param table - table to query
   * @param clazz - class of objects to be returned
   * @param fieldName - database column to return, for example  @link {@link PostgresClient#DEFAULT_JSONB_FIELD_NAME}
   * @param wrapper - filter to select records
   * @param returnCount - whether to return totalRecords, the number of matching records when disabling OFFSET and LIMIT
   * @param returnIdField - if the id field should also be returned, must be true for facets
   * @param facets - fields to calculate counts for
   * @param distinctOn - database column to calculate the number of distinct values for, null or empty string for none
   */
  public <T> Future<Results<T>> get(String table, Class<T> clazz,
      String fieldName, CQLWrapper wrapper, boolean returnCount, boolean returnIdField,
      List<FacetField> facets, String distinctOn) {

    try {
      QueryHelper queryHelper = postgresClient.buildQueryHelper(table, fieldName, wrapper, returnIdField, facets, distinctOn);
      Function<TotaledResults, Results<T>> resultSetMapper = totaledResults ->
      postgresClient.processResults(totaledResults.set, totaledResults.estimatedTotal, queryHelper.offset, queryHelper.limit, clazz);
      if (returnCount) {
        return postgresClient.processQueryWithCount(pgConnection, queryHelper, "get", resultSetMapper);
      } else {
        return Future.future(promise -> postgresClient.processQuery(pgConnection, queryHelper, null, "get", resultSetMapper, promise));
      }
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return Future.failedFuture(e);
    }
  }

  /**
   * Returns records selected by {@link Criterion} filter
   * @param table - table to query
   * @param clazz - class of objects to be returned
   * @param fieldName - database column to return, for example  @link {@link PostgresClient#DEFAULT_JSONB_FIELD_NAME}
   * @param filter - records to select
   * @param returnCount - whether to return totalRecords, the number of matching records
   *         when disabling OFFSET and LIMIT
   * @param facets - fields to calculate counts for
   */
  public <T> Future<Results<T>> get(String table, Class<T> clazz, String fieldName, Criterion filter,
      boolean returnCount, List<FacetField> facets) {

    CQLWrapper cqlWrapper = new CQLWrapper(filter);
    return get(table, clazz, fieldName, cqlWrapper, returnCount, true, facets, null);
  }

  /**
   * Returns records selected by {@link Criterion} filter
   * @param table - table to query
   * @param clazz - class of objects to be returned
   * @param filter - records to select
   * @param returnCount - whether to return totalRecords, the number of matching records
   *         when disabling OFFSET and LIMIT
   * @param facets - fields to calculate counts for
   */
  public <T> Future<Results<T>> get(String table, Class<T> clazz, Criterion filter,
      boolean returnCount, List<FacetField> facets) {

    return get(table, clazz, PostgresClient.DEFAULT_JSONB_FIELD_NAME, filter, returnCount, facets);
  }

  /**
   * Returns records selected by {@link Criterion} filter.
   *
   * @param table - table to query
   * @param clazz - class of objects to be returned
   * @param filter - which records to select
   * @param returnCount - whether to return totalRecords, the number of matching records when disabling OFFSET and LIMIT
   */
  public <T> Future<Results<T>> get(String table, Class<T> clazz, Criterion filter, boolean returnCount) {
    return get(table, clazz, PostgresClient.DEFAULT_JSONB_FIELD_NAME, new CQLWrapper(filter),
        returnCount, false, null, null);
  }

  /**
   * Returns records selected by {@link Criterion} filter.
   *
   * <p>Doesn't calculate totalRecords, the number of matching records when disabling OFFSET and LIMIT.
   *
   * @param table - table to query
   * @param clazz - class of objects to be returned
   * @param filter - which records to select
   */
  public <T> Future<Results<T>> get(String table, Class<T> clazz, Criterion filter) {
    return get(table, clazz, PostgresClient.DEFAULT_JSONB_FIELD_NAME, new CQLWrapper(filter),
        false, false, null, null);
  }

  /**
   * Returns records selected by {@link CQLWrapper} filter.
   *
   * <p>Doesn't calculate totalRecords, the number of matching records when disabling OFFSET and LIMIT.
   *
   * @param table - table to query
   * @param clazz - class of objects to be returned
   * @param filter - which records to select
   */
  public <T> Future<Results<T>> get(String table, Class<T> clazz, CQLWrapper filter) {
    return get(table, clazz, PostgresClient.DEFAULT_JSONB_FIELD_NAME, filter, false, false, null, null);
  }

  /**
   * Returns records selected by {@link CQLWrapper} filter.
   *
   * @param table - table to query
   * @param clazz - class of objects to be returned
   * @param filter - which records to select
   * @param returnCount - whether to return totalRecords, the number of matching records
   *         when disabling OFFSET and LIMIT
   */
  public <T> Future<Results<T>> get(String table, Class<T> clazz, CQLWrapper filter, boolean returnCount) {
    return get(table, clazz, PostgresClient.DEFAULT_JSONB_FIELD_NAME, filter, returnCount, false, null, null);
  }

  /**
   * Stream records selected by CQLWrapper.
   *
   * @param table - table to query
   * @param clazz - class of objects to be returned
   * @param fieldName - database column to return, for example {@link PostgresClient#DEFAULT_JSONB_FIELD_NAME}
   * @param wrapper - filter to select records
   * @param returnIdField - if the id field should also be returned, must be true for facets
   * @param distinctOn - database column to calculate the number of distinct values for, null or empty string for none
   * @param facets - fields to calculate counts for
   * @param streamHandler contains {@link ResultInfo} and handlers to process the stream
   */
  public <T> Future<Void> streamGet(String table, Class<T> clazz, String fieldName, CQLWrapper wrapper,
      boolean returnIdField, String distinctOn, List<FacetField> facets,
      Handler<AsyncResult<PostgresClientStreamResult<T>>> streamHandler) {

    Promise<Void> promise = Promise.promise();
    postgresClient.doStreamGetCount(pgConnection, false, table, clazz, fieldName, wrapper,
        returnIdField, distinctOn, facets, handler -> {
          if (handler.failed()) {
            promise.tryFail(handler.cause());
            return;
          }
          handler.result().setDoneHandler(throwable -> {
            if (throwable != null) {
              promise.tryFail(throwable);
              return;
            }
            promise.tryComplete();
          });
          streamHandler.handle(handler);
        });
    return promise.future();
  }

  /**
   * Stream records selected by CQLWrapper.
   *
   * @param table - table to query
   * @param clazz - class of objects to be returned
   * @param wrapper - filter to select records
   * @param streamHandler contains {@link ResultInfo} and handlers to process the stream
   */
  public <T> Future<Void> streamGet(String table, Class<T> clazz, CQLWrapper wrapper,
      Handler<AsyncResult<PostgresClientStreamResult<T>>> streamHandler) {

    return streamGet(table, clazz, PostgresClient.DEFAULT_JSONB_FIELD_NAME, wrapper, false, null, null, streamHandler);
  }

  /**
   * Get a stream of the results of the {@code sql} query.
   *
   * Sample usage:
   *
   * <pre>
   * postgresClient.withTrans(conn -> step1()
   *     .compose(x -> conn.selectStream("SELECT i FROM numbers WHERE i > $1", Tuple.tuple(5), 100,
   *         rowStream -> rowStream.handler(row -> task.process(row))))
   *     .compose(x -> ...
   * </pre>
   *
   * <p>Use withReadTrans if all SQL queries are read-only (no nextval(), no UPDATE, ...):</p>
   *
   * <pre>
   *  postgresClient.withReadTrans(conn ->
   *     conn.selectStream("SELECT i FROM numbers WHERE i > $1", Tuple.tuple(5), 100,
   *         rowStream -> rowStream.handler(row -> task.process(row))));
   * </pre>
   *
   * @param params arguments for {@code $} placeholders in {@code sql}
   * @param chunkSize cursor fetch size
   */
  public Future<Void> selectStream(String sql, Tuple params, int chunkSize, Handler<RowStream<Row>> rowStreamHandler) {
    try {
      return pgConnection.prepare(sql)
      .compose(preparedStatement -> {
        PreparedRowStream rowStream = new PreparedRowStream(preparedStatement, chunkSize, params);
        rowStreamHandler.handle(rowStream);
        return rowStream.getResult().eventually(x -> preparedStatement.close());
      });
    } catch (Throwable e) {
      log.error(e.getMessage() + " - " + sql, e);
      return Future.failedFuture(e);
    }
  }

  /**
   * Get a stream of the results of the {@code sql} query.
   *
   * The chunk size is {@link PostgresClient#STREAM_GET_DEFAULT_CHUNK_SIZE}.
   *
   * Sample usage:
   *
   * <pre>
   * postgresClient.withTrans(conn -> step1()
   *     .compose(x -> conn.selectStream("SELECT i FROM numbers WHERE i > $1", Tuple.tuple(5),
   *         rowStream -> rowStream.handler(row -> task.process(row))))
   *     .compose(x -> ...
   * </pre>
   *
   * <p>Use withReadTrans if all SQL queries are read-only (no nextval(), no UPDATE, ...):
   *    *
   *    * <pre>
   *    * postgresClient.withReadTrans(conn ->
   *    *     conn.selectStream("SELECT i FROM numbers WHERE i > $1", Tuple.tuple(5),
   *    *         rowStream -> rowStream.handler(row -> task.process(row))));
   *    * </pre>
   * </p>
   *
   * @param params arguments for {@code $} placeholders in {@code sql}
   */
  public Future<Void> selectStream(String sql, Tuple params, Handler<RowStream<Row>> rowStreamHandler) {
    return selectStream(sql, params, PostgresClient.STREAM_GET_DEFAULT_CHUNK_SIZE, rowStreamHandler);
  }

  /**
   * Send a parameterized/prepared statement.
   *
   * @param sql - the SQL command to run
   * @param params - the values for the {@code $} placeholders, empty if none
   * @return the reply from the database
   */
  public Future<RowSet<Row>> execute(String sql, Tuple params) {
    try {
      long start = log.isDebugEnabled() ? System.nanoTime() : 0;
      // more than optimization.. preparedQuery does not work for multiple SQL statements
      if (params.size() == 0) {
        return pgConnection.query(sql).execute()
            .onComplete(x -> log.debug(() -> durationMsg("execute", sql, start)));
      }
      return pgConnection.preparedQuery(sql).execute(params)
          .onComplete(x -> log.debug(() -> durationMsg("execute", sql, start)));
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return Future.failedFuture(e);
    }
  }

  /**
   * Run an SQL statement.
   *
   * <p>Use {@link #execute(String, Tuple)} with {@code $1, $2, ...} parameters to
   * avoid SQL injection.
   *
   * @param sql - the SQL command to run
   * @return the reply from the database
   */
  public Future<RowSet<Row>> execute(String sql) {
    return execute(sql, Tuple.tuple());
  }

  /**
   * Run a parameterized/prepared statement with a list of tuples as parameters.
   * This is atomic, if one Tuple fails the complete list fails: all or nothing.
   *
   * @param sql - the SQL command to run
   * @param params - there is one list entry for each SQL invocation containing the
   *                    parameters for the {@code $} placeholders.
   * @return the reply from the database, one RowSet per params Tuple. null if params.size() == 0.
   */
  public Future<RowSet<Row>> execute(String sql, List<Tuple> params) {

    try {
      if (params.size() == 0) {
        return Future.succeededFuture();
      }
      long start = log.isDebugEnabled() ? System.nanoTime() : 0;
      return pgConnection.prepare(sql)
          .compose(preparedStatement -> preparedStatement.query().executeBatch(params)
              .eventually(x -> preparedStatement.close()))
          .onComplete(x -> log.debug(() -> durationMsg("execute", sql, start)));
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return Future.failedFuture(e);
    }
  }

}
