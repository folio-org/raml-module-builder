package org.folio.rest.persist;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.Tuple;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.persist.PostgresClient.FunctionWithException;

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
      String sql = "INSERT INTO " + postgresClient.getSchemaName() + "." + table
          + " (id, jsonb) VALUES ($1, " + (convertEntity ? "$2" : "$2::text") + ")"
          + (upsert ? " ON CONFLICT (id) DO UPDATE SET jsonb=EXCLUDED.jsonb" : "")
          + " RETURNING " + (returnId ? "id" : "''");
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

}
