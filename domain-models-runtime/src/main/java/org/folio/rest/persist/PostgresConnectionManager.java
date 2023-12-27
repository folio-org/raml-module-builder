package org.folio.rest.persist;

import io.vertx.pgclient.PgConnection;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.SqlConnection;

import io.vertx.core.Future;

import java.util.HashMap;
import java.util.Map;

public class PostgresConnectionManager {
  private Map<String, SQLConnection> tenantConnections;

  private String currentTenantId;

  public PostgresConnectionManager() {
    this.tenantConnections = new HashMap<>();
  }

  public Future<PgConnection> getConnection(PgPool pool, boolean sharedPgPool, String schemaName, String tenantId) {
    Future<SqlConnection> future;

    future = pool.getConnection();

    if (! sharedPgPool) {
      return future.map(sqlConnection -> (PgConnection) sqlConnection);
    }

    // running the two SET queries adds about 1.5 ms execution time
    // "SET SCHEMA ..." sets the search_path because neither "SET ROLE" nor "SET SESSION AUTHORIZATION" set it
    String sql = PostgresClient.DEFAULT_SCHEMA.equals(tenantId)
            ? "SET ROLE NONE; SET SCHEMA ''"
            : ("SET ROLE '" + schemaName + "'; SET SCHEMA '" + schemaName + "'");

    return future.compose(sqlConnection ->
        sqlConnection.query(sql).execute()
            .map((PgConnection) sqlConnection));
  }
}
