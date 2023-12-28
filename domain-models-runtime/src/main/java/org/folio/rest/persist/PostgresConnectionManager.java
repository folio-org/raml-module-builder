package org.folio.rest.persist;

import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.Pool;
import io.vertx.core.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PostgresConnectionManager {
  private static final Logger LOG = LogManager.getLogger(PostgresConnectionManager.class);

  private PgConnection currentConnection;

  private String currentTenant;

  public PostgresConnectionManager() {
    // TODO Document
  }

  public void clear() {
    this.currentTenant = "";
    this.currentConnection = null;

    LOG.debug("Cleared connection manager");
  }

  public Future<PgConnection> getConnection(Pool pool, String schemaName, String tenantId) {
    if (! PostgresClient.sharedPgPool) {
      LOG.debug("Shared - not in shared mode");
      return pool.getConnection().map(PgConnection.class::cast);
    }

    if (tenantId.equals(currentTenant)) {
      // TODO This is rather ineffcient since it always uses the same conn for the tenant.
      // TODO Perhaps create a list as long as this is true and then re-use from that list.
      LOG.debug("Shared - Recycling connection for {}", currentTenant);
      return Future.succeededFuture(currentConnection);
    } else {
      currentTenant = tenantId;

      LOG.debug("Shared - Creating connection for {}", currentTenant);

      // running the two SET queries adds about 1.5 ms execution time
      // "SET SCHEMA ..." sets the search_path because neither "SET ROLE" nor "SET SESSION AUTHORIZATION" set it
      String sql = PostgresClient.DEFAULT_SCHEMA.equals(tenantId)
          ? "SET ROLE NONE; SET SCHEMA ''"
          : ("SET ROLE '" + schemaName + "'; SET SCHEMA '" + schemaName + "'");

      return pool.getConnection().compose(sqlConnection -> {
        this.currentConnection = (PgConnection) sqlConnection;
        return sqlConnection.query(sql).execute()
            .map((PgConnection) sqlConnection);
      });
    }
  }
}
