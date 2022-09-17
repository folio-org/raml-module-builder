package org.folio.rest.impl;

import freemarker.template.TemplateException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Tuple;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.ws.rs.core.Response;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.jaxrs.model.TenantJob;
import org.folio.rest.jaxrs.resource.Tenant;
import org.folio.rest.persist.PostgresClient;
import org.folio.dbschema.Schema;
import org.folio.rest.persist.ddlgen.SchemaMaker;
import org.folio.dbschema.TenantOperation;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.rest.tools.client.exceptions.ResponseException;
import org.folio.rest.tools.utils.TenantTool;

/**
 * @author shale
 *
 */
public class TenantAPI implements Tenant {

  public static final String TABLE_JSON = "templates/db_scripts/schema.json";

  private static final Logger log = LogManager.getLogger(TenantAPI.class);

  private static Map<String, List<Promise<Void>>> waiters = new HashMap<>();

  PostgresClient postgresClient(Context context) {
    return PostgresClient.getInstance(context.owner());
  }

  Future<Void> requirePostgres(Context context, int minNum, String minVersion) {
    return
        postgresClient(context)
        .select("SELECT current_setting('server_version_num')::int AS num, "
            + "current_setting('server_version') AS version")
        .map(rowSet -> {
          int num = rowSet.iterator().next().getInteger("num");
          String version = rowSet.iterator().next().getString("version");
          if (minNum > num) {
            throw new UnsupportedOperationException(
                "Expected PostgreSQL server version " + minVersion + " or later but found " + version);
          }
          return null;
        });
  }

  private String getLocalString(Context context, String key) {
    Object object = context.getLocal(key);
    if (object == null) {
      return null;
    }
    return object.toString();
  }

  Future<Void> requirePostgresVersion(Context context) {
    String minNum = getLocalString(context, "postgres_min_version_num");
    String min = getLocalString(context, "postgres_min_version");
    if (minNum == null || min == null) {
      return requirePostgres(context, 120000, "12.0");
    }
    return requirePostgres(context, Integer.parseInt(minNum), min);
  }

  Future<Boolean> tenantExists(Context context, String tenantId){
    /* connect as user in postgres-conf.json file (super user) - so that all commands will be available */
    return postgresClient(context).select(
        "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = '"
            + PostgresClient.convertToPsqlStandard(tenantId) +"');")
        .map(result -> result.iterator().next().getBoolean(0))
        .onFailure(e -> log.error(e.getMessage(), e));
  }

  /**
   * @return previous (currently stored) rmb_internal info; null if none exist
   */
  Future<JsonObject> getRmbInternal(Context context, String tenantId, boolean tenantExists) {
    if (! tenantExists) {
      return Future.succeededFuture(null);
    }
    String sql = "SELECT jsonb " +
        "FROM " + PostgresClient.convertToPsqlStandard(tenantId) + ".rmb_internal";
    return postgresClient(context).selectSingle(sql)
        .map(row -> row == null ? null : row.getJsonObject(0))
        .onFailure(e -> log.error(e.getMessage(), e));
  }

  String getTablePath() {
    return TABLE_JSON;
  }

  public static class NoSchemaJsonException extends RuntimeException {
  }

  /**
   * @param tenantExists false for initial installation, true for upgrading
   * @param tenantAttributes parameters like module version that may influence generated SQL
   * @param previousVersion module version stored
   * @param previousSchema schema to upgrade from, may be null if unknown and on initial install
   * @return the SQL commands . 0 is immediate script (create, disable), 1 (optional) is update schema script.
   * @throws NoSchemaJsonException when templates/db_scripts/schema.json doesn't exist
   * @throws TemplateException when processing templates/db_scripts/schema.json fails
   */
  public String [] sqlFile(String tenantId, boolean tenantExists, TenantAttributes tenantAttributes,
      String previousVersion, Schema previousSchema) throws IOException, TemplateException {

    InputStream tableInput = TenantAPI.class.getClassLoader().getResourceAsStream(getTablePath());
    if (tableInput == null) {
      log.info("Could not find templates/db_scripts/schema.json , "
          + " RMB will not run any scripts for " + tenantId);
      throw new NoSchemaJsonException();
    }

    TenantOperation op = TenantOperation.CREATE;
    String newVersion = tenantAttributes == null ? null : tenantAttributes.getModuleTo();
    if (tenantExists) {
      op = TenantOperation.UPDATE;
    }

    SchemaMaker sMaker = new SchemaMaker(tenantId, PostgresClient.getModuleName(), op, previousVersion, newVersion);

    String tableInputStr = IOUtils.toString(tableInput, StandardCharsets.UTF_8);
    sMaker.setSchemaJson(tableInputStr);
    Schema schema = ObjectMapperTool.readValue(tableInputStr, Schema.class);
    sMaker.setSchema(schema);
    sMaker.setPreviousSchema(previousSchema);

    if (tenantAttributes != null && Boolean.TRUE.equals(tenantAttributes.getPurge())) {
      return new String [] { sMaker.generatePurge() };
    }
    if (tenantAttributes != null && tenantAttributes.getModuleFrom() != null &&
         tenantAttributes.getModuleTo() == null) {
      return new String [] { "" };
    }
    String [] scripts = new String[] { sMaker.generateCreate(), sMaker.generateSchemas() };
    log.debug("GENERATED CREATE {}", scripts[0]);
    log.debug("GENERATED SCHEMAS {}", scripts[1]);
    return scripts;
  }

  Future<String[]> sqlFile(Context context, String tenantId,
                           TenantAttributes tenantAttributes, boolean tenantExists) {

    return getRmbInternal(context, tenantId, tenantExists)
        .compose(result -> {
          Schema schema = null;
          String version = "0.0";
          if (result != null) {
            version = result.getString("moduleVersion");
            String schemaString = result.getString("schemaJson");
            if (schemaString != null) {
              schema = ObjectMapperTool.readValue(schemaString, Schema.class);
            }
          }
          try {
            String [] sqlFile = sqlFile(tenantId, tenantExists, tenantAttributes, version, schema);
            return Future.succeededFuture(sqlFile);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          } catch (TemplateException e) {  // checked exception from main.tpl parsing
            throw new IllegalArgumentException(e);
          } catch (NoSchemaJsonException e) {  // checked exception from main.tpl parsing
            throw new IllegalArgumentException("No schema.json");
          }
        });
  }

  Future<Void> saveJob(TenantJob tenantJob, String tenantId, String jobId, Context context) {
    String table = PostgresClient.convertToPsqlStandard(tenantId) + ".rmb_job";
    String sql = "INSERT INTO " + table + " VALUES ($1, $2)";
    return postgresClient(context).execute(sql, Tuple.of(jobId, JsonObject.mapFrom(tenantJob))).mapEmpty();
  }

  Future<TenantJob> getJob(String tenantId, UUID jobId, Context context) {
    String table = PostgresClient.convertToPsqlStandard(tenantId) + ".rmb_job";
    String sql = "SELECT jsonb FROM " + table + " WHERE id = $1";
    return postgresClient(context).selectSingle(sql, Tuple.of(jobId))
        .compose(reply -> {
          if (reply == null) {
            return Future.failedFuture("Job not found " + jobId);
          }
          JsonObject o = reply.getJsonObject(0);
          return Future.succeededFuture(o.mapTo(TenantJob.class));
        });
  }

  Future<Void> updateJob(TenantJob tenantJob, Context context) {
    String table = PostgresClient.convertToPsqlStandard(tenantJob.getTenant()) + ".rmb_job";
    String sql = "UPDATE " + table + " SET jsonb = $2 WHERE id = $1";
    return postgresClient(context).execute(sql, Tuple.of(tenantJob.getId(), JsonObject.mapFrom(tenantJob))).mapEmpty();
  }

  Future<Void> removeJob(String tenantId, String jobId, Context context) {
    String table = PostgresClient.convertToPsqlStandard(tenantId) + ".rmb_job";
    String sql = "DELETE FROM  " + table + " WHERE id = $1";
    return postgresClient(context).execute(sql, Tuple.of(jobId)).mapEmpty();
  }

  /**
   * Installs or upgrades a module for a tenant.
   *
   * <p>The <code>handler</code> signals an error with a failing result and a {@link ResponseException}.
   *
   * @see <a href="https://github.com/folio-org/raml-module-builder#extending-the-tenant-init">Extending the Tenant Init</a>
   *      for usage examples
   */
  @Validate
  @Override
  public void postTenant(TenantAttributes tenantAttributes, Map<String, String> headers,
                         Handler<AsyncResult<Response>> handler, Context context)  {
    postTenant(true, tenantAttributes, headers, handler, context);
  }

  private void postTenant(boolean async, TenantAttributes tenantAttributes, Map<String, String> headers,
                          Handler<AsyncResult<Response>> handler, Context context) {

    String tenantId = TenantTool.tenantId(headers);
    String id = UUID.randomUUID().toString();
    TenantJob job = new TenantJob();
    job.setId(id);
    job.setTenant(tenantId);
    job.setTenantAttributes(tenantAttributes);
    job.setComplete(false);

    String location = "/_/tenant/" + id;
    requirePostgresVersion(context)
        .compose(x -> tenantExists(context, tenantId))
        .compose(exists -> sqlFile(context, tenantId, tenantAttributes, exists))
        .onFailure(cause -> {
          log.error(cause.getMessage(), cause);
          handler.handle(Future.succeededFuture(PostTenantResponse.respond400WithTextPlain(cause.getMessage())));
        })
        .onSuccess(files ->
          postgresClient(context).runSQLFile(files[0], true)
              .compose(res -> {
                if (!res.isEmpty()) {
                  return Future.failedFuture(res.get(0));
                }
                if (files.length == 1) { // not saving job for disable or purge
                  return Future.succeededFuture();
                }
                return saveJob(job, tenantId, id, context);
              })
              .onFailure(cause -> {
                log.error(cause.getMessage(), cause);
                handler.handle(Future.succeededFuture(PostTenantResponse.respond400WithTextPlain(cause.getMessage())));
              })
              .onSuccess(result -> {
                if (files.length == 1) { // disable or purge?
                  PostgresClient.closeAllClients(tenantId);
                  handler.handle(Future.succeededFuture(PostTenantResponse.respond204()));
                  return;
                }
                if (async) {
                  handler.handle(Future.succeededFuture(PostTenantResponse.respond201WithApplicationJson(job,
                      PostTenantResponse.headersFor201().withLocation(location))));
                }
                runAsync(tenantAttributes, files[1], job, headers, context)
                    .onComplete(res -> {
                      log.info("job {} completed", id);
                      if (async) {
                        completeJob(job, context);
                      } else {
                        if (job.getError() != null) {
                          handler.handle(Future.succeededFuture(
                              PostTenantResponse.respond400WithTextPlain(job.getError())));
                          return;
                        }
                        handler.handle(Future.succeededFuture(PostTenantResponse.respond204()));
                      }
                    });
              })
        );
  }

  private void completeJob(TenantJob job, Context context) {
    job.setComplete(true);
    updateJob(job, context).onComplete(res -> {
      if (res.failed()) {
        log.error("updateJob FAILED {}", res.cause().getMessage(), res.cause());
      }
      List<Promise<Void>> promises = waiters.remove(job.getId());
      if (promises != null) {
        for (Promise<Void> promise : promises) {
          promise.tryComplete();
        }
      }
    });
  }

  Future<Void> runAsync(TenantAttributes tenantAttributes, String file, TenantJob job, Map<String, String> headers, Context context) {
    return postgresClient(context).runSQLFile(file, true)
        .compose(res -> {
          if (!res.isEmpty()) {
            job.setMessages(res);
            return Future.failedFuture("SQL error");
          }
          if (tenantAttributes == null) {
            return Future.succeededFuture();
          }
          return loadData(tenantAttributes, job.getTenant(), headers, context);
        })
        .onFailure(cause -> {
          String message = cause.getMessage();
          if (message == null) {
            message = cause.getClass().getName();
          }
          job.setError(message);
        }).mapEmpty();
  }

  /**
   * Stub load sample/reference data.
   * @param attributes information about what to load.
   * @param tenantId tenant ID
   * @param headers HTTP headers for the request (Okapi)
   * @return number of records loaded
   */
  Future<Integer> loadData(TenantAttributes attributes, String tenantId, Map<String, String> headers,
                           Context vertxContext) {
    return Future.succeededFuture(0);
  }

  /**
   * Tnitialize tenant, synchronous mode.
   *
   * @param tenantAttributes attributes for operation
   * @param headers Okapi headers
   * @param handler response handler
   * @param context Vert.x context
   */
  public void postTenantSync(TenantAttributes tenantAttributes, Map<String, String> headers,
                             Handler<AsyncResult<Response>> handler, Context context)  {
    postTenant(false, tenantAttributes, headers, handler, context);
  }

  Future<TenantJob> checkJob(String tenantId, UUID jobId, Context context) {
    return tenantExists(context, tenantId).compose(exists -> {
      if (!exists) {
        return Future.failedFuture("Tenant not found " + tenantId);
      }
      return getJob(tenantId, jobId, context);
    });
  }

  @Override
  public void getTenantByOperationId(String operationId, int wait, Map<String, String> headers,
                                     Handler<AsyncResult<Response>> handler, Context context) {
    String tenantId = TenantTool.tenantId(headers);
    try {
      checkJob(tenantId, UUID.fromString(operationId), context)
          .onSuccess(job -> {
            Response response;
            if (wait > 0 && !Boolean.TRUE.equals(job.getComplete())) {
              Promise<Void> promise = Promise.promise();
              waiters.putIfAbsent(operationId, new LinkedList<>());
              waiters.get(operationId).add(promise);
              context.owner().setTimer(wait, res -> promise.tryComplete());
              promise.future().onComplete(res -> getTenantByOperationId(operationId, 0, headers, handler, context));
              return;
            }
            response = GetTenantByOperationIdResponse.respond200WithApplicationJson(job);
            handler.handle(Future.succeededFuture(response));
          })
          .onFailure(cause -> {
            log.error(cause.getMessage(), cause);
            Response response = GetTenantByOperationIdResponse.respond404WithTextPlain(cause.getMessage());
            handler.handle(Future.succeededFuture(response));
          });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      Response response = GetTenantByOperationIdResponse.respond400WithTextPlain(e.getMessage());
      handler.handle(Future.succeededFuture(response));
    }
  }

  @Override
  public void deleteTenantByOperationId(String operationId, Map<String, String> headers,
                                        Handler<AsyncResult<Response>> handler, Context context) {
    String tenantId = TenantTool.tenantId(headers);
    try {
      checkJob(tenantId, UUID.fromString(operationId), context)
          .onFailure(cause -> {
            log.error(cause.getMessage(), cause);
            Response response = DeleteTenantByOperationIdResponse.respond404WithTextPlain(cause.getMessage());
            handler.handle(Future.succeededFuture(response));
          })
          .onSuccess(job -> {
            removeJob(tenantId, operationId, context)
                .onSuccess(res -> {
                  Response response = DeleteTenantByOperationIdResponse.respond204();
                  handler.handle(Future.succeededFuture(response));
                })
                .onFailure(cause -> {
                  log.error(cause.getMessage(), cause);
                  Response response = DeleteTenantByOperationIdResponse.respond400WithTextPlain(cause.getMessage());
                  handler.handle(Future.succeededFuture(response));
                });
          });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      Response response = GetTenantByOperationIdResponse.respond400WithTextPlain(e.getMessage());
      handler.handle(Future.succeededFuture(response));
    }
  }
}
