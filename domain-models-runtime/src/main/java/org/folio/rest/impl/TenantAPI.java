package org.folio.rest.impl;

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

import io.vertx.ext.web.RoutingContext;
import org.apache.commons.io.IOUtils;
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

import freemarker.template.TemplateException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.sqlclient.Row;

/**
 * @author shale
 *
 */
public class TenantAPI implements Tenant {

  public static final String TABLE_JSON = "templates/db_scripts/schema.json";

  private static final Logger       log               = LoggerFactory.getLogger(TenantAPI.class);

  private static Map<String, TenantJob> jobs = new HashMap<>();
  private static Map<String, List<Promise<Void>>> waiters = new HashMap<>();

  PostgresClient postgresClient(Context context) {
    return PostgresClient.getInstance(context.owner());
  }

  Future<Boolean> tenantExists(Context context, String tenantId) {
    Promise<Boolean> promise = Promise.promise();
    tenantExists(context, tenantId, promise::handle);
    return promise.future();
  }

  void tenantExists(Context context, String tenantId, Handler<AsyncResult<Boolean>> handler){
    /* connect as user in postgres-conf.json file (super user) - so that all commands will be available */
    postgresClient(context).select(
      "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = '"+ PostgresClient.convertToPsqlStandard(tenantId) +"');",
        reply -> {
          try {
            if(reply.succeeded()){
              Boolean aBoolean = reply.result().iterator().next().getBoolean(0);
              handler.handle(io.vertx.core.Future.succeededFuture(aBoolean));
            }
            else {
              log.error(reply.cause().getMessage(), reply.cause());
              handler.handle(io.vertx.core.Future.failedFuture(reply.cause().getMessage()));
            }
          } catch (Exception e) {
            log.error(e.getMessage(), e);
            handler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
          }
    });
  }

  /**
   * @return previous Schema from rmb_internal.jsonb->>'schemaJson', or null if not exist.
   */
  Future<Schema> previousSchema(Context context, String tenantId, boolean tenantExists) {
    Promise<Schema> promise = Promise.promise();

    if (! tenantExists) {
      promise.complete(null);
      return promise.future();
    }

    String sql = "SELECT jsonb->>'schemaJson' " +
        "FROM " + PostgresClient.convertToPsqlStandard(tenantId) + ".rmb_internal";
    postgresClient(context).selectSingle(sql, select -> {
      if (select.failed()) {
        promise.fail(select.cause());
        return;
      }
      try {
        Row row = select.result();
        String schemaString = row == null ? null : row.getString(0);
        if (schemaString == null) {
          promise.complete(null);
          return;
        }
        Schema schema = ObjectMapperTool.getMapper().readValue(schemaString, Schema.class);
        promise.complete(schema);
      } catch (Exception e) {
        promise.fail(e);
      }
    });
    return promise.future();
  }

  String getTablePath() {
    return TABLE_JSON;
  }

  public static class NoSchemaJsonException extends RuntimeException {
  }

  /**
   * @param tenantExists false for initial installation, true for upgrading
   * @param tenantAttributes parameters like module version that may influence generated SQL
   * @param previousSchema schema to upgrade from, may be null if unknown and on initial install
   * @return the SQL commands to create or upgrade the tenant's schema
   * @throws NoSchemaJsonException when templates/db_scripts/schema.json doesn't exist
   * @throws TemplateException when processing templates/db_scripts/schema.json fails
   */
  public String sqlFile(String tenantId, boolean tenantExists, TenantAttributes tenantAttributes,
      Schema previousSchema) throws IOException, TemplateException {

    InputStream tableInput = TenantAPI.class.getClassLoader().getResourceAsStream(getTablePath());
    if (tableInput == null) {
      log.info("Could not find templates/db_scripts/schema.json , "
          + " RMB will not run any scripts for " + tenantId);
      throw new NoSchemaJsonException();
    }

    TenantOperation op = TenantOperation.CREATE;
    String previousVersion = null;
    String newVersion = tenantAttributes == null ? null : tenantAttributes.getModuleTo();
    if (tenantAttributes != null && Boolean.TRUE.equals(tenantAttributes.getPurge())) {
      op = TenantOperation.DELETE;
    } else if (tenantExists) {
      op = TenantOperation.UPDATE;
      if (tenantAttributes != null) {
        previousVersion = tenantAttributes.getModuleFrom();
      }
    }

    SchemaMaker sMaker = new SchemaMaker(tenantId, PostgresClient.getModuleName(), op, previousVersion, newVersion);

    String tableInputStr = IOUtils.toString(tableInput, StandardCharsets.UTF_8);
    sMaker.setSchemaJson(tableInputStr);
    Schema schema = ObjectMapperTool.getMapper().readValue(tableInputStr, Schema.class);
    sMaker.setSchema(schema);
    sMaker.setPreviousSchema(previousSchema);
    String sqlFile = sMaker.generateDDL();
    log.debug("GENERATED SCHEMA " + sqlFile);
    return sqlFile;
  }

  private Future<String> sqlFile(Context context, String tenantId, TenantAttributes tenantAttributes,
                                 boolean tenantExists) {

    return previousSchema(context, tenantId, tenantExists)
        .compose(previousSchema -> {
          try {
            String sqlFile = sqlFile(tenantId, tenantExists, tenantAttributes, previousSchema);
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

  /**
   * @param tenantAttributes parameters like module version that may influence generated SQL
   * @return the SQL commands to create or upgrade the tenant's schema
   * @throws IllegalArgumentException when templates/db_scripts/schema.json doesn't exist
   * @throws TemplateException when processing templates/db_scripts/schema.json fails
   */
  public Future<String> sqlFile(Context context, String tenantId, TenantAttributes tenantAttributes) {
    return tenantExists(context, tenantId)
    .compose(tenantExists -> sqlFile(context, tenantId, tenantAttributes, tenantExists));
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
  public void postTenant(TenantAttributes tenantAttributes, RoutingContext routingContext, Map<String, String> headers,
                         Handler<AsyncResult<Response>> handler, Context context)  {
    postTenant(true, tenantAttributes, routingContext, headers, handler, context);
  }

  private void postTenant(boolean async, TenantAttributes tenantAttributes, RoutingContext routingContext, Map<String, String> headers,
                          Handler<AsyncResult<Response>> handler, Context context)  {

    String tenantId = TenantTool.tenantId(headers);
    String id = UUID.randomUUID().toString();
    TenantJob job = new TenantJob();
    job.setId(id);
    job.setTenant(tenantId);
    job.setTenantAttributes(tenantAttributes);
    job.setComplete(false);

    jobs.put(id, job);
    String location = (routingContext != null ? routingContext.request().uri() : "") + "/" + id;
    if (async) {
      handler.handle(Future.succeededFuture(PostTenantResponse.respond201WithApplicationJson(job,
          PostTenantResponse.headersFor201().withLocation(location))));
    }
    sqlFile(context, tenantId, tenantAttributes)
        .compose(sqlFile -> postgresClient(context).runSQLFile(sqlFile, true))
        .compose(res -> {
          if (!res.isEmpty()) {
            job.setMessages(res);
            return Future.failedFuture("SQL error");
          } else {
            return Future.succeededFuture();
          }
        })
        .compose(res -> {
          if (tenantAttributes == null) {
            return Future.succeededFuture();
          } else if (Boolean.TRUE.equals(tenantAttributes.getPurge())) {
            PostgresClient.closeAllClients(tenantId);
            return Future.succeededFuture();
          } else {
            return loadData(tenantAttributes, tenantId, headers, context);
          }
        })
        .onComplete(res -> {
          job.setComplete(true);
          jobs.put(id, job);
          if (res.failed()) {
            job.setError(res.cause().getMessage());
          }
          List<Promise<Void>> promises = waiters.remove(id);
          if (promises != null) {
            for (Promise<Void> promise : promises) {
              promise.tryComplete();
            }
          }
          if (!async) {
            handler.handle(Future.succeededFuture(PostTenantResponse.respond201WithApplicationJson(job,
                PostTenantResponse.headersFor201().withLocation(location))));
          }
        });
  }

  /**
   * Stub load sample/reference data.
   * @param attributes information about what to load.
   * @param tenantId tenant ID
   * @param headers HTTP headers for the request (Okapi)
   * @return
   */
  Future<Void> loadData(TenantAttributes attributes, String tenantId, Map<String, String> headers,
                        Context vertxContext) {
    return Future.succeededFuture();
  }

  void postTenantSync(TenantAttributes tenantAttributes, Map<String, String> headers,
                             Handler<AsyncResult<Response>> handler, Context context)  {
    postTenant(false, tenantAttributes, null, headers, handler, context);
  }

  @Override
  public void getTenantByOperationId(String operationId, int wait, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> handler, Context vertxContext) {
    TenantJob job = jobs.get(operationId);
    Response response;
    if (job == null) {
      response = GetTenantByOperationIdResponse.respond404WithTextPlain("Operation not found " + operationId);
    } else {
      if (wait > 0 && !Boolean.TRUE.equals(job.getComplete())) {
        Promise<Void> promise = Promise.promise();
        waiters.putIfAbsent(operationId, new LinkedList<>());
        waiters.get(operationId).add(promise);
        vertxContext.owner().setTimer(wait, res -> promise.tryComplete());
        promise.future().onComplete(res -> getTenantByOperationId(operationId, 0, okapiHeaders, handler, vertxContext));
        return;
      }
      response = GetTenantByOperationIdResponse.respond200WithApplicationJson(job);
    }
    handler.handle(Future.succeededFuture(response));
  }

  @Override
  public void deleteTenantByOperationId(String operationId, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> handler, Context vertxContext) {
    TenantJob job = jobs.get(operationId);
    Response response;
    if (job == null) {
      response = DeleteTenantByOperationIdResponse.respond404WithTextPlain("Operation not found " + operationId);
    } else {
      jobs.remove(operationId);
      response = DeleteTenantByOperationIdResponse.respond204();
    }
    handler.handle(Future.succeededFuture(response));
  }

}
