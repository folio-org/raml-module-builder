package org.folio.rest.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.jaxrs.resource.Tenant;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.ddlgen.Schema;
import org.folio.rest.persist.ddlgen.SchemaMaker;
import org.folio.rest.persist.ddlgen.TenantOperation;
import org.folio.rest.tools.ClientGenerator;
import org.folio.rest.tools.PomReader;
import org.folio.rest.tools.client.exceptions.ResponseException;
import org.folio.rest.tools.utils.ObjectMapperTool;
import org.folio.rest.tools.utils.OutStream;
import org.folio.rest.tools.utils.TenantTool;

import freemarker.template.TemplateException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
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

  PostgresClient postgresClient(Context context) {
    return PostgresClient.getInstance(context.owner());
  }

  @Validate
  @Override
  public void deleteTenant(Map<String, String> headers,
      Handler<AsyncResult<Response>> handlers, Context context) {

    context.runOnContext(v -> {
      try {

        String tenantId = TenantTool.calculateTenantId( headers.get(ClientGenerator.OKAPI_HEADER_TENANT) );
        log.info("sending... deleteTenant for " + tenantId);
        tenantExists(context, tenantId,
          h -> {
            boolean exists = false;
            if(h.succeeded()){
              exists = h.result();
              if(!exists){
                handlers.handle(failedFuture(DeleteTenantResponse.
                  respond400WithTextPlain("Tenant does not exist: " + tenantId)));
                log.error("Can not delete. Tenant does not exist: " + tenantId);
                return;
              }
              else{
                log.info("Deleting tenant " + tenantId);
              }
            }
            else{
              handlers.handle(io.vertx.core.Future.failedFuture(h.cause().getMessage()));
              log.error(h.cause().getMessage(), h.cause());
              return;
            }

            String sqlFile = null;
            try {
              SchemaMaker sMaker = new SchemaMaker(tenantId, PostgresClient.getModuleName(), TenantOperation.DELETE, null, PomReader.INSTANCE.getRmbVersion());
              sqlFile = sMaker.generateDDL();

            } catch (Exception e1) {
              handlers.handle(io.vertx.core.Future.failedFuture(e1.getMessage()));
              log.error(e1.getMessage(), e1);
              return;
            }

            log.info("Attempting to run delete script for: " + tenantId);
            log.debug("GENERATED SCHEMA " + sqlFile);
            /* connect as user in postgres-conf.json file (super user) - so that all commands will be available */
            postgresClient(context).runSQLFile(sqlFile, true,
                reply -> {
                  try {
                    String res = "";
                    if(reply.succeeded()){
                      res = new JsonArray(reply.result()).encodePrettily();
                      if(reply.result().size() > 0){
                        log.error("Unable to run the following commands during tenant delete: ");
                        reply.result().forEach(System.out::println);
                        handlers.handle(failedFuture(DeleteTenantResponse.respond400WithTextPlain(res)));
                      }
                      else {
                        OutStream os = new OutStream();
                        os.setData(res);
                        handlers.handle(io.vertx.core.Future.succeededFuture(DeleteTenantResponse.respond204()));
                      }
                    }
                    else {
                      log.error(reply.cause().getMessage(), reply.cause());
                      handlers.handle(failedFuture(DeleteTenantResponse
                        .respond500WithTextPlain(reply.cause().getMessage())));
                    }
                  } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    handlers.handle(failedFuture(DeleteTenantResponse
                      .respond500WithTextPlain(e.getMessage())));
                  }
                });
          });
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        handlers.handle(failedFuture(DeleteTenantResponse
          .respond500WithTextPlain(e.getMessage())));
      }
    });
  }

  Future<Boolean> tenantExists(Context context, String tenantId) {
    Promise<Boolean> promise = Promise.promise();
    tenantExists(context, tenantId, promise.future());
    return promise.future();
  }

  void tenantExists(Context context, String tenantId, Handler<AsyncResult<Boolean>> handler){
    /* connect as user in postgres-conf.json file (super user) - so that all commands will be available */
    postgresClient(context).select(
      "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = '"+ PostgresClient.convertToPsqlStandard(tenantId) +"');",
        reply -> {
          try {
            if(reply.succeeded()){
              handler.handle(io.vertx.core.Future.succeededFuture(reply.result().iterator().next().getBoolean(0)));
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

  @Validate
  @Override
  public void getTenant(Map<String, String> headers, Handler<AsyncResult<Response>> handlers,
      Context context)  {

    context.runOnContext(v -> {
      try {

        String tenantId = TenantTool.calculateTenantId( headers.get(ClientGenerator.OKAPI_HEADER_TENANT) );
        log.info("sending... getTenant for " + tenantId);

        tenantExists(context, tenantId, res -> {
          boolean exists = false;
          if(res.succeeded()){
            exists = res.result();
            handlers.handle(io.vertx.core.Future.succeededFuture(GetTenantResponse.respond200WithTextPlain(String.valueOf(
              exists))));
          }
          else{
            log.error(res.cause().getMessage(), res.cause());
            handlers.handle(failedFuture(GetTenantResponse
              .respond500WithTextPlain(res.cause().getMessage())));
          }
        });
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        handlers.handle(failedFuture(GetTenantResponse
          .respond500WithTextPlain(e.getMessage())));
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
    if (tenantExists) {
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
      }
    });
  }

  /**
   * @param tenantAttributes parameters like module version that may influence generated SQL
   * @return the SQL commands to create or upgrade the tenant's schema
   * @throws NoSchemaJsonException when templates/db_scripts/schema.json doesn't exist
   * @throws TemplateException when processing templates/db_scripts/schema.json fails
   */
  public Future<String> sqlFile(Context context, String tenantId, TenantAttributes tenantAttributes) {
    return tenantExists(context, tenantId)
    .compose(tenantExists -> sqlFile(context, tenantId, tenantAttributes, tenantExists));
  }

  @Validate
  @Override
  public void postTenant(TenantAttributes tenantAttributes, Map<String, String> headers,
      Handler<AsyncResult<Response>> handlers, Context context)  {

    String tenantId = TenantTool.tenantId(headers);
    log.info("sending... postTenant for " + tenantId);
    if (tenantAttributes != null) {
      log.debug("upgrade from " + tenantAttributes.getModuleFrom() + " to " + tenantAttributes.getModuleTo());
    }

    Future<Boolean> tenantExistsFuture = tenantExists(context, tenantId);
    tenantExistsFuture
    .compose(tenantExists -> sqlFile(context, tenantId, tenantAttributes, tenantExists))
    .compose(sqlFile -> postgresClient(context).runSQLFile(sqlFile, true))
    .map(failedStatements -> {
      String jsonListOfFailures = new JsonArray(failedStatements).encodePrettily();
      if (! failedStatements.isEmpty()) {
        return PostTenantResponse.respond400WithTextPlain(jsonListOfFailures);
      }
      boolean tenantExists = tenantExistsFuture.result();
      return tenantExists
              ? PostTenantResponse.respond200WithApplicationJson(jsonListOfFailures)
              : PostTenantResponse.respond201WithApplicationJson(jsonListOfFailures);
    })

    .onFailure(e -> {
      if (e instanceof NoSchemaJsonException) {
        handlers.handle(Future.succeededFuture(PostTenantResponse.respond204()));
        return;
      }
      log.error(e.getMessage(), e);
      String text = e.getMessage() + "\n" + ExceptionUtils.getStackTrace(e);
      Response response = PostTenantResponse.respond500WithTextPlain(text);
      handlers.handle(failedFuture(response));
    })
    .onSuccess(response -> {
      if (response.getStatus() >= 300) {
        handlers.handle(failedFuture(response));
        return;
      }
      handlers.handle(Future.succeededFuture(response));
    });
  }

  /**
   * @return a failed {@link Future} where the failure cause is a {@link ResponseException}
   *         containing the {@code response}
   */
  static Future<Response> failedFuture(Response response) {
    return Future.failedFuture(new ResponseException(response));
  }
}
