package org.folio.rest.impl;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.ddlgen.Schema;
import org.folio.rest.persist.ddlgen.SchemaMaker;
import org.folio.rest.persist.ddlgen.TenantOperation;
import org.folio.rest.tools.ClientGenerator;
import org.folio.rest.tools.PomReader;
import org.folio.rest.tools.messages.Messages;
import org.folio.rest.tools.utils.ObjectMapperTool;
import org.folio.rest.tools.utils.OutStream;
import org.folio.rest.tools.utils.TenantTool;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;


/**
 * @author shale
 *
 */
public class TenantAPI implements org.folio.rest.jaxrs.resource.TenantResource {

  public static final String       TABLE_JSON = "templates/db_scripts/schema.json";
  public static final String       DELETE_JSON = "templates/db_scripts/delete.json";

  private static final String      UPGRADE_FROM_VERSION          = "module_from";
  private static final String      UPGRADE_TO_VERSION            = "module_to";


  private static final Logger       log               = LoggerFactory.getLogger(TenantAPI.class);
  private final Messages            messages          = Messages.getInstance();


  @Validate
  @Override
  public void deleteTenant(Map<String, String> headers,
      Handler<AsyncResult<Response>> handlers, Context context) throws Exception {

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
                handlers.handle(io.vertx.core.Future.succeededFuture(DeleteTenantResponse.
                  withPlainBadRequest("Tenant does not exist: " + tenantId)));
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
/*              InputStream is = TenantAPI.class.getClassLoader().getResourceAsStream(DELETE_JSON);
              if(is == null){
                log.info("No delete json to use for deleting tenant " + tenantId);
                handlers.handle(io.vertx.core.Future.succeededFuture(DeleteTenantResponse.withNoContent()));
                return;
              }
              sqlFile = IOUtils.toString(is);*/
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
            PostgresClient.getInstance(context.owner()).runSQLFile(sqlFile, true,
                reply -> {
                  try {
                    String res = "";
                    if(reply.succeeded()){
                      res = new JsonArray(reply.result()).encodePrettily();
                      if(reply.result().size() > 0){
                        log.error("Unable to run the following commands during tenant delete: ");
                        reply.result().forEach(System.out::println);
                        handlers.handle(io.vertx.core.Future.succeededFuture(DeleteTenantResponse.withPlainBadRequest(res)));
                      }
                      else {
                        OutStream os = new OutStream();
                        os.setData(res);
                        handlers.handle(io.vertx.core.Future.succeededFuture(DeleteTenantResponse.withNoContent()));
                      }
                    }
                    else {
                      log.error(reply.cause().getMessage(), reply.cause());
                      handlers.handle(io.vertx.core.Future.succeededFuture(DeleteTenantResponse
                        .withPlainInternalServerError(reply.cause().getMessage())));
                    }
                  } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    handlers.handle(io.vertx.core.Future.succeededFuture(DeleteTenantResponse
                      .withPlainInternalServerError(e.getMessage())));
                  }
                });
          });
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        handlers.handle(io.vertx.core.Future.succeededFuture(DeleteTenantResponse
          .withPlainInternalServerError(e.getMessage())));
      }
    });
  }

  void tenantExists(Context context, String tenantId, Handler<AsyncResult<Boolean>> handler){
    /* connect as user in postgres-conf.json file (super user) - so that all commands will be available */
    PostgresClient.getInstance(context.owner()).select(
      "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = '"+ PostgresClient.convertToPsqlStandard(tenantId) +"');",
        reply -> {
          try {
            if(reply.succeeded()){
              handler.handle(io.vertx.core.Future.succeededFuture(reply.result().getResults().get(0).getBoolean(0)));
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
      Context context) throws Exception {

    context.runOnContext(v -> {
      try {

        String tenantId = TenantTool.calculateTenantId( headers.get(ClientGenerator.OKAPI_HEADER_TENANT) );
        log.info("sending... postTenant for " + tenantId);

        tenantExists(context, tenantId, res -> {
          boolean exists = false;
          if(res.succeeded()){
            exists = res.result();
            handlers.handle(io.vertx.core.Future.succeededFuture(GetTenantResponse.withPlainOK(String.valueOf(
              exists))));
          }
          else{
            log.error(res.cause().getMessage(), res.cause());
            handlers.handle(io.vertx.core.Future.succeededFuture(GetTenantResponse
              .withPlainInternalServerError(res.cause().getMessage())));
          }
        });
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        handlers.handle(io.vertx.core.Future.succeededFuture(GetTenantResponse
          .withPlainInternalServerError(e.getMessage())));
      }
    });
  }


  @Validate
  @Override
  public void postTenant(TenantAttributes entity, Map<String, String> headers,
      Handler<AsyncResult<Response>> handlers, Context context) throws Exception {

    /**
     * http://host:port/tenant
     * Validation by rmb means the entity is either properly populated on is null
     * depending on whether this is an upgrade or a create tenant
     */

    context.runOnContext(v -> {
      String tenantId = TenantTool.calculateTenantId(headers.get(ClientGenerator.OKAPI_HEADER_TENANT));
      log.info("sending... postTenant for " + tenantId);
      try {
        boolean isUpdateMode[] = new boolean[]{false};
        //body is optional so that the TenantAttributes
        if(entity != null){
          log.debug("upgrade from " + entity.getModuleFrom() + " to " + entity.getModuleTo());
          try {
            if(entity.getModuleFrom() != null){
              isUpdateMode[0] = true;
            }
          } catch (Exception e) {
            log.error(e.getMessage(), e);
            handlers.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse.
              withPlainBadRequest(e.getMessage())));
            return;
          }
        }

        tenantExists(context, tenantId,
          h -> {
            try {
              boolean tenantExists = false;
              if(h.succeeded()){
                tenantExists = h.result();
                if(tenantExists && !isUpdateMode[0]){
                  //tenant exists and a create tenant request was made, then this should do nothing
                  //if tenant exists then only update tenant request is acceptable
                  handlers.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse
                    .withNoContent()));
                  log.warn("Tenant already exists: " + tenantId);
                  return;
                }
                else if(!tenantExists && isUpdateMode[0]){
                  //update requested for a non-existant tenant
                  log.error("Can not update non-existant tenant " + tenantId);
                  handlers.handle(io.vertx.core.Future.succeededFuture(
                    PostTenantResponse.withPlainBadRequest(
                      "Update tenant requested for tenant " + tenantId + ", but tenant does not exist")));
                  return;
                }
                else{
                  log.info("adding/updating tenant " + tenantId);
                }
              }
              else{
                handlers.handle(io.vertx.core.Future.failedFuture(h.cause().getMessage()));
                log.error(h.cause().getMessage(), h.cause());
                return;
              }

              InputStream tableInput = TenantAPI.class.getClassLoader().getResourceAsStream(
                TABLE_JSON);

              if(tableInput == null) {
                handlers.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse
                  .withNoContent()));
                log.info("Could not find templates/db_scripts/schema.json , "
                    + " RMB will not run any scripts for " + tenantId);
                return;
              }

              TenantOperation op = TenantOperation.CREATE;

              String previousVersion = null;
              String newVersion = null;
              if(isUpdateMode[0]){
                op = TenantOperation.UPDATE;
                previousVersion = entity.getModuleFrom();
                newVersion = entity.getModuleTo();
              }

              SchemaMaker sMaker = new SchemaMaker(tenantId, PostgresClient.getModuleName(), op, previousVersion, newVersion);

              String tableInputStr = null;
              if(tableInput != null){
                tableInputStr = IOUtils.toString(tableInput, "UTF8");
                Schema schema = ObjectMapperTool.getMapper().readValue(tableInputStr, Schema.class);
                sMaker.setSchema(schema);
              }

              String sqlFile = sMaker.generateDDL();
              log.debug("GENERATED SCHEMA " + sqlFile);
              /* connect as user in postgres-conf.json file (super user) - so that all commands will be available */
              PostgresClient.getInstance(context.owner()).runSQLFile(sqlFile, true,
                reply -> {
                  try {
                    StringBuffer res = new StringBuffer();
                    if (reply.succeeded()) {
                      boolean failuresExist = false;
                      if(reply.result().size() > 0){
                        failuresExist = true;
                      }
                      res.append(new JsonArray(reply.result()).encodePrettily());
                      OutStream os = new OutStream();
                      if(failuresExist){
                        handlers.handle(io.vertx.core.Future.succeededFuture(
                          PostTenantResponse.withPlainBadRequest(res.toString())));
                      }
                      else{
                        os.setData(res);
                        handlers.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse.withJsonCreated(os)));
                      }
                    } else {
                      log.error(reply.cause().getMessage(), reply.cause());
                      handlers.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse.
                        withPlainInternalServerError(reply.cause().getMessage())));
                    }
                  } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    handlers.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse.
                      withPlainInternalServerError(e.getMessage())));
                  }
                });
            } catch (Exception e) {
              log.error(e.getMessage(), e);
              handlers.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse.
                withPlainInternalServerError(e.getMessage())));
            }
          });
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        handlers.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse.
          withPlainInternalServerError(e.getMessage())));
      }
    });
  }

  /**
   * @param jar
   * @return
   */
  private void validateJson(JsonObject jar) throws Exception {
    //System.out.println("jobj =................................. " + jar);
    if(!jar.containsKey(UPGRADE_FROM_VERSION)){
      throw new Exception(UPGRADE_FROM_VERSION + " entry does not exist in post tenant request body");
    }
  }

  private void toMap(TenantAttributes jar, List<String> placeHolders, List<String> values){
    try {
      placeHolders.add(UPGRADE_FROM_VERSION);
      placeHolders.add(UPGRADE_TO_VERSION);
      values.add(jar.getModuleFrom());
      values.add(jar.getModuleTo());
    } catch (Exception e) {
      log.warn("Unable to parse body", e);
    }
  }

}
