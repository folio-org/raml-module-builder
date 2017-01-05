package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.LoggerFactory;

import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.Map;
import java.util.function.BiConsumer;

import javax.mail.BodyPart;
import javax.mail.internet.MimeMultipart;
import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.folio.rest.RestVerticle;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.resource.AdminResource;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.security.AES;
import org.folio.rest.tools.utils.LRUCache;
import org.folio.rest.tools.utils.LogUtil;
import org.folio.rest.tools.utils.OutStream;
import org.folio.rest.tools.utils.TenantTool;

public class AdminAPI implements AdminResource {

  private static final io.vertx.core.logging.Logger log              = LoggerFactory.getLogger(AdminAPI.class);
  // format of the percentages returned by the /memory api
  private static final DecimalFormat                DECFORMAT        = new DecimalFormat("###.##");
  private static LRUCache<Date, String>             jvmMemoryHistory = LRUCache.newInstance(100);

  @Validate
  @Override
  public void putAdminLoglevel(Level level, String javaPackage, java.util.Map<String, String>okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    try {
      JsonObject updatedLoggers = LogUtil.updateLogConfiguration(javaPackage, level2level(level));
      OutStream os = new OutStream();
      os.setData(updatedLoggers);
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutAdminLoglevelResponse.withJsonOK(os)));
    } catch (Exception e) {
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutAdminLoglevelResponse.withPlainInternalServerError("ERROR"
          + e.getMessage())));
      log.error(e.getMessage(), e);
    }

  }

  @Validate
  @Override
  public void getAdminLoglevel(java.util.Map<String, String>okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    try {
      JsonObject loggers = LogUtil.getLogConfiguration();
      OutStream os = new OutStream();
      os.setData(loggers);
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutAdminLoglevelResponse.withJsonOK(os)));
    } catch (Exception e) {
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutAdminLoglevelResponse.withPlainInternalServerError("ERROR"
          + e.getMessage())));
      log.error(e.getMessage(), e);
    }
  }

  private java.util.logging.Level level2level(Level level) {
    return java.util.logging.Level.parse(level.name());
  }

  @Override
  public void putAdminJstack(java.util.Map<String, String>okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {
    // TODO Auto-generated method stub

  }

  @Override
  public void getAdminJstack(java.util.Map<String, String>okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    final StringBuilder dump = new StringBuilder();
    vertxContext.owner().executeBlocking(
      code -> {
        try {
          dump.append("<html><body>");
          final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
          final ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(
            threadMXBean.getAllThreadIds(), 100);
          for (ThreadInfo threadInfo : threadInfos) {
            dump.append(threadInfo.getThreadName());
            final Thread.State state = threadInfo.getThreadState();
            dump.append("</br>   java.lang.Thread.State: ");
            dump.append(state);
            final StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
            for (final StackTraceElement stackTraceElement : stackTraceElements) {
              dump.append("</br>        at ");
              dump.append(stackTraceElement);
            }
            dump.append("</br></br>");
          }
          dump.append("</body></html>");
          code.complete(dump);
        } catch (Exception e) {
          log.error(e);
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetAdminJstackResponse.withPlainInternalServerError("ERROR"
              + e.getMessage())));
        }
      },
      result -> {
        asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetAdminJstackResponse.withHtmlOK(result.result().toString())));
      });
  }

  @Override
  public void getAdminMemory(boolean history, java.util.Map<String, String>okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {
    final StringBuilder dump = new StringBuilder();
    vertxContext.owner().executeBlocking(
      code -> {
        try {
          dump.append("<br><table>");
          for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            MemoryUsage mem = pool.getCollectionUsage();
            MemoryUsage curMem = pool.getUsage();
            MemoryUsage peakMem = pool.getPeakUsage();
            long usageAfterGC = -1;
            long currentUsage = -1;
            double usageAfterGCPercent = -1;
            double currentUsagePercent = -1;
            long peakUsage = -1;
            if (mem != null) {
              usageAfterGC = mem.getUsed() / 1024 / 1024;
              usageAfterGCPercent = (double) mem.getUsed() / (double) mem.getCommitted() * 100; // Mimic jstat behavior
            }
            if (curMem != null) {
              currentUsage = curMem.getUsed() / 1024 / 1024;
              if (curMem.getMax() > 0) {
                currentUsagePercent = (double) curMem.getUsed() / (double) curMem.getCommitted()
                    * 100; // Mimic jstat behavior
              }
            }
            if (peakMem != null) {
              peakUsage = peakMem.getUsed() / 1024 / 1024;
            }
            dump.append("<tr><td>name: ").append(pool.getName()).append("   </td>").append(
              "<td>memory usage after latest gc: <b>").append(usageAfterGC).append(
              "</b>MB.   </td>").append("<td>type: ").append(pool.getType()).append("   </td>").append(
              "<td>estimate of memory usage: <b>").append(currentUsage).append("</b>MB.   </td>").append(
              "<td>peak usage: ").append(peakUsage).append("MB.   </td>").append(
              "<td> % used memory after GC: <b>").append(DECFORMAT.format(usageAfterGCPercent)).append(
              "</b>  </td>").append("<td> % used memory current: <b>").append(
              DECFORMAT.format(currentUsagePercent)).append("</b>   </td></tr>");
          }
          dump.append("</table><br><br>");
          final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
          final MemoryUsage memInfo = memoryMXBean.getHeapMemoryUsage();
          long memCommittedToJVMByOS = memInfo.getCommitted();
          long memUsedByJVM = memInfo.getUsed();
          dump.append("<b>Total: </b> Memory used/available(MB): ").append(
            (memUsedByJVM / 1024 / 1024)).append("/").append(memCommittedToJVMByOS / 1024 / 1024).append(
            "<br>");
          if (history) {
            StringBuilder historyMem = new StringBuilder();
            jvmMemoryHistory.put(new Date(), dump.toString());
            BiConsumer<Date, String> biConsumer = (key, value) -> historyMem.append(key.toInstant().toString()
                + "<br>" + value);
            jvmMemoryHistory.forEach(biConsumer);
            code.complete(historyMem);
          } else {
            jvmMemoryHistory.clear();
            code.complete(dump);
          }
        } catch (Exception e) {
          log.error(e);
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetAdminMemoryResponse.withPlainInternalServerError("ERROR"
              + e.getMessage())));
        }
      },
      result -> {
        asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetAdminMemoryResponse.withHtmlOK(result.result().toString())));
      });
  }

  @Validate
  @Override
  public void postAdminUploadmultipart(PersistMethod persistMethod, String busAddress,
      String fileName, MimeMultipart entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    if(entity != null){
      int parts = entity.getCount();
      for (int i = 0; i < parts; i++) {
        BodyPart bp = entity.getBodyPart(i);
        System.out.println(bp.getFileName());
        //System.out.println(bp.getContent());
        //System.out.println("-----------------------------------------");
      }
    }
    String name = "";
    try{
      if(fileName == null){
        name = entity.getBodyPart(0).getFileName();
      }
      else{
        name = fileName;
      }
    }
    catch(Exception e){}

    asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PostAdminUploadmultipartResponse.withOK("TODO"
        )));
  }

  @Validate
  @Override
  public void postAdminImportSQL(InputStream entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    String sqlFile = IOUtils.toString(entity, "UTF8");
    PostgresClient.getInstance(vertxContext.owner()).runSQLFile(sqlFile, false, reply -> {
      if(reply.succeeded()){
        reply.result();
        asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PostAdminImportSQLResponse.withOK("")));
      }
      else{
        asyncResultHandler.handle(io.vertx.core.Future.failedFuture(reply.cause().getMessage()));
      }
    });

  }

  @Validate
  @Override
  public void getAdminPostgresActiveSessions(String dbname, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    PostgresClient.getInstance(vertxContext.owner(), "public").select("SELECT pid , usename, "
        + "application_name, client_addr, client_hostname, "
        + "query, state from pg_stat_activity where datname='"+dbname+"'", reply -> {

          if(reply.succeeded()){

            OutStream stream = new OutStream();
            stream.setData(reply.result().toJson());

            asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetAdminPostgresActiveSessionsResponse.
              withJsonOK(stream)));
          }
          else{
            log.error(reply.cause().getMessage(), reply.cause());
            asyncResultHandler.handle(io.vertx.core.Future.failedFuture(reply.cause().getMessage()));
          }
        });
  }

  @Validate
  @Override
  public void getAdminPostgresLoad(String dbname, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    PostgresClient.getInstance(vertxContext.owner()).select("SELECT pg_stat_reset()", reply -> {

          if(reply.succeeded()){
            /* wait 10 seconds for stats to gather and then query stats table for info */
            vertxContext.owner().setTimer(10000, new Handler<Long>() {
              @Override
              public void handle(Long timerID) {
                PostgresClient.getInstance(vertxContext.owner(), "public").select(
                    "SELECT numbackends as CONNECTIONS, xact_commit as TX_COMM, xact_rollback as "
                    + "TX_RLBCK, blks_read + blks_hit as READ_TOTAL, "
                    + "blks_hit * 100 / (blks_read + blks_hit) "
                    + "as BUFFER_HIT_PERCENT FROM pg_stat_database WHERE datname = '"+dbname+"'", reply2 -> {
                  if(reply2.succeeded()){
                    OutStream stream = new OutStream();
                    stream.setData(reply2.result().toJson());
                    asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetAdminPostgresLoadResponse.
                      withJsonOK(stream)));
                  }
                  else{
                    log.error(reply2.cause().getMessage(), reply2.cause());
                    asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetAdminPostgresLoadResponse.
                      withPlainInternalServerError(reply2.cause().getMessage())));
                  }
                });
              }
            });
          }
          else{
            log.error(reply.cause().getMessage(), reply.cause());
            asyncResultHandler.handle(io.vertx.core.Future.failedFuture(reply.cause().getMessage()));
          }
        });
  }

  @Validate
  @Override
  public void getAdminPostgresTableAccessStats(Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    PostgresClient.getInstance(vertxContext.owner()).select(
        "SELECT schemaname,relname,seq_scan,idx_scan,cast(idx_scan "
        + "AS numeric) / (idx_scan + seq_scan) AS idx_scan_pct "
        + "FROM pg_stat_user_tables WHERE (idx_scan + seq_scan)>0 "
        + "ORDER BY idx_scan_pct;", reply -> {

          if(reply.succeeded()){

            OutStream stream = new OutStream();
            stream.setData(reply.result().toJson());

            asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetAdminPostgresTableAccessStatsResponse.
              withJsonOK(stream)));
          }
          else{
            log.error(reply.cause().getMessage(), reply.cause());
            asyncResultHandler.handle(io.vertx.core.Future.failedFuture(reply.cause().getMessage()));
          }
        });
  }

  @Validate
  @Override
  public void getAdminPostgresTableSize(String dbname, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    PostgresClient.getInstance(vertxContext.owner()).select(
      "SELECT relname as \"Table\", pg_size_pretty(pg_relation_size(relid)) As \" Table Size\","
      + " pg_size_pretty(pg_total_relation_size(relid) - pg_relation_size(relid)) as \"Index Size\""
      + " FROM pg_catalog.pg_statio_user_tables ORDER BY pg_total_relation_size(relid) DESC;", reply -> {
        if(reply.succeeded()){

          OutStream stream = new OutStream();
          stream.setData(reply.result().toJson());

          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetAdminPostgresTableAccessStatsResponse.
            withJsonOK(stream)));
        }
        else{
          log.error(reply.cause().getMessage(), reply.cause());
          asyncResultHandler.handle(io.vertx.core.Future.failedFuture(reply.cause().getMessage()));
        }
      });

  }

  @Validate
  @Override
  public void postAdminSetAESKey(String key, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    AES.setSecretKey(key);
    asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PostAdminSetAESKeyResponse.withNoContent()));
  }

  @Override
  public void postAdminGetPassword(String key, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    String tenantId = TenantTool.calculateTenantId( okapiHeaders.get(RestVerticle.OKAPI_HEADER_TENANT) );

    if(tenantId == null){
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
        PostAdminGetPasswordResponse.withPlainBadRequest("Tenant id is null")));
    }
    else{
      try {
        String password = AES.encryptPasswordAsBase64(tenantId, AES.getSecretKeyObject(key));
        asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PostAdminGetPasswordResponse.withPlainOK(password)));
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
          PostAdminGetPasswordResponse.withPlainInternalServerError(e.getMessage())));
      }

    }

  }

  @Override
  public void getAdminTableIndexUsage(Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    PostgresClient.getInstance(vertxContext.owner()).select(
      "SELECT relname, 100 * idx_scan / (seq_scan + idx_scan) percent_of_times_index_used, n_live_tup rows_in_table "+
      "FROM pg_stat_user_tables "+
      "ORDER BY n_live_tup DESC;", reply -> {
        if(reply.succeeded()){

          OutStream stream = new OutStream();
          stream.setData(reply.result().toJson());

          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetAdminTableIndexUsageResponse.
            withJsonOK(stream)));
        }
        else{
          log.error(reply.cause().getMessage(), reply.cause());
          asyncResultHandler.handle(io.vertx.core.Future.failedFuture(reply.cause().getMessage()));
        }
      });
  }

  @Override
  public void getAdminCacheHitRates(Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    try {
      PostgresClient.getInstance(vertxContext.owner()).select(
        "SELECT sum(heap_blks_read) as heap_read, sum(heap_blks_hit)  as heap_hit,"
        + " (sum(heap_blks_hit) - sum(heap_blks_read)) / sum(heap_blks_hit) as ratio "
        + "FROM pg_statio_user_tables;", reply -> {
          if(reply.succeeded()){

            OutStream stream = new OutStream();
            stream.setData(reply.result().toJson());

            asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetAdminCacheHitRatesResponse.
              withJsonOK(stream)));
          }
          else{
            log.error(reply.cause().getMessage(), reply.cause());
            asyncResultHandler.handle(io.vertx.core.Future.failedFuture(reply.cause().getMessage()));
          }
        });
    } catch (Exception e) {
      log.error(e.getMessage());
      asyncResultHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
    }
  }

  @Validate
  @Override
  public void getAdminSlowQueries(int querytimerunning, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    /** the queries returned are of this backend's most recent query. If state is active this field shows the currently
     * executing query. In all other states, it shows the last query that was executed.*/
    PostgresClient.getInstance(vertxContext.owner()).select(
      "SELECT EXTRACT(EPOCH FROM now() - query_start) as runtime, client_addr, usename, datname, waiting, state, query "+
      "FROM  pg_stat_activity " +
      "WHERE now() - query_start > '"+querytimerunning+" seconds'::interval "+
      "ORDER BY runtime DESC;", reply -> {
        if(reply.succeeded()){

          OutStream stream = new OutStream();
          stream.setData(reply.result().toJson());

          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetAdminSlowQueriesResponse.
            withJsonOK(stream)));
        }
        else{
          log.error(reply.cause().getMessage(), reply.cause());
          asyncResultHandler.handle(io.vertx.core.Future.failedFuture(reply.cause().getMessage()));
        }
      });
  }

}
