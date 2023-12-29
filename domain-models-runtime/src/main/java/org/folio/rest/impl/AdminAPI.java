package org.folio.rest.impl;

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

import javax.ws.rs.core.Response;

import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.RowSet;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.RestVerticle;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.AdminLoglevelPutLevel;
import org.folio.rest.jaxrs.model.AdminPostgresMaintenancePostCommand;
import org.folio.rest.jaxrs.resource.Admin;
import org.folio.rest.persist.PostgresClient;
import org.folio.dbschema.Schema;
import org.folio.rest.persist.ddlgen.SchemaMaker;
import org.folio.dbschema.TenantOperation;
import org.folio.rest.security.AES;
import org.folio.rest.tools.utils.LRUCache;
import org.folio.rest.tools.utils.LogUtil;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.rest.tools.utils.OutStream;
import org.folio.rest.tools.utils.TenantTool;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;

public class AdminAPI implements Admin {

  private static final Logger log = LogManager.getLogger(AdminAPI.class);
  // format of the percentages returned by the /memory api
  private static final DecimalFormat                DECFORMAT        = new DecimalFormat("###.##");
  private static LRUCache<Date, String>             jvmMemoryHistory = LRUCache.newInstance(100);

  @Validate
  @Override
  public void putAdminLoglevel(AdminLoglevelPutLevel level, String javaPackage, java.util.Map<String, String>okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    try {
      JsonObject updatedLoggers = LogUtil.updateLogConfiguration(javaPackage, level.name());
      OutStream os = new OutStream();
      os.setData(updatedLoggers);
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutAdminLoglevelResponse.respond200WithApplicationJson(os)));
    } catch (Exception e) {
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutAdminLoglevelResponse.respond500WithTextPlain("ERROR"
          + e.getMessage())));
      log.error(e.getMessage(), e);
    }

  }

  @Validate
  @Override
  public void getAdminLoglevel(java.util.Map<String, String>okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    try {
      JsonObject loggers = LogUtil.getLogConfiguration();
      OutStream os = new OutStream();
      os.setData(loggers);
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutAdminLoglevelResponse.respond200WithApplicationJson(os)));
    } catch (Exception e) {
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutAdminLoglevelResponse.respond500WithTextPlain("ERROR"
          + e.getMessage())));
      log.error(e.getMessage(), e);
    }
  }

  @Override
  public void putAdminJstack(java.util.Map<String, String>okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutAdminJstackResponse.respond500WithTextPlain("NOT IMPLEMENTED")));
  }

  @Override
  public void getAdminJstack(java.util.Map<String, String>okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

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
          log.error(e.getMessage(), e);
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetAdminJstackResponse.respond500WithTextPlain("ERROR"
              + e.getMessage())));
        }
      },
      result -> {
        asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetAdminJstackResponse.respond200WithTextHtml(result.result().toString())));
      });
  }

  @Validate
  @Override
  public void getAdminMemory(boolean history, java.util.Map<String, String>okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
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
          log.error(e.getMessage(), e);
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetAdminMemoryResponse.respond500WithTextPlain("ERROR"
              + e.getMessage())));
        }
      },
      result -> {
        asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetAdminMemoryResponse.respond200WithTextHtml(result.result().toString())));
      });
  }

  @Validate
  @Override
  public void postAdminImportSQL(InputStream entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    //TODO BUG, if database is down, this wont get caught and will return an OK
    //THE sql file must be tenant specific, meaning, to insert into a table the file should
    //have any table name prefixed with the schema - schema.table_name

    try {
      String tenantId = TenantTool.tenantId(okapiHeaders);
      String sqlFile = IOUtils.toString(entity, "UTF8");
      PostgresClient.getInstance(vertxContext.owner(), tenantId).runSqlFile(sqlFile)
      .onFailure(e -> {
        log.error("{}", e.getMessage(), e);
        asyncResultHandler.handle(Future.succeededFuture(
            PostAdminImportSQLResponse.respond400WithTextPlain(e.getMessage())));
      })
      .onSuccess(x -> asyncResultHandler.handle(Future.succeededFuture(
          PostAdminImportSQLResponse.respond200(PostAdminImportSQLResponse.headersFor200())))
      );
    } catch (Exception e) {
      log.error("{}", e.getMessage(), e);
      asyncResultHandler.handle(Future.succeededFuture(
          PostAdminImportSQLResponse.respond500WithTextPlain(e.getMessage())));
    }
  }

  @Validate
  @Override
  public void getAdminPostgresActiveSessions(String dbname, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    PostgresClient.getInstance(vertxContext.owner()).select("SELECT pid , usename, "
        + "application_name, client_addr::text, client_hostname, "
        + "query, state from pg_stat_activity where datname='"+dbname+"'", reply -> {

          if (reply.succeeded()){
            asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetAdminPostgresActiveSessionsResponse.
              respond200WithApplicationJson(new OutStream(reply.result()))));
          } else {
            log.error(reply.cause().getMessage(), reply.cause());
            asyncResultHandler.handle(io.vertx.core.Future.failedFuture(reply.cause().getMessage()));
          }
        });
  }

  @Validate
  @Override
  public void getAdminPostgresLoad(String dbname, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    PostgresClient.getInstance(vertxContext.owner()).select("SELECT pg_stat_reset()", reply -> {

          if (reply.succeeded()){
            /* wait 10 seconds for stats to gather and then query stats table for info */
            vertxContext.owner().setTimer(10000, new Handler<Long>() {
              @Override
              public void handle(Long timerID) {
                PostgresClient.getInstance(vertxContext.owner()).select(
                    "SELECT numbackends as CONNECTIONS, xact_commit as TX_COMM, xact_rollback as "
                    + "TX_RLBCK, blks_read + blks_hit as READ_TOTAL, "
                    + "blks_hit * 100 / (1 + blks_read + blks_hit) "
                    + "as BUFFER_HIT_PERCENT FROM pg_stat_database WHERE datname = '"+dbname+"'", reply2 -> {
                  if (reply2.succeeded()) {
                    asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetAdminPostgresLoadResponse.
                      respond200WithApplicationJson(new OutStream(reply2.result()))));
                  } else {
                    log.error(reply2.cause().getMessage(), reply2.cause());
                    asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetAdminPostgresLoadResponse.
                      respond500WithTextPlain(reply2.cause().getMessage())));
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
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    String sql =
        "SELECT schemaname,relname,seq_scan,idx_scan,"
        + "     cast(idx_scan AS numeric) / (idx_scan + seq_scan) AS idx_scan_pct "
        + "FROM (SELECT schemaname, relname, COALESCE(seq_scan, 0) seq_scan, COALESCE(idx_scan, 0) idx_scan "
        + "      FROM pg_stat_user_tables) x "
        + "WHERE idx_scan + seq_scan > 0 "
        + "ORDER BY idx_scan_pct;";
    PostgresClient.getInstance(vertxContext.owner()).select(sql, reply -> {

          if (reply.succeeded()){
            asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetAdminPostgresTableAccessStatsResponse.
              respond200WithApplicationJson(new OutStream(reply.result()))));
          } else {
            log.error(reply.cause().getMessage(), reply.cause());
            asyncResultHandler.handle(io.vertx.core.Future.failedFuture(reply.cause().getMessage()));
          }
        });
  }

  @Validate
  @Override
  public void getAdminPostgresTableSize(String dbname, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    PostgresClient.getInstance(vertxContext.owner()).select(
      "SELECT schemaname as \"Schema\","
      + " relname as \"Table\","
      + " pg_size_pretty(pg_relation_size(relid)) As \" Table Size\","
      + " pg_size_pretty(pg_total_relation_size(relid) - pg_relation_size(relid)) as \"Index Size\""
      + " FROM pg_catalog.pg_statio_user_tables ORDER BY pg_total_relation_size(relid) DESC;", reply -> {
        if (reply.succeeded()){
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetAdminPostgresTableAccessStatsResponse.
            respond200WithApplicationJson(new OutStream(reply.result()))));
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
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    AES.setSecretKey(key);
    asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PostAdminSetAESKeyResponse.respond204()));
  }

  @Validate
  @Override
  public void postAdminGetPassword(String key, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    String tenantId = TenantTool.calculateTenantId( okapiHeaders.get(RestVerticle.OKAPI_HEADER_TENANT) );

    if(tenantId == null){
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
        PostAdminGetPasswordResponse.respond400WithTextPlain("Tenant id is null")));
    }
    else{
      try {
        String password = AES.encryptPasswordAsBase64(tenantId, AES.getSecretKeyObject(key));
        asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PostAdminGetPasswordResponse.respond200WithTextPlain(password)));
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
          PostAdminGetPasswordResponse.respond500WithTextPlain(e.getMessage())));
      }

    }

  }

  @Override
  public void getAdminTableIndexUsage(Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    PostgresClient.getInstance(vertxContext.owner()).select(
      "SELECT schemaname, relname, 100 * idx_scan / NULLIF(seq_scan + idx_scan, 0) percent_of_times_index_used, "+
      "       n_live_tup rows_in_table "+
      "FROM (SELECT schemaname, relname, COALESCE(seq_scan, 0) seq_scan, COALESCE(idx_scan, 0) idx_scan, n_live_tup "+
      "      FROM pg_stat_user_tables) x "+
      "ORDER BY n_live_tup DESC;", reply -> {
        if(reply.succeeded()){

          OutStream stream = new OutStream(reply.result());

          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetAdminTableIndexUsageResponse.
            respond200WithApplicationJson(stream)));
        }
        else{
          log.error(reply.cause().getMessage(), reply.cause());
          asyncResultHandler.handle(io.vertx.core.Future.failedFuture(reply.cause().getMessage()));
        }
      });
  }

  @Override
  public void getAdminCacheHitRates(Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    try {
      PostgresClient.getInstance(vertxContext.owner()).select(
        "SELECT sum(heap_blks_read) as heap_read, sum(heap_blks_hit)  as heap_hit,"
        + " (sum(heap_blks_hit) - sum(heap_blks_read)) / NULLIF(sum(heap_blks_hit),0) as ratio "
        + "FROM pg_statio_user_tables;", reply -> {
          if(reply.succeeded()){

            OutStream stream = new OutStream(reply.result());

            asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetAdminCacheHitRatesResponse.
              respond200WithApplicationJson(stream)));
          }
          else{
            log.error(reply.cause().getMessage(), reply.cause());
            asyncResultHandler.handle(io.vertx.core.Future.failedFuture(reply.cause().getMessage()));
          }
        });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      asyncResultHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
    }
  }

  @Validate
  @Override
  public void getAdminSlowQueries(int querytimerunning, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    /** the queries returned are of this backend's most recent query. If state is active this field shows the currently
     * executing query. In all other states, it shows the last query that was executed.*/
    PostgresClient.getInstance(vertxContext.owner()).select(
      "SELECT EXTRACT(EPOCH FROM now() - query_start) as runtime, client_addr, usename, datname, state, query "+
      "FROM  pg_stat_activity " +
      "WHERE now() - query_start > '"+querytimerunning+" seconds'::interval "+
      "ORDER BY runtime DESC;", reply -> {
        if(reply.succeeded()){

          OutStream stream = new OutStream(reply.result());

          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetAdminSlowQueriesResponse.
            respond200WithApplicationJson(stream)));
        }
        else{
          log.error(reply.cause().getMessage(), reply.cause());
          asyncResultHandler.handle(io.vertx.core.Future.failedFuture(reply.cause().getMessage()));
        }
      });
  }

  @Override
  public void getAdminHealth(Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    OutStream stream = new OutStream();
    stream.setData("OK");
    asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetAdminHealthResponse.respond200WithAnyAny(stream)));

  }

  @Validate
  @Override
  public void getAdminTotalDbSize(String dbname, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    PostgresClient.getInstance(vertxContext.owner()).select(
      "select pg_size_pretty(pg_database_size('"+dbname+"')) as db_size", reply -> {
        if(reply.succeeded()){

          OutStream stream = new OutStream(reply.result());

          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetAdminTotalDbSizeResponse.
            respond200WithApplicationJson(stream)));
        }
        else{
          log.error(reply.cause().getMessage(), reply.cause());
          asyncResultHandler.handle(io.vertx.core.Future.failedFuture(reply.cause().getMessage()));
        }
      });
  }

  @Override
  public void getAdminDbCacheSummary(Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    PostgresClient.getInstance(vertxContext.owner()).select(
      "CREATE EXTENSION IF NOT EXISTS \"pg_buffercache\" WITH SCHEMA public", reply1 -> {
        if(reply1.succeeded()){
          PostgresClient.getInstance(vertxContext.owner()).select(
            "SELECT c.relname, pg_size_pretty(count(*) * 8192) as buffered, round(100.0 * count(*) / "+
                "(SELECT setting FROM pg_settings WHERE name='shared_buffers')::integer,1) AS buffers_percent,"+
                "round(100.0 * count(*) * 8192 / pg_relation_size(c.oid),1) AS percent_of_relation FROM pg_class c " +
                "INNER JOIN pg_buffercache b ON b.relfilenode = c.relfilenode INNER JOIN pg_database d "+
                "ON (b.reldatabase = d.oid AND d.datname = current_database()) GROUP BY c.oid,c.relname "+
                "ORDER BY 3 DESC LIMIT 20;", reply2 -> {
              if (reply2.succeeded()){
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetAdminDbCacheSummaryResponse.
                  respond200WithApplicationJson(new OutStream(reply2.result()))));
              } else {
                log.error(reply2.cause().getMessage(), reply2.cause());
                asyncResultHandler.handle(io.vertx.core.Future.failedFuture(reply2.cause().getMessage()));
              }
            });
        }
        else{
          log.error(reply1.cause().getMessage(), reply1.cause());
          asyncResultHandler.handle(io.vertx.core.Future.failedFuture(reply1.cause().getMessage()));
        }
      });
  }

  @Validate
  @Override
  public void getAdminListLockingQueries(String dbname, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    PostgresClient.getInstance(vertxContext.owner()).select(
      "SELECT blockedq.pid AS blocked_pid, blockedq.query as blocked_query, "
      + "blockingq.pid AS blocking_pid, blockingq.query as blocking_query FROM pg_catalog.pg_locks blocked "
      + "JOIN pg_stat_activity blockedq ON blocked.pid = blockedq.pid "
      + "JOIN pg_catalog.pg_locks blocking ON (blocking.transactionid=blocked.transactionid AND blocked.pid != blocking.pid) "
      + "JOIN pg_stat_activity blockingq ON blocking.pid = blockingq.pid "
      + "WHERE NOT blocked.granted AND blockingq.datname='"+dbname+"';",
      reply -> {
        if (reply.succeeded()){
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetAdminListLockingQueriesResponse.
            respond200WithApplicationJson(new OutStream(reply.result()))));
        }
        else{
          log.error(reply.cause().getMessage(), reply.cause());
          asyncResultHandler.handle(io.vertx.core.Future.failedFuture(reply.cause().getMessage()));
        }
    });
  }

  @Validate
  @Override
  public void deleteAdminKillQuery(String pid, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    PostgresClient.getInstance(vertxContext.owner()).select(
      "SELECT pg_terminate_backend('"+pid+"');", reply -> {
        if(reply.succeeded()){
          if (!Boolean.TRUE.equals(reply.result().iterator().next().getBoolean(0))) {
            asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(DeleteAdminKillQueryResponse.respond404WithTextPlain(pid)));
          }
          else{
            asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(DeleteAdminKillQueryResponse.respond204()));
          }
        }
        else{
          log.error(reply.cause().getMessage(), reply.cause());
          asyncResultHandler.handle(io.vertx.core.Future.failedFuture(reply.cause().getMessage()));
        }
    });
  }

  @Validate
  @Override
  public void postAdminPostgresMaintenance(String table, AdminPostgresMaintenancePostCommand command, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    String querySuffix = getSchema(okapiHeaders) +"."+table+";";
    String query = null;

    if(AdminPostgresMaintenancePostCommand.ANALYZE == command){
      query = "analyze " + querySuffix;
    }
    else if(AdminPostgresMaintenancePostCommand.VACUUM == command){
      query = "vacuum " + querySuffix;
    }
    else if(AdminPostgresMaintenancePostCommand.VACUUMANALYZE == command){
      query = "vacuum analyze " + querySuffix;
    }
    else if(AdminPostgresMaintenancePostCommand.VACUUMVERBOSE == command){
      query = "vacuum verbose " + querySuffix;
    }
    try{
      PostgresClient.getInstance(vertxContext.owner()).select(query, reply -> {
        if (reply.succeeded()){
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PostAdminPostgresMaintenanceResponse.
            respond201WithApplicationJson(new OutStream(reply.result()))));
        } else {
          log.error(reply.cause().getMessage(), reply.cause());
          asyncResultHandler.handle(io.vertx.core.Future.failedFuture(reply.cause().getMessage()));
        }
      });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      asyncResultHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
    }
  }

  @Validate
  @Override
  public void putAdminPostgresDropIndexes(Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    String schema = getSchema(okapiHeaders);
    String query =
        "SELECT * FROM pg_catalog.pg_class c JOIN pg_catalog.pg_index i ON (c.oid = i.indexrelid ) "
        + "JOIN pg_class t ON (i.indrelid = t.oid ) JOIN pg_namespace n ON (c.relnamespace = n.oid ) "
        + "WHERE c.relkind = 'i' AND n.nspname = '"+schema+"';";
    try{
      PostgresClient.getInstance(vertxContext.owner()).select(query, reply -> {
        if(reply.succeeded()){
          int indexes2delete[] = new int[]{ 0 };
          RowSet<Row> rs = reply.result();
          StringBuilder concatIndexNames = new StringBuilder();
          RowIterator<Row> iterator = rs.iterator();
          while (iterator.hasNext()) {
            Row row = iterator.next();
            String indexName = row.getString(0);
            if(!indexName.endsWith("_pkey")){
              indexes2delete[0]++;
              if(concatIndexNames.length() > 0){
                concatIndexNames.append(", ");
              }
              concatIndexNames.append(schema).append(".").append(indexName);
            }
          }

          String dropIndexQuery = "DROP INDEX " + concatIndexNames.toString() + ";";
          if(indexes2delete[0] > 0){
            PostgresClient.getInstance(vertxContext.owner()).select(dropIndexQuery, reply2 -> {
              if(reply2.succeeded()){
                log.info("Deleted " + indexes2delete[0] + " indexes");
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutAdminPostgresDropIndexesResponse.respond204()));
              }
              else{
                log.error(reply.cause().getMessage(), reply.cause());
                asyncResultHandler.handle(io.vertx.core.Future.failedFuture(reply2.cause().getMessage()));
              }
            });
          }
          else{
            log.info("No indexes to delete");
            asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
              PutAdminPostgresDropIndexesResponse.respond400WithTextPlain(
                  "No indexes to delete, for tenant " + TenantTool.tenantId(okapiHeaders))));
          }
        }
        else{
          log.error(reply.cause().getMessage(), reply.cause());
          asyncResultHandler.handle(io.vertx.core.Future.failedFuture(reply.cause().getMessage()));
        }
      });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      asyncResultHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
    }
  }

  @Validate
  @Override
  public void putAdminPostgresCreateIndexes(Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    try {
      String tenantId = TenantTool.tenantId(okapiHeaders);
      SchemaMaker sMaker = new SchemaMaker(tenantId, PostgresClient.getModuleName(),
        TenantOperation.CREATE, null, null);

      InputStream tableInput = AdminAPI.class.getClassLoader().getResourceAsStream(
        TenantAPI.TABLE_JSON);

      String tableInputStr = null;
      if(tableInput != null){
        tableInputStr = IOUtils.toString(tableInput, "UTF8");
        Schema schema = ObjectMapperTool.getMapper().readValue(tableInputStr, Schema.class);
        sMaker.setSchema(schema);
      }

      String sqlFile = sMaker.generateIndexesOnly();

      PostgresClient.getInstance(vertxContext.owner()).runSqlFile(sqlFile)
      .onFailure(e -> {
        log.error("{}", e.getMessage(), e);
        asyncResultHandler.handle(Future.succeededFuture(
            PutAdminPostgresCreateIndexesResponse.respond400WithTextPlain(e.getMessage())));
      })
      .onSuccess(x ->
        asyncResultHandler.handle(Future.succeededFuture(
            PutAdminPostgresCreateIndexesResponse.respond204()))
      );
    } catch (Exception e) {
      log.error("{}", e.getMessage(), e);
      asyncResultHandler.handle(Future.succeededFuture(PutAdminPostgresCreateIndexesResponse.
        respond500WithTextPlain(e.getMessage())));
    }

  }

  private String getSchema(Map<String,String> okapiHeaders) {
    return PostgresClient.convertToPsqlStandard(TenantTool.tenantId(okapiHeaders));
  }

}
