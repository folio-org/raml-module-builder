package org.folio.rest.tools.monitor;

import io.vertx.core.json.JsonObject;

import java.util.DoubleSummaryStatistics;
import java.util.Hashtable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * @author shale
 *
 */
public class StatsTracker {

  private static Map<String, Cache<Long, Long>> registeredStatRequesters =
      new Hashtable<String, Cache<Long, Long>>();

  private StatsTracker(){}

  /** add a metric (time) to a specific resource - for example
   * type = PostgresClient.get
   * time = single operation time in nanoseconds
   * Note: assumes single thread access by vertx so not synced */
  public static void addStatElement(String type, long time){
    Cache<Long, Long> statRepo = registeredStatRequesters.get(type);
    if(statRepo == null){
      registeredStatRequesters.put(type, CacheBuilder.newBuilder()
          .maximumSize(1000)
          .expireAfterWrite(2, TimeUnit.MINUTES)
          .build());
      statRepo = registeredStatRequesters.get(type);
    }
    statRepo.put(System.nanoTime(), (time/1000000));
  }

  public static String calculateStatsFor(String type){
    Cache<Long, Long> statRepo =registeredStatRequesters.get(type);
    if(statRepo != null){
      DoubleSummaryStatistics stats =
          statRepo.asMap().values().stream().collect(Collectors.summarizingDouble(l -> l));
      return stats.toString();
    }
    return null;
  }

  public static String spillAllStats(){
    Set<Entry<String, Cache<Long, Long>>> all = registeredStatRequesters.entrySet();
    StringBuilder sb = new StringBuilder();
    JsonObject job = new JsonObject();
    all.forEach( stat -> {
      job.put(stat.getKey(),
        stat.getValue().asMap().values().stream().collect(Collectors.summarizingDouble(l -> l)).toString());

    });
    return job.encodePrettily();
  }

}
