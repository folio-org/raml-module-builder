package org.folio.rest.tools.monitor;

import io.vertx.core.json.JsonObject;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.codahale.metrics.Snapshot;

/**
 * @author shale
 *
 */
public class StatsTracker {

  private static Set<String> registeredStatRequesters =
      new HashSet<String>();

  private static final MetricRegistry METRICS = new MetricRegistry();

  private StatsTracker(){}

  /** add a metric (time) to a specific resource - for example
   * type = PostgresClient.get
   * time = single operation time in nanoseconds
   */
  public static void addStatElement(String type, long time){
    // unsynchronized but fast check
    if (!registeredStatRequesters.contains(type)) {
      // prevent race condition - registering the same type twice will throw an exception
      synchronized(registeredStatRequesters) {
        // synchronized check
        if (!registeredStatRequesters.contains(type)) {
          METRICS.register(type, new Histogram(new SlidingTimeWindowReservoir(60,
            TimeUnit.SECONDS)));
          registeredStatRequesters.add(type);
        }
      }
    }
    METRICS.histogram(type).update(time/1000000);
  }

  public static JsonObject calculateStatsFor(String type){
    JsonObject j = new JsonObject();
    Snapshot snap = METRICS.histogram(type).getSnapshot();
    if(snap != null){
      j.put("entryCount", snap.size());
      j.put("min", snap.getMin());
      j.put("max", snap.getMax());
      j.put("mean", snap.getMean());
      j.put("median", snap.getMedian());
      j.put("75th", snap.get75thPercentile());
      j.put("95th", snap.get95thPercentile());
      j.put("99th", snap.get99thPercentile());
      j.put("stdDev", snap.getStdDev());
    }
    return j;
  }

  public static JsonObject spillAllStats(){
    JsonObject j = new JsonObject();
    registeredStatRequesters.forEach( stat -> {
      j.put(stat, calculateStatsFor(stat));
    });
    return j;
  }

}
