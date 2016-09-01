package com.sling.rest.persist.listeners;

import java.util.HashMap;
import java.util.Map;

import org.jooq.ExecuteContext;
import org.jooq.ExecuteType;
import org.jooq.impl.DefaultExecuteListener;

/**
 * @author shale
 *
 */
// Extending DefaultExecuteListener, which provides empty implementations for
// all methods...
public class StatisticsListener extends DefaultExecuteListener {
  public static Map<ExecuteType, Integer> STATISTICS = new HashMap<ExecuteType, Integer>();

  // Count "start" events for every type of query executed by jOOQ
  @Override
  public void start(ExecuteContext ctx) {
    synchronized (STATISTICS) {
      Integer count = STATISTICS.get(ctx.type());
      if (count == null) {
        count = 0;
      }
      STATISTICS.put(ctx.type(), count + 1);
    }
  }
}