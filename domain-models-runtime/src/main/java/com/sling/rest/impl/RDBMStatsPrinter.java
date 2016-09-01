package com.sling.rest.impl;

import org.jooq.ExecuteType;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import com.sling.rest.resource.interfaces.PeriodicAPI;

/**
 * @author shale
 *
 */
public class RDBMStatsPrinter implements PeriodicAPI {

  @Override
  public long runEvery() {
    // TODO Auto-generated method stub
    return 30000;
  }

  @Override
  public void run(Vertx vertx, Context context) {
    // TODO Auto-generated method stub
    //LogUtil.formatLogMessage(RDBMStatsPrinter.class.getName(), "run", "DB STATISTICS");
    //LogUtil.formatLogMessage(RDBMStatsPrinter.class.getName(), "run", "-------------");
    for (ExecuteType type : ExecuteType.values()) {
    //  LogUtil.formatLogMessage(RDBMStatsPrinter.class.getName(), "run", type.name() + "\t" +
    //      StatisticsListener.STATISTICS.get(type) + " executions");
    }
  }

}
