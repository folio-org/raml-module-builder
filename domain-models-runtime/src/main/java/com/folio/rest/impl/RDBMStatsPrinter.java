package com.folio.rest.impl;

import org.jooq.ExecuteType;

import com.folio.rest.resource.interfaces.PeriodicAPI;

import io.vertx.core.Context;
import io.vertx.core.Vertx;

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
