package org.folio.rest.tools.utils;

import io.vertx.core.VertxOptions;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxInfluxDbOptions;

/**
 * Metrics Utilities.
 */
public class MetricsUtil {

  private MetricsUtil() {
  }

  /**
   * Config metrics options - specifically use InfluxDb micrometer options.
   *
   * @param vertxOptions   - {@link VertxOptions}
   * @param influxUrl      - default to http://localhost:8086
   * @param influxDbName   - default to okapi
   * @param influxUserName - default to null
   * @param influxPassword - default to null
   */
  public static void config(VertxOptions vertxOptions, String influxUrl,
      String influxDbName, String influxUserName, String influxPassword) {
    VertxInfluxDbOptions influxDbOptions = new VertxInfluxDbOptions()
        .setEnabled(true)
        .setUri(influxUrl == null ? "http://localhost:8086" : influxUrl)
        .setDb(influxDbName == null ? "rmb" : influxDbName);
    if (influxUserName != null) {
      influxDbOptions.setUserName(influxUserName);
    }
    if (influxPassword != null) {
      influxDbOptions.setPassword(influxPassword);
    }
    vertxOptions.setMetricsOptions(new MicrometerMetricsOptions()
        .setEnabled(true)
        .setInfluxDbOptions(influxDbOptions));
  }

}
