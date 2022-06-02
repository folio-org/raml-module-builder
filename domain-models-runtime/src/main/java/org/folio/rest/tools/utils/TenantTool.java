package org.folio.rest.tools.utils;

import java.util.Map;

import org.folio.rest.RestVerticle;

/**
 * Utility functions for tenant handling.
 */
public final class TenantTool {
  private TenantTool() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  /**
   * @param tenantId tenant to calculate the tenantId for
   * @return the tenantId, this is "folio_shared" if tenantId is null
   */
  public static String calculateTenantId(String tenantId) {
    if(tenantId == null){
      return "folio_shared";
    }
    return tenantId;
  }

  /**
   * @param headers HTTP headers to use, may be empty, but not null
   * @return the tenantId for the headers, returns the default "folio_shared" if undefined
   */
  public static String tenantId(Map<String, String> headers) {
    String tenant = null;
    for (var entry : headers.entrySet()) {
      if (entry.getKey().equalsIgnoreCase(RestVerticle.OKAPI_HEADER_TENANT)) {
        tenant = entry.getValue();
        break;
      }
    }
    return calculateTenantId(tenant);
  }
}
