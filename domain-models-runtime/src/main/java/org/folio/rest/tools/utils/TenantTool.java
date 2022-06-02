package org.folio.rest.tools.utils;

import java.util.Map;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
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
   * Return the tenantId if available in the headers, or the default "folio_shared" if undefined.
   *
   * @param headers HTTP headers to use, may be empty, but not null; the keys of the map must be case insensitive,
   *    for example {@link CaseInsensitiveMap}
   */
  public static String tenantId(Map<String, String> headers) {
    return calculateTenantId(headers.get(RestVerticle.OKAPI_HEADER_TENANT));
  }
}
