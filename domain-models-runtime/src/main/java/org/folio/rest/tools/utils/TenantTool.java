package org.folio.rest.tools.utils;

/**
 * @author shale
 *
 */
public final class TenantTool {
  private TenantTool() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  public static String calculateTenantId(String tenantId){
    if(tenantId == null){
      return "folio_shared";
    }
    return tenantId;
  }

}
