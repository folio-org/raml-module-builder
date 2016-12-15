package org.folio.rest.tools.utils;

/**
 * @author shale
 *
 */
public class TenantTool {

  public static String calculateTenantId(String tenantId){
    if(tenantId == null){
      return "folio_shared";
    }
    return tenantId;
  }

}
