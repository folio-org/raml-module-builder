package org.folio.rest.impl;

import io.vertx.core.Context;
import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.tools.utils.TenantLoading;

import java.util.Map;

public class TenantRefAPI extends TenantAPI {

  @Override
  Future<Integer> loadData(TenantAttributes attributes, String tenantId,
                           Map<String, String> headers, Context vertxContext) {
    return super.loadData(attributes, tenantId, headers, vertxContext)
        .compose(recordsLoaded -> new TenantLoading()
            .withKey("loadReference").withLead("ref-data")
            .withIdContent().add("data", "bees/bees")
            .perform(attributes, headers, vertxContext, recordsLoaded));
  }

}
