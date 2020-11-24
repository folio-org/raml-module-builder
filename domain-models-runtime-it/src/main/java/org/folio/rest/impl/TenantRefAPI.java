package org.folio.rest.impl;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.tools.utils.TenantLoading;

import java.util.Map;

public class TenantRefAPI extends TenantAPI {

  @Override
  Future<Void> loadData(TenantAttributes attributes, String tenantId, Map<String, String> headers,
                        Context verxContext) {
    return super.loadData(attributes, tenantId, headers, verxContext).compose(res -> {
      TenantLoading tl = new TenantLoading();
      tl.withKey("loadReference").withLead("ref-data")
          .withIdContent().add("data", "bees/bees");
      Promise<Integer> promise = Promise.promise();
      tl.perform(attributes, headers, verxContext.owner(), promise);
      return promise.future().mapEmpty();
    });
  }

}
