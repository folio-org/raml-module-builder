package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.folio.rest.annotations.Stream;
import org.folio.rest.jaxrs.resource.Usersupload;

public class UsersuploadAPI implements Usersupload {

  // message length for a stream
  private static Map<String, Integer> streams = new HashMap<>();

  @Stream
  @Override
  public void postUsersupload(InputStream entity, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    final String id = okapiHeaders.get("streamed_id");
    final String complete = okapiHeaders.get("complete");
    final String aborting = okapiHeaders.get("streamed_abort");
    if (aborting != null) {
      System.out.println("ABORTING");
      asyncResultHandler.handle(Future.succeededFuture(
        PostUsersuploadResponse.respond400WithTextPlain("aborting")));
      return;
    }
    int length = streams.getOrDefault(id, 0);
    try {
      byte[] buf = new byte[100];
      int r;
      while ((r = entity.read(buf, 0, buf.length)) != -1) {
        length += r;
      }
      streams.put(id, length);
      if (complete != null) {  // end-of-stream?
        streams.remove(id);
      } else {
        length = 0; // response with 0 until we reach end-of-stream
      }
      asyncResultHandler.handle(Future.succeededFuture(
              PostUsersuploadResponse.respond201WithTextPlain(Integer.toString(length))));
    } catch (IOException ex) {
      asyncResultHandler.handle(Future.succeededFuture(
              PostUsersuploadResponse.respond400WithTextPlain(ex.getLocalizedMessage())));
    }
  }
}
