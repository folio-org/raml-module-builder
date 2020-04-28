package org.folio.rest.tools.utils;

import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.RowSet;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Holds and writes an Object whose {@link Object#toString()} is used when invoking {@link #write(OutputStream)}.
 *
 * {@link #write(OutputStream)} throws a {@link NullPointerException} if no object has been set or has been set to null.
 */
public class OutStream implements javax.ws.rs.core.StreamingOutput {

  private Object data;

  public OutStream() {
  }

  public OutStream(RowSet<Row> result) {
    RowIterator<Row> it = result.iterator();
    StringBuilder s = new StringBuilder();
    while (it.hasNext()) {
      s.append(it.next().toString());
    }
    data = s.toString();
  }
  @Override
  public void write(OutputStream output) throws IOException {
    // use UTF_8, do not use Charset.getDefaultCharset()
    output.write(data.toString().getBytes(StandardCharsets.UTF_8));
    output.flush();
  }

  /**
   * Return the object set by the last invocation of {@link #setData(Object)}.
   * @return
   */
  public Object getData() {
    return data;
  }

  /**
   * Set the object whose toString() will be used by {@link #write(OutputStream)}.
   * @param data  object to write.
   */
  public void setData(Object data) {
    this.data = data;
  }

}
