package org.folio.rest.tools.utils;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Holds and writes a binary byte [].
 *
 * write(output) throws a NullPointerException if no data has been set or has been set to null.
 */
public class BinaryOutStream implements javax.ws.rs.core.StreamingOutput {

  private byte[] data;

  @Override
  public void write(OutputStream output) throws IOException {
    output.write(this.data);
    output.flush();
  }

  public byte[] getData() {
    return data;
  }

  public void setData(byte[] data) {
    this.data = data;
  }

}
