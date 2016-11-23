package org.folio.rest.tools.utils;

import java.io.IOException;
import java.io.OutputStream;

import javax.ws.rs.WebApplicationException;

/**
 * @author shale
 *
 */
public class BinaryOutStream implements javax.ws.rs.core.StreamingOutput {

  private byte[] data;

  @Override
  public void write(OutputStream output) throws IOException, WebApplicationException {

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