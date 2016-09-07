package org.folio.rest.tools.utils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import javax.ws.rs.WebApplicationException;

public class OutStream implements javax.ws.rs.core.StreamingOutput {

  private Object data;

  @Override
  public void write(OutputStream output) throws IOException, WebApplicationException {

    Writer writer = new BufferedWriter(new OutputStreamWriter(output));
    writer.write(this.data.toString());
    writer.flush();
  }

  public Object getData() {
    return data;
  }

  public void setData(Object data) {
    this.data = data;
  }

}
