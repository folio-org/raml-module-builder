package org.folio.rest.tools.utils;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import javax.ws.rs.WebApplicationException;

import org.junit.Test;

public class BinaryOutStreamTest {
  @Test
  public void getWithoutSet() {
    assertNull(new BinaryOutStream().getData());
  }

  @Test
  public void setGet() {
    BinaryOutStream s = new BinaryOutStream();
    s.setData        (new byte [] { 'a', 'b' });
    assertArrayEquals(new byte [] { 'a', 'b' },  s.getData());
  }

  @Test
  public void setGet2() {
    BinaryOutStream s = new BinaryOutStream();
    s.setData        (new byte [] { 'a', 'b' });
    s.setData        (new byte [] { 'c', 'd' });
    assertArrayEquals(new byte [] { 'c', 'd' },  s.getData());
  }

  @Test
  public void setGetNull() {
    BinaryOutStream s = new BinaryOutStream();
    s.setData(new byte [] { 'a', 'b' });
    s.setData(null);
    assertNull(s.getData());
  }

  @Test(expected = NullPointerException.class)
  public void writeNull() throws WebApplicationException, IOException {
    new BinaryOutStream().write(new ByteArrayOutputStream());
  }

  private void testBytes(Charset charset) throws WebApplicationException, IOException {
    BinaryOutStream s = new BinaryOutStream();
    s.setData("ä".getBytes(charset));
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    s.write(result);
    assertArrayEquals("ä".getBytes(charset), result.toByteArray());
  }

  @Test
  public void utf8() throws WebApplicationException, IOException {
    testBytes(StandardCharsets.UTF_8);
  }

  @Test
  public void iso_8859_1() throws WebApplicationException, IOException {
    testBytes(StandardCharsets.ISO_8859_1);
  }

}
