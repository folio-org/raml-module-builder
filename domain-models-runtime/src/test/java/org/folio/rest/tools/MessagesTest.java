package org.folio.rest.tools;

import static org.junit.Assert.*;
import org.folio.rest.tools.messages.*;
import org.junit.Test;

public class MessagesTest {
  @Test
  public void getMessage() {
    Messages m = Messages.getInstance();
    assertEquals("Content-type header must be a but it is \"b\"", m.getMessage("en", "10006", "a", "b"));
    assertEquals("Content-type header must be a but it is \"b\"", m.getMessage("__", "10006", "a", "b"));
    assertEquals("Content-type header must be a but it is \"b\"", m.getMessage(null, "10006", "a", "b"));
  }
}
