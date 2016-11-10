package org.folio.rest.tools;

import static org.junit.Assert.assertEquals;

import org.folio.rest.tools.messages.*;
import org.junit.Test;

public class MessagesTest {
  private static String __(String language, MessageEnum consts, Object... messageArguments) {
    return Messages.getInstance().getMessage(language, consts, messageArguments);
  }

  @Test
  public void getMessage() {
    MessageConsts c = MessageConsts.ContentTypeError;
    assertEquals("Content-type header must be a but it is \"b\"",      __("en", c, "a", "b"));
    assertEquals("Content-type header must be a but it is \"b\"",      __("__", c, "a", "b"));
    assertEquals("Content-type header must be a but it is \"b\"",      __(null, c, "a", "b"));
    assertEquals("Content-type Header muss a sein, aber er ist \"b\"", __("de", c, "a", "b"));
  }

  @Test
  public void umlaut() {
    assertEquals("Operation wird nicht unterstützt", __("de", MessageConsts.OperationNotSupported));
  }
}
