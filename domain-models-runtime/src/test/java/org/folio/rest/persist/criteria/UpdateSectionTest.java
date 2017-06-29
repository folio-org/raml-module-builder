package org.folio.rest.persist.criteria;

import static org.junit.Assert.*;

import org.folio.rest.persist.Criteria.UpdateSection;
import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.Parameters;
import junitparams.JUnitParamsRunner;

@RunWith(JUnitParamsRunner.class)
public class UpdateSectionTest {
  public Object[] parametersForGetValueForString() {
    String q = "\"";  // one double quote character
    return new Object[]{
        new Object[]{ "",   q       +q },  // empty string is ""
        new Object[]{ "a",  q+"a"   +q },  // a is "a"
        new Object[]{ "\\", q+"\\\\"+q },  // backslash is "\\"
        new Object[]{ "\n", q+"\\n" +q }   // newline is "\n"
   };
  }

  @Test
  @Parameters
  public void getValueForString(String in, String json) {
    UpdateSection updateSection = new UpdateSection();
    updateSection.setValue(in);
    assertEquals(json, updateSection.getValue());
  }

  @Test
  public void getValueForNull() {
    UpdateSection updateSection = new UpdateSection();
    updateSection.setValue(null);
    assertNull(updateSection.getValue());
  }

  @Test
  public void getValueForDouble() {
    UpdateSection updateSection = new UpdateSection();
    updateSection.setValue(1.234);
    assertEquals("1.234", updateSection.getValue());
  }
}
