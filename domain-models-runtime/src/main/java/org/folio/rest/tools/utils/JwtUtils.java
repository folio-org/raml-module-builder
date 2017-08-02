package org.folio.rest.tools.utils;

import java.io.UnsupportedEncodingException;
import java.util.Base64;

/**
 * @author shale
 *
 */
public class JwtUtils {

  public static String getJson(String strEncoded) throws UnsupportedEncodingException{
    byte[] decodedBytes = Base64.getDecoder().decode(strEncoded);
    return new String(decodedBytes, "UTF-8");
  }

}
