package org.folio.rest.tools.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;

import javax.mail.MessagingException;
import javax.mail.internet.InternetHeaders;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.common.primitives.Bytes;

import io.vertx.core.buffer.Buffer;

/**
 * @author shale
 *
 */
public class FileUploadsUtil {

  private static final int FILE_SIZE_LIMIT = 1024*1024*100; // 100mb limit

  //to separate the multiparts into parts based on a boundary in the body - need to find the boundary separator
  //need encoding for this - if "charset" is passed in the header request then it should be used instead
  private static final String DEFAULT_ENCODING = "ISO-8859-1";

  private static final Logger log = LogManager.getLogger(FileUploadsUtil.class);

  public static MimeMultipart MultiPartFormData(Buffer buffer) throws IOException {

    return MultiPartFormData(buffer, FILE_SIZE_LIMIT, DEFAULT_ENCODING);

  }

  public static MimeMultipart MultiPartFormData(Buffer buffer, String encoding) throws IOException {

    return MultiPartFormData(buffer, FILE_SIZE_LIMIT, encoding);

  }

  private static MimeMultipart split(byte[] pattern, byte[] input, int sizeLimit) {

    MimeMultipart mmp = new MimeMultipart();
    int start = 0;
    int pos   = Bytes.indexOf(input, pattern);
    int size  = input.length;
    int entryCount = 0;
    ByteBuffer buffer = ByteBuffer.wrap(input);

    while(pos != -1 && start < size){

      int end = pos + pattern.length;
      if(entryCount != 0){
        //dont add the boundary itself - which is what you have in the first iteration
        buffer.position(start);

        //not a copy but points to the buffer
        //used for the indexOf functionality to keep checking
        //further on in the buffer - current pos -> end of buffer
        byte[] tmpBuffer = buffer.slice().array();

        //set limit - now that limit is set re-slice to only get the needed
        //area -
        buffer.limit(end);

        try {
          MimeBodyPart mbp = new MimeBodyPart(new InternetHeaders(), buffer.slice().array());
          mmp.addBodyPart(mbp);
        } catch (MessagingException e) {
          log.error(e.getMessage(), e);
        }
        pos = Bytes.indexOf(tmpBuffer, pattern);
      }
      entryCount++;
      start = end;
    }
    return mmp;
  }

  public static MimeMultipart MultiPartFormData(Buffer buffer, int sizeLimit, String encoding) throws IOException {

    int contentLength = buffer.length();
    ArrayList<Byte> vBytes = new ArrayList<>();
    byte[] b = new byte[1];
    int paramCount = 0;

    // if the file they are trying to upload is too big, throw an exception
    if(contentLength > sizeLimit) {
      throw new IOException("File has exceeded size limit.");
    }

    byte[] bytes = buffer.getBytes();

    byte[] separator = "\n".getBytes(Charset.forName(encoding));

    int endBoundaryPos = Bytes.indexOf(bytes, separator);

    //only copy the boundary (the first line) - not copying all content
    byte[] boundary = Arrays.copyOfRange(bytes, 0, endBoundaryPos + separator.length);

    return split(boundary, bytes, sizeLimit);

  }
}
