package org.folio.rest;

import java.io.FileInputStream;

import javax.mail.BodyPart;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;

import org.folio.rest.client.AdminClient;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.resource.AdminResource.PersistMethod;

/**
 * @author shale
 *
 */
public class Test {

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub
    MimeMultipart mmp = new MimeMultipart();
    BodyPart bp = new MimeBodyPart(new FileInputStream("C:\\Git\\mod-files\\ramls\\mod-files\\files.raml"));
    bp.setDisposition("form-data");
    bp.setFileName("abc.raml");
    BodyPart bp2 = new MimeBodyPart(new FileInputStream("C:\\Git\\mod-files\\ramls\\mod-files\\files.raml"));
    bp2.setDisposition("form-data");
    bp2.setFileName("abcd.raml");
    mmp.addBodyPart(bp);
    mmp.addBodyPart(bp2);
    AdminClient aClient = new AdminClient("localhost", 8888, null, null, false);
    aClient.postUploadmultipart(PersistMethod.SAVE, null, "abc",
      mmp, reply -> {
      reply.statusCode();
    });

/*    aClient.postImportSQL(
      Test.class.getClassLoader().getResourceAsStream("create_config.sql"), reply -> {
      reply.statusCode();
    });*/
    aClient.getJstack( trace -> {
      trace.bodyHandler( content -> {
        System.out.println(content);
      });
    });

    TenantClient tc = new TenantClient("localhost", 8888, "harvard", "harvard");
    tc.post(null, response -> {
      response.bodyHandler( body -> {
        System.out.println(body.toString());
        tc.delete( reply -> {
          reply.bodyHandler( body2 -> {
            System.out.println(body2.toString());
          });
        });
      });
    });
    aClient.getPostgresActiveSessions("postgres",  reply -> {
      reply.bodyHandler( body -> {
        System.out.println(body.toString("UTF8"));
      });
    });
    aClient.getPostgresLoad("postgres",  reply -> {
      reply.bodyHandler( body -> {
        System.out.println(body.toString("UTF8"));
      });
    });
    aClient.getPostgresTableAccessStats( reply -> {
      reply.bodyHandler( body -> {
        System.out.println(body.toString("UTF8"));
      });
    });
    aClient.getPostgresTableSize("postgres", reply -> {
      reply.bodyHandler( body -> {
        System.out.println(body.toString("UTF8"));
      });
    });
  }
}
