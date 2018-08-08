package org.folio.rest;

import org.folio.rest.client.AdminClient;


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
/*    MimeMultipart mmp = new MimeMultipart();
    BodyPart bp = new MimeBodyPart(new FileInputStream("C:\\Git\\mod-files\\ramls\\mod-files\\files.raml"));
    bp.setDisposition("form-data");
    bp.setFileName("abc.raml");
    BodyPart bp2 = new MimeBodyPart(new FileInputStream("C:\\Git\\mod-files\\ramls\\mod-files\\files.raml"));
    bp2.setDisposition("form-data");
    bp2.setFileName("abcd.raml");
    mmp.addBodyPart(bp);
    mmp.addBodyPart(bp2);*/
    AdminClient aClient = new AdminClient("localhost", 8888, null, null, false);
    /*
    AdminUploadmultipartPostMultipartFormData data =
        new AdminUploadmultipartPostMultipartFormDataImpl();

    List<File> a = new ArrayList<>();
    org.folio.rest.jaxrs.model.File t = new org.folio.rest.jaxrs.model.FileImpl();
    t.setFile(new java.io.File("create_config.sql"));
    a.add(t);
    data.setFiles(a);
    aClient.postAdminUploadmultipart(AdminUploadmultipartPostPersistMethod.SAVE, null, "abc",
      data, reply -> {
      reply.statusCode();
    });

    aClient.postImportSQL(
      Test.class.getClassLoader().getResourceAsStream("create_config.sql"), reply -> {
      reply.statusCode();
    });
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
    */

/*    aClient.putAdminLoglevel(AdminLoglevelPutLevel.FINE, "org", reply -> {
      reply.bodyHandler( body -> {
        System.out.println(body.toString("UTF8"));
      });
    });*/
    aClient.postAdminImportSQL(
      Test.class.getClassLoader().getResourceAsStream("job.json"), reply -> {
      reply.statusCode();
    });
    aClient.getAdminPostgresActiveSessions("postgres",  reply -> {
      reply.bodyHandler( body -> {
        System.out.println(body.toString("UTF8"));
      });
    });
    aClient.getAdminPostgresLoad("postgres",  reply -> {
      reply.bodyHandler( body -> {
        System.out.println(body.toString("UTF8"));
      });
    });
    aClient.getAdminPostgresTableAccessStats( reply -> {
      reply.bodyHandler( body -> {
        System.out.println(body.toString("UTF8"));
      });
    });
    aClient.getAdminPostgresTableSize("postgres", reply -> {
      reply.bodyHandler( body -> {
        System.out.println(body.toString("UTF8"));
      });
    });
  }
}
