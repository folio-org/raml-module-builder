package org.folio.rest.impl;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.UUID;

import org.folio.rest.jaxrs.model.Job;
import org.folio.rest.jaxrs.model.JobsConf;
import org.folio.rest.persist.PostgresClientITBase;
import org.folio.rest.tools.RTFConsts;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class JobAPIIT extends PostgresClientITBase {
  static {
    setTenant("public");
  }

  private final static String schemaJobConfs = schema + "." + RTFConsts.JOB_CONF_COLLECTION;
  private final static String schemaJobs     = schema + "." + RTFConsts.JOBS_COLLECTION;
  private final static String myInstId = UUID.randomUUID().toString();

  @BeforeClass
  public static void beforeClass(TestContext context) throws Exception {
    setUpClass(context);
    executeSuperuser(context,
        "CREATE TABLE " + schemaJobConfs + " (id UUID PRIMARY KEY, jsonb JSONB NOT NULL)",
        "CREATE TABLE " + schemaJobs     + " (id UUID PRIMARY KEY, jsonb JSONB NOT NULL)",
        "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA " + schema + " TO " + schema);
  }

  private String jobConfs(String module, String name, String type, boolean enabled, String instId) {
    return new JsonObject()
        .put("module", module)
        .put("name", name)
        .put("type", type)
        .put("enabled", enabled)
        .put("inst_id", instId)
        .encode();
  }

  private String insertJobConf(TestContext context, String module, String name, String type, boolean enabled) {
    String id = UUID.randomUUID().toString();
    execute(context, "INSERT INTO " + schemaJobConfs + " VALUES ('" + id + "', '" +
        jobConfs("MyMod", "MyJob", "MyType", true, myInstId) + "');\n");
    return id;
  }

  private String jobs(String jobConfId, String creator, String status, String instId) {
    return new JsonObject()
        .put("job_conf_id", jobConfId)
        .put("creator", creator)
        .put("status", status)
        .put("inst_id", instId)
        .encode();
  }

  private String insertJob(TestContext context, String jobConfId, String creator, String status) {
    String id = UUID.randomUUID().toString();
    execute(context, "INSERT INTO " + schemaJobs + " VALUES ('" + id + "', '" +
        jobs(jobConfId, creator, status, myInstId) + "');\n");
    return id;
  }

  @Test
  public void getJobsJobconfsByJobconfsId(TestContext context) {
    String jobConfId = insertJobConf(context, "MyMod", "MyJob", "MyType", true);
    JobAPI jobApi = new JobAPI();
    jobApi.getJobsJobconfsByJobconfsId(jobConfId, null, okapiHeaders, context.asyncAssertSuccess(get -> {
      assertThat(get.getStatus(), is(200));
      JobsConf jobsConf = (JobsConf) get.getEntity();
      assertThat(jobsConf.getName(), is("MyJob"));
    }), vertx.getOrCreateContext());
  }

  @Test
  public void putJobsJobconfsJobsByJobconfsIdAndJobId(TestContext context) {
    String jobConfId = insertJobConf(context, "MyMod", "MyJob", "MyType", true);
    String jobId = insertJob(context, jobConfId, "MyCreator", "MyStatus");
    Job job = new Job().withId(jobId).withJobConfId(jobConfId).withCreator("OtherCreator").withStatus("OtherStatus");
    JobAPI jobApi = new JobAPI();
    jobApi.putJobsJobconfsJobsByJobconfsIdAndJobId(jobId, jobConfId, null, job, okapiHeaders, context.asyncAssertSuccess(put -> {
      assertThat(put.getStatus(), is(204));
      jobApi.getJobsJobconfsJobsByJobconfsIdAndJobId(jobId, jobConfId, null, okapiHeaders, context.asyncAssertSuccess(get -> {
        assertThat(get.getStatus(), is(200));
        Job job2 = (Job) get.getEntity();
        assertThat(job2.getCreator(), is(job.getCreator()));
        assertThat(job2.getStatus(), is(job.getStatus()));
      }), vertx.getOrCreateContext());
    }), vertx.getOrCreateContext());
  }
}
