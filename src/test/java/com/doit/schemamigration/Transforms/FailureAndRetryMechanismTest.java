package com.doit.schemamigration.Transforms;

import static java.util.Arrays.asList;

import com.google.api.client.googleapis.util.Utils;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse.InsertErrors;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

// Fake Write to Big Query
// https://github.com/apache/beam/blob/v2.19.0/sdks/java/io/google-cloud-platform/src/test/java/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIOWriteTest.java
public class FailureAndRetryMechanismTest {
  private PipelineOptions options;
  private final TemporaryFolder testFolder = new TemporaryFolder();
  private TestPipeline p;

  @Rule
  public final transient TestRule folderThenPipeline =
      new TestRule() {
        @Override
        public Statement apply(final Statement base, final Description description) {
          // We need to set up the temporary folder, and then set up the TestPipeline based on the
          // chosen folder. Unfortunately, since rule evaluation order is unspecified and unrelated
          // to field order, and is separate from construction, that requires manually creating this
          // TestRule.
          Statement withPipeline =
              new Statement() {
                @Override
                public void evaluate() throws Throwable {
                  options = TestPipeline.testingPipelineOptions();
                  options.as(BigQueryOptions.class).setProject("project-id");
                  options
                      .as(BigQueryOptions.class)
                      .setTempLocation(testFolder.getRoot().getAbsolutePath());
                  p = TestPipeline.fromOptions(options);
                  p.apply(base, description).evaluate();
                }
              };
          return testFolder.apply(withPipeline, description);
        }
      };

  final FakeDatasetService fakeDatasetService = new FakeDatasetService();
  final FakeJobService fakeJobService = new FakeJobService();
  final FakeBigQueryServices fakeBqServices =
      new FakeBigQueryServices()
          .withDatasetService(fakeDatasetService)
          .withJobService(fakeJobService);
  final BigQueryIO.Write<BigQueryInsertError> write =
      BigQueryIO.<BigQueryInsertError>write().withTestServices(fakeBqServices).withoutValidation();

  @Before
  public void setup() throws IOException, InterruptedException {
    final CoderRegistry coderRegistry = p.getCoderRegistry();
    coderRegistry.registerCoderForType(
        TypeDescriptor.of(BigQueryInsertError.class), BigQueryInsertErrorCoder.of());

    FakeDatasetService.setUp();
    fakeDatasetService.createDataset("a12345", "b", "", "", 1L);
  }

  @Test
  public void testRetryMechanism() {
    final TableReference tableReference = BigQueryHelpers.parseTableSpec("a12345:b.c");
    final InsertErrors insertError = new InsertErrors();
    final TableRow retriedData = new TableRow();
    retriedData.set("a", "a");
    retriedData.set("retry", 0);
    final TableRow failed = new TableRow();
    failed.set("retry", 5);

    final BigQueryInsertError bigQueryInsertErrorRetry =
        new BigQueryInsertError(retriedData, insertError, tableReference);
    final BigQueryInsertError bigQueryInsertErrorFailed =
        new BigQueryInsertError(failed, insertError, tableReference);

    final PCollection<BigQueryInsertError> errorCollection =
        p.apply(
            "Create test data for failure",
            Create.of(asList(bigQueryInsertErrorRetry, bigQueryInsertErrorFailed)));

    final FailureAndRetryMechanism retryMechanism =
        new FailureAndRetryMechanism(
            "a12345:b.c", "retry", "time", Duration.standardMinutes(1), 3, write);

    final PCollection<String> result = retryMechanism.expand(errorCollection);

    final TableRow expectedResult = retriedData.clone();
    expectedResult.set("retry", 1);
    expectedResult.setFactory(Utils.getDefaultJsonFactory());

    PAssert.that(result).containsInAnyOrder(expectedResult.toString());

    p.run();
  }
}
