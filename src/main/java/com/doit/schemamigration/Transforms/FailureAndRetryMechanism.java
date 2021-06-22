package com.doit.schemamigration.Transforms;

import static com.doit.schemamigration.Parsers.TableRowToSchema.dateTimeFormatter;
import static java.util.Arrays.asList;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;

import com.google.api.client.googleapis.util.Utils;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.Objects;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertErrorCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.Minutes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailureAndRetryMechanism
    extends PTransform<@NonNull PCollection<BigQueryInsertError>, @NonNull PCollection<String>> {
  final Logger logger = LoggerFactory.getLogger(FailureAndRetryMechanism.class);
  static final TableSchema failOverSchema =
      new TableSchema()
          .setFields(
              asList(
                  new TableFieldSchema().setName("bad_data").setType("STRING"),
                  new TableFieldSchema().setName("table_dest").setType("STRING"),
                  new TableFieldSchema().setName("error").setType("STRING"),
                  new TableFieldSchema().setName("insert_time").setType("DATETIME")));
  static final TupleTag<BigQueryInsertError> retry = new TupleTag<>();
  static final TupleTag<BigQueryInsertError> dump = new TupleTag<>();
  static final TupleTag<BigQueryInsertError> all = new TupleTag<>();
  final String failOverTable;
  final String retryAttemptJsonField;
  final String processedTimeJsonField;
  final Duration windowSize;
  final Integer numberOfAllowedAttempts;
  final BigQueryIO.Write<BigQueryInsertError> write;

  public FailureAndRetryMechanism(
      final String failOverTable,
      final String retryAttemptJsonField,
      final String processedTimeJsonField,
      final Duration windowSize,
      final Integer numberOfAllowedAttempts,
      final BigQueryIO.Write<BigQueryInsertError> write) {
    this.retryAttemptJsonField = retryAttemptJsonField;
    this.numberOfAllowedAttempts = numberOfAllowedAttempts;
    this.processedTimeJsonField = processedTimeJsonField;
    this.windowSize = windowSize;
    this.failOverTable = failOverTable;
    this.write = write;
  }

  @Override
  @NonNull
  public PCollection<String> expand(PCollection<BigQueryInsertError> input) {
    final PCollectionTuple failures =
        input.apply(
            "Split out data not going to be retried",
            ParDo.of(new TagData()).withOutputTags(all, TupleTagList.of(retry).and(dump)));

    failures
        .get(dump)
        .setCoder(BigQueryInsertErrorCoder.of())
        .apply(
            "Write to fail over table BigQuery",
            write
                .to(failOverTable)
                .withFormatFunction(
                    (BigQueryInsertError error) -> {
                      final TableRow tableRow = new TableRow();
                      tableRow.set("bad_data", Objects.requireNonNull(error).getRow().toString());
                      tableRow.set("table_dest", error.getTable().getTableId());
                      tableRow.set("error", error.getError().toString());
                      tableRow.set("insert_time", dateTimeFormatter.print(Instant.now()));
                      return tableRow;
                    })
                .withCreateDisposition(CREATE_IF_NEEDED)
                .withSchema(failOverSchema)
                .withWriteDisposition(WRITE_APPEND));

    return failures
        .get(retry)
        .setCoder(BigQueryInsertErrorCoder.of())
        .apply(
            "Get Table Data",
            MapElements.into(TypeDescriptor.of(String.class))
                .via(
                    (BigQueryInsertError item) -> {
                      final TableRow tableRow = Objects.requireNonNull(item).getRow();
                      tableRow.setFactory(Utils.getDefaultJsonFactory());
                      return tableRow.toString();
                    }));
  }

  class TagData extends DoFn<BigQueryInsertError, BigQueryInsertError> {
    @ProcessElement
    public void processElement(@Element BigQueryInsertError element, MultiOutputReceiver out) {
      final int retryAttemptNumber =
          Integer.parseInt(element.getRow().getOrDefault(retryAttemptJsonField, 0).toString());

      if (retryAttemptNumber < numberOfAllowedAttempts) {
        final TableRow updatedTableRow = element.getRow().clone();

        final DateTime processedTime =
            dateTimeFormatter.parseDateTime(
                element
                    .getRow()
                    .getOrDefault(processedTimeJsonField, dateTimeFormatter.print(Instant.now()))
                    .toString());

        if (retryAttemptNumber == 0) {
          updatedTableRow.set(retryAttemptJsonField, 1);
        } else {
          final long updatedRetryAttemptNumber =
              Minutes.minutesBetween(processedTime, Instant.now()).getMinutes()
                  / windowSize.getStandardMinutes();

          updatedTableRow.set(retryAttemptJsonField, updatedRetryAttemptNumber);
        }

        final BigQueryInsertError updatedInsertError =
            new BigQueryInsertError(updatedTableRow, element.getError(), element.getTable());

        out.get(retry).output(updatedInsertError);
      } else {
        out.get(dump).output(element);
      }
      out.get(all).output(element);
    }
  }
}
