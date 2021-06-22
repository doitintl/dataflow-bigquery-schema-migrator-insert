package com.doit.schemamigration.Transforms;

import static com.doit.schemamigration.Parsers.TableRowToSchema.convertToSchema;

import com.google.cloud.bigquery.Schema;
import java.util.Objects;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class KeyByDestTable extends DoFn<BigQueryInsertError, KV<String, Schema>> {
  @ProcessElement
  public void processElement(final ProcessContext context) {
    // Use OutputReceiver.output to emit the output element.
    final BigQueryInsertError error = context.element();
    context.output(
        KV.of(
            Objects.requireNonNull(error).getTable().getTableId(),
            convertToSchema(error.getRow())));
  }
}
