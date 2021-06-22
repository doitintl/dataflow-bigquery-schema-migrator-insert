package com.doit.schemamigration.Transforms;

import static com.doit.schemamigration.Transforms.MergeSchema.mergeSchemas;

import com.google.cloud.bigquery.Schema;
import java.util.Objects;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class CombineBySchema implements SerializableFunction<Iterable<Schema>, Schema> {
  @Override
  public Schema apply(Iterable<Schema> input) {
    return StreamSupport.stream(Objects.requireNonNull(input).spliterator(), false)
        .reduce(Schema.of(), (Schema a, Schema b) -> Schema.of(mergeSchemas(b, a)));
  }
}
