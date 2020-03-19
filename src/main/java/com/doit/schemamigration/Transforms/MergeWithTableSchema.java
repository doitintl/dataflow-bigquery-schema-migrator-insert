package com.doit.schemamigration.Transforms;

import static com.google.cloud.bigquery.BigQueryOptions.newBuilder;

import com.google.cloud.bigquery.*;
import java.util.ArrayList;
import java.util.Optional;
import org.apache.beam.repackaged.core.org.antlr.v4.runtime.misc.OrderedHashSet;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergeWithTableSchema
    extends PTransform<PCollection<KV<String, Schema>>, PCollection<KV<String, Schema>>> {
  final String projectName;
  final String datasetName;
  public static BigQuery bigQuery;
  final Logger logger = LoggerFactory.getLogger(MergeWithTableSchema.class);

  public MergeWithTableSchema(final String projectName, final String datasetName) {
    this.projectName = projectName;
    this.datasetName = datasetName;
    this.bigQuery =
        new BigQueryOptions.DefaultBigQueryFactory()
            .create(newBuilder().setProjectId(projectName).build());
  }

  @Override
  public PCollection<KV<String, Schema>> expand(final PCollection<KV<String, Schema>> input) {
    return input.apply("Merge Each Schema with pre-existing table", ParDo.of(new MergeSchema()));
  }

  class MergeSchema extends DoFn<KV<String, Schema>, KV<String, Schema>> {
    final Logger logger = LoggerFactory.getLogger(MergeSchema.class);

    @ProcessElement
    public void processElement(final ProcessContext c) {
      final KV<String, Schema> tableAndSchema = c.element();
      final TableId tableId = TableId.of(datasetName, tableAndSchema.getKey());
      final Optional<Table> targetTable = getOrCreateTable(tableId, tableAndSchema.getValue());
      if (targetTable.isPresent()) {
        final Table table = targetTable.get();
        logger.info("Target table is: {}", table);
        updateTargetTableSchema(tableAndSchema.getValue(), table);
      }
      c.output(tableAndSchema);
    }

    public Optional<Table> getOrCreateTable(final TableId tableId, final Schema Schema) {
      try {
        return Optional.of(bigQuery.getTable(tableId));
      } catch (BigQueryException | NullPointerException e) {
        logger.info("Creating table: {}", tableId.getTable());
        bigQuery.create(TableInfo.of(tableId, StandardTableDefinition.of(Schema)));
      }
      return Optional.empty();
    }
  }

  public void updateTargetTableSchema(
      final Schema incomingTableAndSchema, final Table targetTable) {
    final Schema targetTableSchema = targetTable.getDefinition().getSchema();
    final OrderedHashSet<Field> fields = mergeSchemas(incomingTableAndSchema, targetTableSchema);

    if (targetTableSchema != null && fields.size() > targetTableSchema.getFields().size()) {
      logger.info("New schema is: {}", fields.toString());
      final Table updatedTable =
          targetTable
              .toBuilder()
              .setTableId(targetTable.getTableId())
              .setDefinition(StandardTableDefinition.of(Schema.of(fields)))
              .build();
      try {
        updatedTable.update();
      } catch (BigQueryException e) {
        logger.error("Failed to add column");
        throw new IllegalStateException(e);
      }
      return;
    }
    logger.warn("New schema not created");
  }

  public static OrderedHashSet<Field> mergeSchemas(
      Schema incomingTableAndSchema, Schema targetTableSchema) {
    final OrderedHashSet<Field> fields = new OrderedHashSet<>();
    fields.addAll(targetTableSchema != null ? targetTableSchema.getFields() : new ArrayList<>());
    incomingTableAndSchema
        .getFields()
        .forEach(
            field -> {
              final Optional<Field> doesFieldKeyExist =
                  fields.stream().filter(item -> item.getName().equals(field.getName())).findAny();
              if (!doesFieldKeyExist.isPresent()) {
                fields.add(field);
              }
            });
    return fields;
  }
}
