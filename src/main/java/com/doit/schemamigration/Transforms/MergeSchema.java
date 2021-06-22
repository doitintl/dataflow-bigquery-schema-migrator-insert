package com.doit.schemamigration.Transforms;

import static com.google.cloud.bigquery.BigQueryOptions.newBuilder;

import com.google.cloud.bigquery.*;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Optional;
import org.apache.beam.repackaged.core.org.antlr.v4.runtime.misc.OrderedHashSet;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergeSchema extends DoFn<KV<String, Schema>, KV<String, Schema>> {
  final String projectName;
  final String datasetName;
  static BigQuery bigQuery;
  final Logger logger = LoggerFactory.getLogger(MergeSchema.class);

  public MergeSchema(final String projectName, final String datasetName) {
    this.projectName = projectName;
    this.datasetName = datasetName;
    ensureBigQueryExists();
  }

  public void ensureBigQueryExists() {
    bigQuery =
        new BigQueryOptions.DefaultBigQueryFactory()
            .create(newBuilder().setProjectId(projectName).build());
  }

  @ProcessElement
  public void processElement(final ProcessContext c) {
    final KV<String, Schema> tableAndSchema = c.element();
    final TableId tableId =
        TableId.of(datasetName, Objects.requireNonNull(tableAndSchema).getKey());
    final Optional<Table> targetTable = getOrCreateTable(tableId, tableAndSchema.getValue());
    if (targetTable.isPresent()) {
      final Table table = targetTable.get();
      logger.debug("Target table is: {}", table);
      updateTargetTableSchema(tableAndSchema.getValue(), table);
    }
    c.output(tableAndSchema);
  }

  public Optional<Table> getOrCreateTable(final TableId tableId, final Schema Schema) {
    ensureBigQueryExists();
    logger.debug("Big query status: {}", bigQuery);
    // Try to get table id from big query
    try {
      return Optional.of(bigQuery.getTable(tableId));
    } catch (BigQueryException | NullPointerException e) {
      logger.error("Failed to get tableId: {}\nWith Exception {}", tableId, e.toString());
    }
    // Try to create table id
    try {
      logger.debug("Creating table: {}", tableId.getTable());
      bigQuery.create(TableInfo.of(tableId, StandardTableDefinition.of(Schema)));
      return Optional.of(bigQuery.getTable(tableId));
    } catch (BigQueryException | NullPointerException e) {
      logger.error("Failed to create table {}\nWith Exception {}", tableId, e.toString());
    }
    // If finding id fails and creating table fails, return empty
    return Optional.empty();
  }

  public void updateTargetTableSchema(
      final Schema incomingTableAndSchema, final Table targetTable) {
    final Schema targetTableSchema = targetTable.getDefinition().getSchema();
    final OrderedHashSet<Field> newFields = mergeSchemas(incomingTableAndSchema, targetTableSchema);

    if (targetTableSchema != null && newFields.size() > targetTableSchema.getFields().size()) {
      logger.debug("New schema is: {}", newFields.toString());
      final Table updatedTable =
          targetTable
              .toBuilder()
              .setTableId(targetTable.getTableId())
              .setDefinition(StandardTableDefinition.of(Schema.of(newFields)))
              .build();
      try {
        updatedTable.update();
      } catch (BigQueryException e) {
        logger.error("New schema not created, error: {}", e.toString());
      }
    }
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
