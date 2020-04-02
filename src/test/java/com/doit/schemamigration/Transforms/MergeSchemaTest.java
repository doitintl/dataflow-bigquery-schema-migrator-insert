package com.doit.schemamigration.Transforms;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.google.cloud.bigquery.*;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class MergeSchemaTest {
  private MergeSchema mergeSchema;

  @Before
  public void setup() {
    mergeSchema.bigQuery = mock(BigQuery.class);
    mergeSchema = new MergeSchema("test", "test");
  }

  @Test
  public void updateTargetTableSchemaTestWithTheSameKey() {
    final Schema incomingSchema = Schema.of(Field.of("a", StandardSQLTypeName.STRING));

    final Table table = mock(Table.class);
    final TableDefinition tableDefinition = mock(TableDefinition.class);
    when(table.getDefinition()).thenReturn(tableDefinition);
    when(tableDefinition.getSchema())
        .thenReturn(Schema.of(Field.of("a", StandardSQLTypeName.STRING)));

    mergeSchema.updateTargetTableSchema(incomingSchema, table);
    assertThat(table.getDefinition().getSchema(), is(equalTo(incomingSchema)));
  }

  @Test
  public void updateTargetTableSchemaTestWithTheDifferentKeys() {
    final Schema incomingSchema =
        Schema.of(
            Field.of("a", StandardSQLTypeName.STRING), Field.of("b", StandardSQLTypeName.STRING));

    final Table table = mock(Table.class);
    final TableDefinition tableDefinition = mock(TableDefinition.class);
    final Table.Builder builder = mock(Table.Builder.class);
    final Schema defaultSchema = Schema.of(Field.of("a", StandardSQLTypeName.STRING));
    when(table.getDefinition()).thenReturn(tableDefinition);
    when(tableDefinition.getSchema()).thenReturn(defaultSchema);
    when(table.getTableId()).thenReturn(TableId.of("test", "test"));
    when(table.toBuilder()).thenReturn(builder);
    when(builder.setTableId(any(TableId.class))).thenReturn(builder);
    when(builder.setDefinition(any(TableDefinition.class))).thenReturn(builder);
    when(builder.build()).thenReturn(table);
    when(table.update()).thenReturn(table);

    mergeSchema.updateTargetTableSchema(incomingSchema, table);
    final ArgumentCaptor<TableDefinition> tableDefinitionCaptor =
        ArgumentCaptor.forClass(TableDefinition.class);
    verify(builder).setDefinition(tableDefinitionCaptor.capture());
    assertThat(tableDefinitionCaptor.getValue().getSchema(), is(equalTo(incomingSchema)));
  }

  @Test
  public void updateTargetTableSchemaTestWithTheDifferentSchema() {
    final Schema incomingSchema =
        Schema.of(
            Field.of("a", StandardSQLTypeName.INT64), Field.of("b", StandardSQLTypeName.STRING));

    final Table table = mock(Table.class);
    final TableDefinition tableDefinition = mock(TableDefinition.class);
    final Table.Builder builder = mock(Table.Builder.class);
    final Schema defaultSchema = Schema.of(Field.of("a", StandardSQLTypeName.STRING));
    when(table.getDefinition()).thenReturn(tableDefinition);
    when(tableDefinition.getSchema()).thenReturn(defaultSchema);
    when(table.getTableId()).thenReturn(TableId.of("test", "test"));
    when(table.toBuilder()).thenReturn(builder);
    when(builder.setTableId(any(TableId.class))).thenReturn(builder);
    when(builder.setDefinition(any(TableDefinition.class))).thenReturn(builder);
    when(builder.build()).thenReturn(table);
    when(table.update()).thenReturn(table);

    mergeSchema.updateTargetTableSchema(incomingSchema, table);
    final ArgumentCaptor<TableDefinition> tableDefinitionCaptor =
        ArgumentCaptor.forClass(TableDefinition.class);
    verify(builder).setDefinition(tableDefinitionCaptor.capture());
    final Schema expectedResult =
        Schema.of(defaultSchema.getFields().get(0), incomingSchema.getFields().get(1));
    assertThat(tableDefinitionCaptor.getValue().getSchema(), is(equalTo(expectedResult)));
  }
}
