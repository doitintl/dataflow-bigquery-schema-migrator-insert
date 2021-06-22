package com.doit.schemamigration.Parsers;

import static com.doit.schemamigration.Parsers.TableRowToSchema.convertToSchema;
import static com.google.cloud.bigquery.Field.Mode.REPEATED;
import static com.google.cloud.bigquery.LegacySQLTypeName.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.Schema;
import java.util.ArrayList;
import java.util.HashMap;
import org.junit.BeforeClass;
import org.junit.Test;

public class TableRowToSchemaTest {
  static final TableRow simple = new TableRow();

  @BeforeClass
  public static void init() {
    simple.put("int", 1);
    simple.put("string", "a");
    simple.put("int64", 3L);
    simple.put("date", "2020-01-01 00:00:00");
    simple.put("double", 3.0);
  }

  @Test
  public void toSchemaTestComplexRecord() {
    final TableRow grandParent = new TableRow();
    final HashMap<String, Object> parent = new HashMap<>();
    final HashMap<String, Object> child = new HashMap<>();
    final ArrayList<Object> childList =
        new ArrayList<>() {
          {
            add(child);
          }
        };
    final ArrayList<Object> parentList =
        new ArrayList<>() {
          {
            add(1);
            add(5);
            add(childList);
          }
        };
    child.putAll(simple);
    parent.put("child", childList);
    parent.put("I LIkE LISTS", parentList);
    grandParent.put("parent1", parent);
    grandParent.put("parent2", parent);

    final Schema schema = convertToSchema(grandParent);
    assertThat(schema.getFields().get(0).getType(), is(equalTo(RECORD)));
    final var schemaParent = schema.getFields().get("parent1");
    assertThat(schemaParent.getType(), is(equalTo(RECORD)));
    final var schemaChild = schemaParent.getSubFields().get(0);
    assertThat(schemaChild.getType(), is(equalTo(RECORD)));
    assertThat(schemaChild.getMode(), is(equalTo(REPEATED)));
  }

  @Test
  public void toSchemaTestSimple() {
    final Schema schema = convertToSchema(simple);
    assertThat(schema.getFields().get(0).getType(), is(equalTo(INTEGER)));
    assertThat(schema.getFields().get("string").getType(), is(equalTo(STRING)));
    assertThat(schema.getFields().get(2).getType(), is(equalTo(INTEGER)));
    assertThat(schema.getFields().get(3).getType(), is(equalTo(DATETIME)));
    assertThat(schema.getFields().get(4).getType(), is(equalTo(FLOAT)));
  }

  @Test
  public void toSchemaFailBack() {
    final TableRow tableRow = new TableRow();
    tableRow.put("int", Object.class);
    final Schema schema = convertToSchema(tableRow);
    assertThat(schema.getFields().get(0).getType(), is(equalTo(STRING)));
  }
}
