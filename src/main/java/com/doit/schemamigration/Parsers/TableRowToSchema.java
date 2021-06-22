package com.doit.schemamigration.Parsers;

import static com.google.cloud.bigquery.Field.Mode.REPEATED;
import static com.google.cloud.bigquery.LegacySQLTypeName.RECORD;
import static com.google.cloud.bigquery.StandardSQLTypeName.*;
import static java.util.stream.Collectors.toList;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.*;
import java.util.*;
import java.util.Map.Entry;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TableRowToSchema {
  static final Logger logger = LoggerFactory.getLogger(TableRowToSchema.class);

  public static Schema convertToSchema(final TableRow tableRow) {
    final List<Field> fields =
        tableRow.entrySet().stream().map(item -> createSchema(item)).collect(toList());
    return Schema.of(fields);
  }

  static Field createSchema(final Entry<String, Object> item) {
    return getField(item.getKey(), item.getValue());
  }

  private static Field getField(final String key, final Object value) {
    final var type = findType(value);
    if (type == STRUCT) {
      final var internal = (HashMap<String, Object>) value;
      return Field.of(key, RECORD, getSubFields(internal));
    }
    if (type == ARRAY) {
      final var internal = (ArrayList<Object>) value;
      var subFields = getSubFields(internal);
      return Field.of(key, RECORD, subFields).toBuilder().setMode(REPEATED).build();
    }
    return Field.of(key, type);
  }

  static FieldList getSubFields(final HashMap<String, Object> subObject) {
    final var subFields =
        subObject.entrySet().stream().map(entry -> createSchema(entry)).collect(toList());
    return FieldList.of(subFields);
  }

  static FieldList getSubFields(final ArrayList<Object> subObject) {
    final var subFields = new ArrayList<Field>();
    for (int i = 0; i < subObject.size(); i++) {
      final var item = subObject.get(i);
      subFields.add(getField(Integer.toString(i), item));
    }
    return FieldList.of(subFields);
  }

  static StandardSQLTypeName findType(final Object object) {
    if (object instanceof HashMap) {
      return STRUCT;
    }
    if (object instanceof Collection) {
      return ARRAY;
    }
    return findSimpleType(object);
  }

  static StandardSQLTypeName findSimpleType(final Object object) {
    // Numeric Types
    if (object instanceof Integer || object instanceof Long) {
      return INT64;
    }
    if (object instanceof Byte || object instanceof Byte[]) {
      return BYTES;
    }
    if (object instanceof Double || object instanceof Float) {
      return FLOAT64;
    }
    // Date/time check
    try {
      dateTimeFormatter.parseDateTime(object.toString());
      return DATETIME;
    } catch (IllegalArgumentException ignored) {
    }
    return STRING;
  }

  /**
   * Formats BigQuery seconds-since-epoch into String matching JSON export. Thread-safe and
   * immutable.
   */
  public static final DateTimeFormatter dateTimeFormatter =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC();
}
