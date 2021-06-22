package com.doit.schemamigration.Parsers;

import com.google.api.services.bigquery.model.TableRow;
import com.owlike.genson.Genson;
import com.owlike.genson.JsonBindingException;
import com.owlike.genson.stream.JsonStreamException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JsonToTableRow {
  static final Logger logger = LoggerFactory.getLogger(JsonToTableRow.class);

  public static TableRow convertFromString(final String json) {
    final Genson genson = new Genson();
    logger.debug("Incoming jsonString: {}", json);
    try {
      final var convertedObject = genson.deserialize(json, HashMap.class);
      return generate(convertedObject);
    } catch (JsonBindingException | JsonStreamException | NullPointerException e) {
      logger.debug("Failed to parse message:\n {}\nException thrown: {}", json, e);
    }
    return new TableRow();
  }

  static <T extends Map<?, ?>> TableRow generate(final T map) {
    final var out = new TableRow();
    for (var entry : map.entrySet()) {
      if (entry != null) {
        out.put((String) entry.getKey(), entry.getValue());
      }
    }
    logger.debug("Outgoing tablerow: {}", out);
    return out;
  }
}
