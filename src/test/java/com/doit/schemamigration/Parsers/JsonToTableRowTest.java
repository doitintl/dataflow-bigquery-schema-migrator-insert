package com.doit.schemamigration.Parsers;

import static com.doit.schemamigration.Parsers.JsonToTableRow.convertFromString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;

import com.google.api.services.bigquery.model.TableRow;
import org.junit.Test;

public class JsonToTableRowTest {
  @Test
  public void convertFromStringTest() {
    final String jsonString =
        "{\"dest_table\":\"dest_table\",\"time\":\"1585644689\", \"retry\":1}";
    final TableRow result = convertFromString(jsonString);
    assertThat(result.get("retry"), is(equalTo(1L)));
    assertThat(result.get("time"), is(equalTo("1585644689")));
    assertThat(result.get("dest_table"), is(equalTo("dest_table")));
  }

  @Test
  public void badJSONStringIsEmptyTableRow() {
    final String jsonString = "i am not a jsonstring";
    assertThat(convertFromString(jsonString), is(equalTo(new TableRow())));
  }
}
