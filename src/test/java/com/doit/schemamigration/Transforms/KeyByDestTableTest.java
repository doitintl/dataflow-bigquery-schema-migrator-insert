package com.doit.schemamigration.Transforms;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.*;

import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

public class KeyByDestTableTest {
  @Mock
  private final DoFn<BigQueryInsertError, KV<String, Schema>>.ProcessContext processContext =
      mock(DoFn.ProcessContext.class);

  @Captor
  private final ArgumentCaptor<KV<String, Schema>> captor = ArgumentCaptor.forClass(KV.class);

  @Test
  public void processElementTest() {
    final TableReference tableReference = BigQueryHelpers.parseTableSpec("a12345:b.c");
    final TableDataInsertAllResponse.InsertErrors insertError =
        new TableDataInsertAllResponse.InsertErrors();
    final TableRow retriedData = new TableRow();
    retriedData.set("a", "a");

    final BigQueryInsertError bigQueryInsertErrorRetry =
        new BigQueryInsertError(retriedData, insertError, tableReference);

    when(processContext.element()).thenReturn(bigQueryInsertErrorRetry);

    final KeyByDestTable keyByDestTable = new KeyByDestTable();
    keyByDestTable.processElement(processContext);
    verify(processContext).output(captor.capture());

    final KV<String, Schema> expectedResult =
        KV.of(tableReference.getTableId(), Schema.of(Field.of("a", StandardSQLTypeName.STRING)));
    assertThat(captor.getValue(), is(equalTo(expectedResult)));
  }
}
