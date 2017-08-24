/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.connect.jdbc.source;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.sql.rowset.serial.SerialBlob;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

// Tests conversion of data types and schemas. These use the types supported by Derby, which
// might not cover everything in the SQL standards and definitely doesn't cover any non-standard
// types, but should cover most of the JDBC types which is all we see anyway
public class JdbcSourceTaskNullableExConversionTest extends JdbcSourceTaskTestBase {

  @Before
  public void setup() throws Exception {
    super.setup();
  }

  private void configAndStart( String s ) {
    Map<String,String> tableConfig = singleTableConfig();
    if (null != s)
      tableConfig.put( "column.default.test.id", "0" );
    task.start(tableConfig);
  }

  @After
  public void tearDown() throws Exception {
    task.stop();
    super.tearDown();
  }

  @Test
  public void testNullableSmallIntZero() throws Exception {
    configAndStart("0");
    Schema expectedSchema = SchemaBuilder.int16().optional().defaultValue(Short.valueOf((short)0)).build();
    typeConversion("SMALLINT", true, 1, expectedSchema, (short) 1);
    typeConversion("SMALLINT", true, null, expectedSchema, (short) 0);
  }

  @Test
  public void testNullableSmallIntNull() throws Exception {
    configAndStart(null);
    Schema expectedSchema = SchemaBuilder.int16().optional().build();
    typeConversion("SMALLINT", true, 1, expectedSchema, (short) 1);
    typeConversion("SMALLINT", true, null, expectedSchema, null);
  }

  @Test
  public void testNumeric() throws Exception {
    configAndStart("0");

    typeConversion("NUMERIC(1)", false,
            new EmbeddedDerby.Literal("CAST (1 AS NUMERIC)"),
            Schema.INT8_SCHEMA,new Byte("1"));

    Schema expectedSchema = SchemaBuilder.int8().optional().defaultValue((byte)0).build();
    typeConversion("NUMERIC(1)", true,
            null,
            expectedSchema,new Byte("0"));


    typeConversion("NUMERIC(3)", false,
            new EmbeddedDerby.Literal("CAST (123 AS NUMERIC)"),
            Schema.INT16_SCHEMA,new Short("123"));
    typeConversion("NUMERIC(5)", false,
            new EmbeddedDerby.Literal("CAST (12345 AS NUMERIC)"),
            Schema.INT32_SCHEMA,new Integer("12345"));
    typeConversion("NUMERIC(10)", false,
            new EmbeddedDerby.Literal("CAST (1234567890 AS NUMERIC(10))"),
            Schema.INT64_SCHEMA,new Long("1234567890"));
  }


/*

  @Test
  public void testNullableTinyInt() throws Exception {
    Schema expectedSchema = SchemaBuilder.int8().optional().defaultValue(Short.valueOf((short)0)).build();
    typeConversion("TINYINT", true, 1, expectedSchema, (byte) 1);
    typeConversion("TINYINT", true, null, expectedSchema, (byte) 0);
  }
*/

  // Derby has an XML type, but the JDBC driver doesn't implement any of the type bindings,
  // returning strings instead, so the XML type is not tested here

  private void typeConversion(String sqlType, boolean nullable,
                              Object sqlValue, Schema convertedSchema,
                              Object convertedValue) throws Exception {
    String sqlColumnSpec = sqlType;
    if (!nullable) {
      sqlColumnSpec += " NOT NULL";
    }
    db.createTable(SINGLE_TABLE_NAME, "id", sqlColumnSpec);
    db.insert(SINGLE_TABLE_NAME, "id", sqlValue);
    List<SourceRecord> records = task.poll();
    validateRecords(records, convertedSchema, convertedValue);
    db.dropTable(SINGLE_TABLE_NAME);
  }

  /**
   * Validates schema and type of returned record data. Assumes single-field values since this is
   * only used for validating type information.
   */
  private void validateRecords(List<SourceRecord> records, Schema expectedFieldSchema,
                               Object expectedValue) {
    // Validate # of records and object type
    assertEquals(1, records.size());
    Object objValue = records.get(0).value();
    assertTrue(objValue instanceof Struct);
    Struct value = (Struct) objValue;

    // Validate schema
    Schema schema = value.schema();
    assertEquals(Type.STRUCT, schema.type());
    List<Field> fields = schema.fields();

    assertEquals(1, fields.size());

    Schema fieldSchema = fields.get(0).schema();
    assertEquals(expectedFieldSchema, fieldSchema);
    if (expectedValue instanceof byte[]) {
      assertTrue(value.get(fields.get(0)) instanceof byte[]);
      assertEquals(ByteBuffer.wrap((byte[])expectedValue),
                   ByteBuffer.wrap((byte[])value.get(fields.get(0))));
    } else {
      assertEquals(expectedValue, value.get(fields.get(0)));
    }
  }
}
