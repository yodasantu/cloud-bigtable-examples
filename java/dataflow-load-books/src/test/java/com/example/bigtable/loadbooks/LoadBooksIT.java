/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.example.bigtable.loadbooks;

import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for the {@link LoadBooks} Dataflow pipeline.
 */
@RunWith(JUnit4.class)
public class LoadBooksIT {
  private static final String EMULATOR_ENV = "BIGTABLE_EMULATOR_HOST";
  private static final String INSTANCE_ENV = "BIGTABLE_INSTANCE_ID";
  private static final String PROJECT_ENV = "GOOGLE_CLOUD_PROJECT";
  private static final byte[] TABLE_NAME = Bytes.toBytes("helloworld");
  private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("cf1");
  private static final byte[] COLUMN_NAME = Bytes.toBytes("greeting");
  Connection bigtableConnection;

  @Before
  public void setUp() throws Exception {
    String emulator = System.getenv(EMULATOR_ENV);
    boolean isEmulatorSet = emulator != null && !emulator.isEmpty();
    String project = System.getenv(PROJECT_ENV);

    if (project == null || project.isEmpty()) {
      project = "ignored";
      assert isEmulatorSet : String.format(
          "%s must be set if %s is not set", EMULATOR_ENV, PROJECT_ENV);
    }

    String instance = System.getenv(INSTANCE_ENV);

    if (instance == null || instance.isEmpty()) {
      instance = "ignored";
      assert isEmulatorSet : String.format(
          "%s must be set if %s is not set", EMULATOR_ENV, INSTANCE_ENV);
    }

    Configuration configuration = BigtableConfiguration.configure(project, instance);
    bigtableConnection = BigtableConfiguration.connect(configuration);
  }

  @After
  public void tearDown() throws Exception {
    bigtableConnection.close();
  }

  @Test
  public void main_writesBigtable() throws Exception {
    Connection connection = bigtableConnection;

      // [START writing_rows]
      // Retrieve the table we just created so we can do some reads and writes
    System.out.println("In test");
      Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
    System.out.println("Got table");

      // Write some rows to the table
        // Each row has a unique row key.
        //
        // Note: This example uses sequential numeric IDs for simplicity, but
        // this can result in poor performance in a production application.
        // Since rows are stored in sorted order by key, sequential keys can
        // result in poor distribution of operations across nodes.
        //
        // For more information about how to design a Bigtable schema for the
        // best performance, see the documentation:
        //
        //     https://cloud.google.com/bigtable/docs/schema-design
        String rowKey = "greeting";

        // Put a single row into the table. We could also pass a list of Puts to write a batch.
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(COLUMN_FAMILY_NAME, COLUMN_NAME, Bytes.toBytes("Hello, Tim."));
    System.out.println("made greeting");
        table.put(put);
    System.out.println("put greeting");
      // [END writing_rows]

      // [START getting_a_row]
      // Get the first greeting by row key
      Result getResult = table.get(new Get(Bytes.toBytes(rowKey)));
      String greeting = Bytes.toString(getResult.getValue(COLUMN_FAMILY_NAME, COLUMN_NAME));
      System.out.println("Get a single greeting by row key");
      System.out.printf("\t%s = %s\n", rowKey, greeting);
      // [END getting_a_row]

      // [START scanning_all_rows]
      // Now scan across all rows.
      Scan scan = new Scan();

      ResultScanner scanner = table.getScanner(scan);
      for (Result row : scanner) {
        byte[] valueBytes = row.getValue(COLUMN_FAMILY_NAME, COLUMN_NAME);
        System.out.println('\t' + Bytes.toString(valueBytes));
      }
      // [END scanning_all_rows]
  }
}
