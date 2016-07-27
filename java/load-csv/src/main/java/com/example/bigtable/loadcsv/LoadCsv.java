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

package com.example.bigtable.loadcsv;

import com.google.cloud.bigtable.dataflow.CloudBigtableOptions;
import com.google.cloud.bigtable.dataflow.CloudBigtableIO;
import com.google.cloud.bigtable.dataflow.CloudBigtableTableConfiguration;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.bigtable.dataflow.CloudBigtableConfiguration;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.io.bigtable.BigtableIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.Filter;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.FlatMapElements;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.values.KV;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.io.UnsupportedEncodingException;


/**
 * An example that counts words in Shakespeare.
 *
 * <p>This class, {@link MinimalWordCount}, is the first in a series of four successively more
 * detailed 'word count' examples. Here, for simplicity, we don't show any error-checking or
 * argument processing, and focus on construction of the pipeline, which chains together the
 * application of core transforms.
 *
 * <p>Next, see the {@link WordCount} pipeline, then the {@link DebuggingWordCount}, and finally
 * the {@link WindowedWordCount} pipeline, for more detailed examples that introduce additional
 * concepts.
 *
 * <p>Concepts:
 * <pre>
 *   1. Reading data from text files
 *   2. Specifying 'inline' transforms
 *   3. Counting a PCollection
 *   4. Writing data to Cloud Storage as text files
 * </pre>
 *
 * <p>To execute this pipeline, first edit the code to set your project ID, the staging
 * location, and the output location. The specified GCS bucket(s) must already exist.
 *
 * <p>Then, run the pipeline as described in the README. It will be deployed and run using the
 * Dataflow service. No args are required to run the pipeline. You can see the results in your
 * output bucket in the GCS browser.
 */
public class LoadCsv {
  private static final Charset STRING_ENCODING = StandardCharsets.UTF_8;
  private static final byte[] FAMILY = Bytes.toBytes("cf1");
  private static final byte[] COUNT_QUALIFIER = Bytes.toBytes("count");

  public static interface BigtableCsvOptions extends CloudBigtableOptions {
    String getColumnSeparator();
    void setColumnSeparator(String separator);
    String getInputFile();
    void setInputFile(String location);
  }

  public static void main(String[] args) {
    // CloudBigtableOptions is one way to retrieve the options.  It's not required.
    // https://github.com/GoogleCloudPlatform/cloud-bigtable-examples/blob/master/java/dataflow-connector-examples/src/main/java/com/google/cloud/bigtable/dataflow/example/HelloWorldWrite.java
    BigtableCsvOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(BigtableCsvOptions.class);
    CloudBigtableTableConfiguration config =
        CloudBigtableTableConfiguration.fromCBTOptions(options);

    final String columnSeparator;
    if (options.getColumnSeparator() == null) {
      columnSeparator = "\t"; // ","
    } else {
      columnSeparator = options.getColumnSeparator();
    }

    // Create the Pipeline object with the options we defined above.
    Pipeline p = Pipeline.create(options);

    CloudBigtableIO.initializeForWrite(p);

    // Apply the pipeline's transforms.

    // Concept #1: Apply a root transform to the pipeline; in this case, TextIO.Read to read a set
    // of input text files. TextIO.Read returns a PCollection where each element is one line from
    // the input text (a set of Shakespeare's texts).
    p.apply(TextIO.Read.from(options.getInputFile()))
        // Concept #2: Apply a ParDo transform to our PCollection of text lines. This ParDo invokes a
        // DoFn (defined in-line) on each element that tokenizes the text line into individual words.
        // The ParDo returns a PCollection<String>, where each element is an individual word in
        // Shakespeare's collected texts.
        .apply(
            FlatMapElements
                .via((String doc) -> Arrays.asList(doc.split("\n")))
                .withOutputType(new TypeDescriptor<String>() {}))
        .apply(Filter.byPredicate((String line) -> !line.isEmpty()))

        // Apply a MapElements transform that formats our PCollection of word counts into a printable
        // string, suitable for writing to an output file.
        .apply(MapElements.via((String line) -> {
                            String[] elements = line.split(columnSeparator);
          byte[] key = elements[1].getBytes(STRING_ENCODING);
          byte[] data = elements[2].getBytes(STRING_ENCODING);


                          return (Mutation) new Put(key).addColumn(FAMILY, COUNT_QUALIFIER, data);
        })
            .withOutputType(new TypeDescriptor<Mutation>() {}))
     
     // Concept #4: Apply a write transform, TextIO.Write, at the end of the pipeline.
     // TextIO.Write writes the contents of a PCollection (in this case, our PCollection of
     // formatted strings) to a series of text files in Google Cloud Storage.
     // CHANGE 3/3: The Google Cloud Storage path is required for outputting the results to.
     .apply(CloudBigtableIO.writeToTable(config));

    // Run the pipeline.
    p.run();
  }
}
