# Loading CSVs and TSVs

The examples included in this module serve to demonstrate the basic
functionality of Google Cloud Dataflow, and act as starting points for
the development of more complex pipelines.


## Building and Running

The examples in this repository can be built and executed from the root directory by running:

Once you have followed the general Cloud Dataflow
[Getting Started](https://cloud.google.com/dataflow/getting-started) instructions, you can execute
the same pipeline on fully managed resources in Google Cloud Platform:

Either run the quickstart or get cbt and create a table.

    go get cloud.google.com/go/bigtable/cmd/cbt
    cbt -project=swast-bigtable-examples -instance=my-instance ls
    cbt -project=swast-bigtable-examples -instance=my-instance createtable helloworld
    
Run the load tool.

    mvn compile exec:java \
        -Dexec.mainClass=com.example.bigtable.loadcsv.LoadCsv \
        -Dexec.args="--project=my-project \
            --bigtableProjectId=my-project \
            --bigtableInstanceId=my-instance \
            --bigtableTableId=helloworld \
            --runner=BlockingDataflowPipelineRunner \
            --stagingLocation=gs://my-bucket/optional-prefix \
            --inputFile=gs://books/syntactic-ngrams/eng/biarcs.24-of-99.gz"


Make sure to use your project id, not the project number or the descriptive name.
The Cloud Storage location should be entered in the form of
`gs://bucket/path/to/staging/directory`.

Alternatively, you may choose to bundle all dependencies into a single JAR and
execute it outside of the Maven environment. For example, you can execute the
following commands to create the
bundled JAR of the examples and execute it both locally and in Cloud
Platform:

    mvn package

    java -cp target/google-cloud-dataflow-java-examples-all-bundled-<VERSION>.jar \
    com.google.cloud.dataflow.examples.WordCount \
    --inputFile=<INPUT FILE PATTERN> --output=<OUTPUT FILE>

    java -cp target/google-cloud-dataflow-java-examples-all-bundled-<VERSION>.jar \
    com.google.cloud.dataflow.examples.WordCount \
    --project=<YOUR CLOUD PLATFORM PROJECT ID> \
    --stagingLocation=<YOUR CLOUD STORAGE LOCATION> \
    --runner=BlockingDataflowPipelineRunner

Other examples can be run similarly by replacing the `WordCount` class path with the example classpath, e.g.
`com.google.cloud.dataflow.examples.cookbook.BigQueryTornadoes`,
and adjusting runtime options under the `Dexec.args` parameter, as specified in
the example itself.

Note that when running Maven on Microsoft Windows platform, backslashes (`\`)
under the `Dexec.args` parameter should be escaped with another backslash. For
example, input file pattern of `c:\*.txt` should be entered as `c:\\*.txt`.
