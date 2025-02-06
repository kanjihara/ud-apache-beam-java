package org.udacity.section2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;
import java.util.Arrays;

public class Branching {

    // Named static DoFn for splitting rows
    static class SplitRowFn extends DoFn<String, String[]> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String[]> out) {
            out.output(element.split(","));
        }
    }

    public void branching(){
        // Create a Beam pipeline
        Pipeline p = Pipeline.create();

        // Read data from text file
        PCollection<String> inputCollection = p.apply("Read from text file",
                TextIO.read().from("data/input/dept_data.txt"));

        // Split rows (assuming a function SplitRow is implemented separately)
        PCollection<String[]> splitRows = inputCollection.apply("SplitRow", ParDo.of(new SplitRowFn()));

        // Process Accounts department
        PCollection<KV<String, Integer>> accountsCount = splitRows
                .apply("Get all Accounts dept persons", Filter.by(record -> record[3].trim().equals("Accounts")))
                .apply("Pair each accounts employee with 1",
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                                .via(record -> KV.of("Accounts, " + record[1], 1)))
                .apply("Group and sum", Combine.perKey(Sum.ofIntegers()));

        // Process HR department
        PCollection<KV<String, Integer>> hrCount = splitRows
                .apply("Get all HR dept persons", Filter.by(record -> record[3].trim().equals("HR")))
                .apply("Pair each hr employee with 1",
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                                .via(record -> KV.of("HR, " + record[1], 1)))
                .apply("Group and sum", Combine.perKey(Sum.ofIntegers()));

        // Merge and write output
        PCollection<KV<String, Integer>> output = PCollectionList.of(accountsCount).and(hrCount)
                .apply("Flatten", Flatten.pCollections());

        // Convert KV<String, Integer> to String for writing
        PCollection<String> outputFormatted = output.apply("Format Output",
                MapElements.into(TypeDescriptors.strings())
                        .via(kv -> kv.getKey() + ": " + kv.getValue()));

        // Write results to a file
        outputFormatted.apply("Write results to file",
                TextIO.write().to("data/both").withSuffix(".txt").withoutSharding());


        // Run the pipeline
        p.run().waitUntilFinish();

    }

    public static void main(String[] args) {
        new Branching().branching();

    }
}
