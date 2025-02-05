package org.udacity.section2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.TypeDescriptors;
import java.io.Serializable;

public class ReadElements {

    // Named static DoFn for splitting rows
    static class SplitRowFn extends DoFn<String, String[]> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String[]> out) {
            out.output(element.split(","));
        }
    }

    // Named static SerializableFunction for filtering
    static class FilterAccountsFn implements SerializableFunction<String[], Boolean>, Serializable {
        @Override
        public Boolean apply(String[] record) {
            return record[3].equals("Accounts");
        }
    }

    // Named static DoFn for mapping to KV pairs
    static class MapToKeyValueFn extends DoFn<String[], KV<String, Integer>> {
        @ProcessElement
        public void processElement(@Element String[] record, OutputReceiver<KV<String, Integer>> out) {
            out.output(KV.of(record[1], 1));
        }
    }

    public void readElements() {
        Pipeline p = Pipeline.create();



        p.apply("ReadLines", TextIO.read().from("data/input/dept_data.txt"))
                .apply("SplitRow", ParDo.of(new SplitRowFn()))
                .apply("FilterAccounts", Filter.by(new FilterAccountsFn()))
                .apply("MapToKeyValue", ParDo.of(new MapToKeyValueFn()))
                .apply("SumCounts", Combine.perKey(Sum.ofIntegers()))
                .apply("FormatResults", MapElements.into(TypeDescriptors.strings())
                        .via((KV<String, Integer> kv) -> kv.getKey() + ": " + kv.getValue()))
                .apply("WriteResults", TextIO.write().to("data/output/output_new_final"));

        p.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        new ReadElements().readElements();
    }
}
