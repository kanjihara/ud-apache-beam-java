package org.udacity.section2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import java.util.ArrayList;
import java.util.List;

public class Pardo {
    static class SplitRow extends DoFn<String, List<String>> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<List<String>> out) {
            String[] parts = element.split(",");
            List<String> list = new ArrayList<>();
            for (String part : parts) {
                list.add(part);
            }
            out.output(list);
        }
    }

    static class FilterAccountsEmployee extends DoFn<List<String>, List<String>> {
        @ProcessElement
        public void processElement(@Element List<String> element, OutputReceiver<List<String>> out) {
            if (element.get(3).equals("Accounts")) {
                out.output(element);
            }
        }
    }

    static class PairEmployees extends DoFn<List<String>, KV<String, Integer>> {
        @ProcessElement
        public void processElement(@Element List<String> element, OutputReceiver<KV<String, Integer>> out) {
            out.output(KV.of(element.get(3) + "," + element.get(1), 1));
        }
    }

    static class Counting extends DoFn<KV<String, Iterable<Integer>>, KV<String, Integer>> {
        @ProcessElement
        public void processElement(@Element KV<String, Iterable<Integer>> element, OutputReceiver<KV<String, Integer>> out) {
            int sum = 0;
            for (int value : element.getValue()) {
                sum += value;
            }
            out.output(KV.of(element.getKey(), sum));
        }
    }

    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        PCollection<String> lines = p.apply(TextIO.read().from("data/input/dept_data.txt"));

        PCollection<List<String>> splitRows = lines.apply(ParDo.of(new SplitRow()));
        PCollection<List<String>> filteredEmployees = splitRows.apply(ParDo.of(new FilterAccountsEmployee()));
        PCollection<KV<String, Integer>> pairedEmployees = filteredEmployees.apply(ParDo.of(new PairEmployees()));
        PCollection<KV<String, Iterable<Integer>>> grouped = pairedEmployees.apply(GroupByKey.create());
        PCollection<KV<String, Integer>> counted = grouped.apply(ParDo.of(new Counting()));

        counted.apply(MapElements.via(new SimpleFunction<KV<String, Integer>, Void>() {
            @Override
            public Void apply(KV<String, Integer> input) {
                System.out.println(input.getKey() + ": " + input.getValue());
                return null;
            }
        }));

        p.run().waitUntilFinish();
    }
}
