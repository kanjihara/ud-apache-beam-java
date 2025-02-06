package org.udacity.section2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

public class Combiner {
    static class AverageFn extends Combine.CombineFn<Double, AverageFn.Accumulator, Double> {
        static class Accumulator {
            double sum = 0;
            int count = 0;
        }

        @Override
        public Accumulator createAccumulator() {
            return new Accumulator();
        }

        @Override
        public Accumulator addInput(Accumulator accumulator, Double input) {
            accumulator.sum += input;
            accumulator.count += 1;
            return accumulator;
        }

        @Override
        public Accumulator mergeAccumulators(Iterable<Accumulator> accumulators) {
            Accumulator merged = new Accumulator();
            for (Accumulator acc : accumulators) {
                merged.sum += acc.sum;
                merged.count += acc.count;
            }
            return merged;
        }

        @Override
        public Double extractOutput(Accumulator accumulator) {
            return accumulator.count == 0 ? Double.NaN : accumulator.sum / accumulator.count;
        }
    }

    static class PrintAverage extends SimpleFunction<Double, Void> {
        @Override
        public Void apply(Double input) {
            System.out.println("Average: " + input);
            return null;
        }
    }

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        PCollection<Double> numbers = pipeline
                .apply("Create Numbers", Create.of(15.0, 5.0, 7.0, 7.0, 9.0, 23.0, 13.0, 5.0))
                .setCoder(DoubleCoder.of());

        PCollection<Double> average = numbers
                .apply("Combine Globally", Combine.globally(new AverageFn()).withoutDefaults())
                .setCoder(DoubleCoder.of());

        average.apply("Print Average", MapElements.via(new PrintAverage()));

        pipeline.run().waitUntilFinish();
    }
}
