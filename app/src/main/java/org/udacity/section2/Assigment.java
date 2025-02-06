package org.udacity.section2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import java.util.Arrays;

public class Assigment {

    public static void main(String[] args) {
        countWords();
    }

    private static void countWords() {
        // Create a Beam pipeline
        Pipeline p = Pipeline.create();

        // Read data from text file and process
        PCollection<String> input = p.apply("Read file", TextIO.read().from("data/input/data.txt"));
//        input.apply("Print Read file", MapElements.into(TypeDescriptors.strings()).via(line -> { System.out.println(line); return line; }));

        PCollection<String> words = input.apply("Split between blank space", FlatMapElements.into(TypeDescriptors.strings())
                .via(line -> Arrays.asList(line.split(" "))));
//        words.apply("Print Split words", MapElements.into(TypeDescriptors.strings()).via(word -> { System.out.println(word); return word; }));

        PCollection<Long> wordOnes = words.apply("Convert to Long", MapElements.into(TypeDescriptors.longs()).via(word -> 1L));
        wordOnes.apply("Print Converted to Long", MapElements.into(TypeDescriptors.longs()).via(count -> { System.out.println(count); return count; }));

        PCollection<Long> wordCount = wordOnes.apply("Count words", Combine.globally(Sum.ofLongs()));
        wordCount.apply("Print Count words", MapElements.into(TypeDescriptors.longs()).via(count -> { System.out.println(count); return count; }));

        // Write results to file
        wordCount.apply("Convert to String", MapElements.into(TypeDescriptors.strings()).via(Object::toString))
                .apply("Write results to file", TextIO.write().to("section-2/data/output_new_final1").withSuffix(".txt"));

        // Run the pipeline
        p.run().waitUntilFinish();
    }
}
