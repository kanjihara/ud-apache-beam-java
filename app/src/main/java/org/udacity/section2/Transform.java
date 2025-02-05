package org.udacity.section2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Transform {

    public void transformString() {
        Pipeline p2 = Pipeline.create();

        // Create a PCollection with in-memory data
        PCollection<String> lines = p2.apply(Create.of(
                Arrays.asList(
                        "Using create transform",
                        "to generate in memory data",
                        "This is 3rd line",
                        "Thanks"
                )
        ));

        // Write output to a text file
        lines.apply(TextIO.write().to("data/outCreate1").withSuffix(".txt"));

        // Run the pipeline
        p2.run().waitUntilFinish();
    }

    public void transformStringImprovement() {
        Pipeline p2 = Pipeline.create();

        p2.apply("Create", Create.of(
                Arrays.asList( "Using create transform",
                        "to generate in memory data",
                        "This is 3rd line",
                        "Thanks")
        )).apply("Write Results",TextIO.write().to("data/outCreate1").withSuffix(".txt")
                .withoutSharding());


        // Run the pipeline
        p2.run().waitUntilFinish();
    }

    public void transformObjects() {
        Pipeline p2 = Pipeline.create();

        p2.apply("Create", Create.of(Arrays.asList("maths,52", "english,75", "science,82", "computer,65", "maths,85")))  // No nested Create.of()
                .apply("Convert to String", MapElements.into(TypeDescriptors.strings()).via(element -> element))
                .apply("Write Results",
                        TextIO.write().to("data/outCreate3").withSuffix(".txt").withoutSharding());
        // Run the pipeline
        p2.run().waitUntilFinish();
    }

    public void transformArraysObjects() {
        Pipeline p2 = Pipeline.create();

        p2.apply("Create", Create.of(
                        Map.of(
                                "row1", List.of(1, 2, 3, 4, 5),
                                "row2", List.of(1, 2, 3, 4, 5)
                        ).entrySet().stream()
                                .map(entry -> entry.getKey() + ": " + entry.getValue())
                                .collect(Collectors.toList())
                ))  // No nested Create.of()
                .apply("Write Results", TextIO.write().to(
                        "data/outCreate4").withSuffix(".txt").withoutSharding()
                );
        // Run the pipeline
        p2.run().waitUntilFinish();
    }

    public void transformNumbers() {
        Pipeline p2 = Pipeline.create();

        p2.apply("Create", Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9))  // No nested Create.of()
                .apply("Convert to String", MapElements.into(TypeDescriptors.strings()).via(integer -> integer != null ? integer.toString() : null))
                .apply("Write Results",
                        TextIO.write().to("data/outCreate1").withSuffix(".txt").withoutSharding());
        // Run the pipeline
        p2.run().waitUntilFinish();
    }



    public static void main(String[] args) {
//        new Transform().transformStringImprovement();
//        new Transform().transformNumbers();
//        new Transform().transformObjects();
        new Transform().transformArraysObjects();

    }

}
