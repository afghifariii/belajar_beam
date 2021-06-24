package com.digitalskola;

import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;


public class Main {

    public static void main(String[] args) {
        // write your code here
        PipelineOptions options = PipelineOptionsFactory.as(DirectOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        List<String> topics = new ArrayList<>(Collections.singletonList("data_pengguna"));
        Map<String, Object> consumerConfig = new HashMap<String, Object>() {{
            put("group.id", "beam_streaming_1");
            put("auto.offset.reset", "earliest");
        }};

        PCollection<KafkaRecord<String, String>> streamData = pipeline.apply("Read from kafka", KafkaIO.<String, String>read()
                .withBootstrapServers("localhost:29092")
                .withTopics(topics)
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withConsumerConfigUpdates(consumerConfig)

        );

        streamData.apply("Print Record", ParDo.of(new DoFn<KafkaRecord<String, String>, Void>() {
            @ProcessElement
            public void processElement(ProcessContext processContext) {
                KV<String, String> kafkaRecord = processContext.element().getKV();
                String kafkaValue = kafkaRecord.getValue().replace("\n", " ");
                System.out.println(kafkaValue);
            }
        }));

        pipeline.run();
    }
}
