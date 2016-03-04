/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.dataflow.example;

import java.util.Collections;
import java.util.Map;

import kafka.serializer.StringDecoder;

import org.joda.time.Duration;
import org.joda.time.Instant;

import com.cloudera.dataflow.io.ConsoleIO;
import com.cloudera.dataflow.io.KafkaIO;
import com.cloudera.dataflow.spark.SparkPipelineRunner;
import com.cloudera.dataflow.spark.streaming.SparkStreamingPipelineOptions;
import com.cloudera.dataflow.spark.streaming.SparkStreamingPipelineOptionsFactory;
import com.esotericsoftware.minlog.Log;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.ImmutableMap;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterWatermark;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;


public class WordCount {
    private static final long TEST_TIMEOUT_MSEC = 1000L;
    private static final String TOPIC = "kafka_dataflow_test_topic";
    
	static class ExtractWordsFn extends DoFn<String, String> {
    private final Aggregator<Long, Long> emptyLines =
        createAggregator("emptyLines", new Sum.SumLongFn());

    @Override
    public void processElement(ProcessContext c) {
    //	Log.info("----processElement.processElement :" + c.element());
    	
      if (c.element().trim().isEmpty()) {
        emptyLines.addValue(1L);
      }

      // Split the line into words.
      String[] words = c.element().split("[^a-zA-Z']+");

      // Output each word encountered into the output PCollection.
      for (String word : words) {
        if (!word.isEmpty()) {
          c.output(word);
        }
      }
    }
  }

  public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
    @Override
    public String apply(KV<String, Long> input) {
    	
      //Log.info("----OUTPUT:" + input.getKey() + ": " + input.getValue());	
      return input.getKey() + ": " + input.getValue();
    }
  }

  public static class CountWords extends PTransform<PCollection<String>,
      PCollection<KV<String, Long>>> {
    @Override
    public PCollection<KV<String, Long>> apply(PCollection<String> lines) {

    //	Log.info("----CountWords 1:" + lines.toString());
      // Convert lines of text into individual words.
      PCollection<String> words = lines.apply(
          ParDo.of(new ExtractWordsFn()));

      // Count the number of times each word occurs.
      PCollection<KV<String, Long>> wordCounts =
          words.apply(Count.<String>perElement());

      return wordCounts;
    }
  }

  public static void main(String[] args) {
    SparkStreamingPipelineOptions options = SparkStreamingPipelineOptionsFactory.create();
    options.setAppName(WordCount.class.getClass().getSimpleName());
    options.setRunner(SparkPipelineRunner.class);
    options.setTimeout(TEST_TIMEOUT_MSEC);
    Pipeline p = Pipeline.create(options);

    Map<String, String> kafkaParams = ImmutableMap.of(
            "metadata.broker.list", "localhost:9092",
            "auto.offset.reset", "smallest"
    );

    PCollection<KV<String, String>> kafkaInput = p.apply(KafkaIO.Read.from(StringDecoder.class,
        StringDecoder.class, String.class, String.class, Collections.singleton(TOPIC),
        kafkaParams));

    PCollection<KV<String, String>> windowedWords = kafkaInput
        .apply(Window.<KV<String, String>>into(FixedWindows.of(Duration.standardMinutes(5))));
        		//.triggering(AfterWatermark.pastEndOfWindow()).withAllowedLateness(Duration.ZERO)
				//.discardingFiredPanes());
    
    PCollection<String> formattedKV = windowedWords.apply(ParDo.of(new FormatKVFn()));
    formattedKV.apply(new CountWords())
    	.apply(MapElements.via(new FormatAsTextFn()))
    	//.apply(ConsoleIO.Write.<String>from());
    	.apply(TextIO.Write.named("WriteCounts").to("hdfs://localhost:9000/user/yunxu/out-wordcount"));
    
    p.run();
    
    while(true)
    {
    	try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    }
  }

  private static class FormatKVFn extends DoFn<KV<String, String>, String> 
  {
	    @Override
	    public void processElement(ProcessContext c) {
	      //c.output();
	    	
	      c.outputWithTimestamp(c.element().getKey() + "," + c.element().getValue(), Instant.now());
	      
	     // Log.info("----KAFKA INPUT:" + c.element().getKey() + "," + c.element().getValue());
	    }
	  }
}
