package com.example.springcloud.kafka.wordcount;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.kafka.support.serializer.JsonSerde;

public class WordCountProcessorTest {

  private TopologyTestDriver testDriver;
  public static final String INPUT_TOPIC = WordCountProcessor.INPUT_TOPIC;
  public static final String OUTPUT_TOPIC = WordCountProcessor.OUTPUT_TOPIC;

  final Serde<String> stringSerde = Serdes.String();
  final JsonSerde<WordCount> countSerde = new JsonSerde<>(WordCount.class);
  final Serde<Bytes> nullSerde = Serdes.Bytes();
  private ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(
      stringSerde.serializer(), stringSerde.serializer());

  static Properties getStreamsConfiguration() {
    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "TopologyTestDriver");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    return streamsConfiguration;
  }

  @Before
  public void setup() {
    final StreamsBuilder builder = new StreamsBuilder();
    KStream<Bytes, String> input = builder
        .stream(INPUT_TOPIC, Consumed.with(nullSerde, stringSerde));
    WordCountProcessor app = new WordCountProcessor();
    final Function<KStream<Bytes, String>, KStream<Bytes, WordCount>> process = app.process();

    final KStream<Bytes, WordCount> output = process.apply(input);
    output.to(OUTPUT_TOPIC, Produced.with(nullSerde, countSerde));
    testDriver = new TopologyTestDriver(builder.build(), getStreamsConfiguration());
  }

  @After
  public void tearDown() {
    try {
      testDriver.close();
    } catch (final RuntimeException e) {
      System.out.println("Ignoring exception, test failing in Windows due this exception:" + e
          .getLocalizedMessage());
    }
  }

  private ProducerRecord<Bytes, WordCount> readOutput() {
    return testDriver.readOutput(OUTPUT_TOPIC, nullSerde.deserializer(), countSerde.deserializer());
  }

  private Map<String, Long> getOutputList() {
    final Map<String, Long> output = new HashMap<>();
    ProducerRecord<Bytes, WordCount> outputRow;
    while ((outputRow = readOutput()) != null) {
      output.put(outputRow.value().getWord(), outputRow.value().getCount());
    }
    return output;
  }

  @Test
  public void testOneWord() {
    final String nullKey = null;
    //Feed word "Hello" to inputTopic and no kafka key, timestamp is irrelevant in this case
    testDriver.pipeInput(recordFactory.create(INPUT_TOPIC, nullKey, "Hello", 1L));
    //Read and validate output
    final ProducerRecord<Bytes, WordCount> output = readOutput();
    assertThat(output).isNotNull();
    assertThat(output.value()).isEqualToComparingFieldByField(
        new WordCount("hello", 1L, new Date(0), new Date(
            WordCountProcessor.WINDOW_SIZE_MS)));
    //No more output in topic
    assertThat(readOutput()).isNull();
  }

  @Test
  public void shouldCountWords() {
    final List<String> inputLines = Arrays.asList(
        "Kafka Streams Examples",
        "Spring Cloud Stream Sample",
        "Using Kafka Streams Test Utils"
    );
    final List<KeyValue<String, String>> inputRecords = inputLines.stream()
        .map(v -> new KeyValue<String, String>(null, v)).collect(Collectors.toList());

    final Map<String, Long> expectedWordCounts = new HashMap<>();
    expectedWordCounts.put("spring", 1L);
    expectedWordCounts.put("cloud", 1L);
    expectedWordCounts.put("examples", 1L);
    expectedWordCounts.put("sample", 1L);
    expectedWordCounts.put("streams", 2L);
    expectedWordCounts.put("stream", 1L);
    expectedWordCounts.put("test", 1L);
    expectedWordCounts.put("utils", 1L);
    expectedWordCounts.put("kafka", 2L);
    expectedWordCounts.put("using", 1L);

    testDriver.pipeInput(recordFactory
        .create(INPUT_TOPIC, inputRecords, 1L, 1000L)); //All feed in same 30s time window
    final Map<String, Long> actualWordCounts = getOutputList();
    assertThat(actualWordCounts).containsAllEntriesOf(expectedWordCounts)
        .hasSameSizeAs(expectedWordCounts);
  }

}
