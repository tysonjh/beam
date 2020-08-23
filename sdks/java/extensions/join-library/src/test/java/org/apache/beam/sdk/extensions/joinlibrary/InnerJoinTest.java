/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.joinlibrary;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/** This test Inner Join functionality. */
public class InnerJoinTest {
  private List<KV<String, Long>> leftListOfKv = new ArrayList<>();
  private List<KV<String, String>> rightListOfKv = new ArrayList<>();
  private List<KV<String, KV<Long, String>>> expectedResult = new ArrayList<>();

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Before
  public void setup() {

    leftListOfKv = new ArrayList<>();
    rightListOfKv = new ArrayList<>();

    expectedResult = new ArrayList<>();
  }

  @Test
  public void testJoinOneToOneMapping() {
    leftListOfKv.add(KV.of("Key1", 5L));
    leftListOfKv.add(KV.of("Key2", 4L));
    PCollection<KV<String, Long>> leftCollection = p.apply("CreateLeft", Create.of(leftListOfKv));

    rightListOfKv.add(KV.of("Key1", "foo"));
    rightListOfKv.add(KV.of("Key2", "bar"));
    PCollection<KV<String, String>> rightCollection =
        p.apply("CreateRight", Create.of(rightListOfKv));

    PCollection<KV<String, KV<Long, String>>> output =
        Join.innerJoin(leftCollection, rightCollection);

    expectedResult.add(KV.of("Key1", KV.of(5L, "foo")));
    expectedResult.add(KV.of("Key2", KV.of(4L, "bar")));
    PAssert.that(output).containsInAnyOrder(expectedResult);

    p.run();
  }

  @Test
  public void testJoinOneToManyMapping() {
    leftListOfKv.add(KV.of("Key2", 4L));
    PCollection<KV<String, Long>> leftCollection = p.apply("CreateLeft", Create.of(leftListOfKv));

    rightListOfKv.add(KV.of("Key2", "bar"));
    rightListOfKv.add(KV.of("Key2", "gazonk"));
    PCollection<KV<String, String>> rightCollection =
        p.apply("CreateRight", Create.of(rightListOfKv));

    PCollection<KV<String, KV<Long, String>>> output =
        Join.innerJoin(leftCollection, rightCollection);

    expectedResult.add(KV.of("Key2", KV.of(4L, "bar")));
    expectedResult.add(KV.of("Key2", KV.of(4L, "gazonk")));
    PAssert.that(output).containsInAnyOrder(expectedResult);

    p.run();
  }

  @Test
  public void testJoinManyToOneMapping() {
    leftListOfKv.add(KV.of("Key2", 4L));
    leftListOfKv.add(KV.of("Key2", 6L));
    PCollection<KV<String, Long>> leftCollection = p.apply("CreateLeft", Create.of(leftListOfKv));

    rightListOfKv.add(KV.of("Key2", "bar"));
    PCollection<KV<String, String>> rightCollection =
        p.apply("CreateRight", Create.of(rightListOfKv));

    PCollection<KV<String, KV<Long, String>>> output =
        Join.innerJoin(leftCollection, rightCollection);

    expectedResult.add(KV.of("Key2", KV.of(4L, "bar")));
    expectedResult.add(KV.of("Key2", KV.of(6L, "bar")));
    PAssert.that(output).containsInAnyOrder(expectedResult);

    p.run();
  }

  @Test
  public void testJoinNoneToNoneMapping() {
    leftListOfKv.add(KV.of("Key2", 4L));
    PCollection<KV<String, Long>> leftCollection = p.apply("CreateLeft", Create.of(leftListOfKv));

    rightListOfKv.add(KV.of("Key3", "bar"));
    PCollection<KV<String, String>> rightCollection =
        p.apply("CreateRight", Create.of(rightListOfKv));

    PCollection<KV<String, KV<Long, String>>> output =
        Join.innerJoin(leftCollection, rightCollection);

    PAssert.that(output).containsInAnyOrder(expectedResult);
    p.run();
  }

  @Test
  public void testMultipleJoinsInSamePipeline() {
    leftListOfKv.add(KV.of("Key2", 4L));
    PCollection<KV<String, Long>> leftCollection = p.apply("CreateLeft", Create.of(leftListOfKv));

    rightListOfKv.add(KV.of("Key2", "bar"));
    PCollection<KV<String, String>> rightCollection =
        p.apply("CreateRight", Create.of(rightListOfKv));

    expectedResult.add(KV.of("Key2", KV.of(4L, "bar")));

    PCollection<KV<String, KV<Long, String>>> output1 =
        Join.innerJoin("Join1", leftCollection, rightCollection);
    PCollection<KV<String, KV<Long, String>>> output2 =
        Join.innerJoin("Join2", leftCollection, rightCollection);
    PAssert.that(output1).containsInAnyOrder(expectedResult);
    PAssert.that(output2).containsInAnyOrder(expectedResult);

    p.run();
  }

  @SuppressWarnings("nullness")
  @Test(expected = NullPointerException.class)
  public void testJoinLeftCollectionNull() {
    p.enableAbandonedNodeEnforcement(false);
    Join.innerJoin(
        null,
        p.apply(
            Create.of(rightListOfKv)
                .withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))));
  }

  @SuppressWarnings("nullness")
  @Test(expected = NullPointerException.class)
  public void testJoinRightCollectionNull() {
    p.enableAbandonedNodeEnforcement(false);
    Join.innerJoin(
        p.apply(
            Create.of(leftListOfKv).withCoder(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))),
        null);
  }

  // Temporal Join Tests
  private static class FirstCharacterEqualsFn extends SimpleFunction<KV<String, String>, Boolean> {
    @Override
    public Boolean apply(KV<String, String> input) {
      return input.getKey().charAt(0) == input.getValue().charAt(0);
    }
  }

  private static class TemporalTestRecord {
    private final String key;
    private final String value;
    private final Instant timestamp;

    private TemporalTestRecord(String key, String value, Instant timestamp) {
      this.key = key;
      this.value = value;
      this.timestamp = timestamp;
    }

    public static TemporalTestRecord of(String key, String value, Instant timestamp) {
      return new TemporalTestRecord(key, value, timestamp);
    }

    public KV<String, String> asKV() {
      return KV.of(key, String.format("%s-%s", value, timestamp));
    }

    public TimestampedValue<KV<String, String>> asTimestampedKV() {
      return TimestampedValue.of(asKV(), timestamp);
    }

    public String getKey() {
      return key;
    }

    public String getValue() {
      return value;
    }

    public Instant getTimestamp() {
      return timestamp;
    }
  }

  TimestampedValue<KV<String, String>> timestampedTemporalTestRecord(
      String key, TemporalTestRecord record) {
    return TimestampedValue.of(KV.of(key, record.toString()), record.getTimestamp());
  }

  @Test
  public void testTemporalJoinWithOneTimestampedValueStreaming() {
    Duration temporalBound = Duration.standardSeconds(1);

    TestStream<KV<String, String>> leftStream =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
            .advanceWatermarkTo(new Instant(0L))
            .addElements(TemporalTestRecord.of("key", "value", new Instant(0)).asTimestampedKV())
            .advanceWatermarkToInfinity();
    PCollection<KV<String, String>> leftCollection = p.apply("LeftStream", leftStream);

    // No elements.
    TestStream<KV<String, String>> rightStream =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
            .advanceWatermarkTo(new Instant(0L))
            .advanceWatermarkToInfinity();

    PCollection<KV<String, String>> rightCollection = p.apply("RightStream", rightStream);

    PCollection<KV<String, KV<String, String>>> output =
        Join.temporalInnerJoin(
            "Join", leftCollection, rightCollection, temporalBound, new FirstCharacterEqualsFn());
    PAssert.that(output).empty();
    p.run();
  }

  @Test
  public void testTemporalJoinWithOneJoinMatchStreaming() {
    Duration temporalBound = Duration.standardSeconds(1);

    TestStream<KV<String, String>> leftStream =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
            .advanceWatermarkTo(new Instant(0L))
            .addElements(TemporalTestRecord.of("key", "v-left", new Instant(0L)).asTimestampedKV())
            .advanceWatermarkToInfinity();
    PCollection<KV<String, String>> leftCollection = p.apply("LeftStream", leftStream);

    TestStream<KV<String, String>> rightStream =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
            .advanceWatermarkTo(new Instant(0L))
            .addElements(TemporalTestRecord.of("key", "v-right", new Instant(0L)).asTimestampedKV())
            .advanceWatermarkToInfinity();

    PCollection<KV<String, String>> rightCollection = p.apply("RightStream", rightStream);

    List<KV<String, KV<String, String>>> expected = new ArrayList<>();
    expected.add(
        KV.of(
            "key",
            KV.of(
                TemporalTestRecord.of("key", "v-left", new Instant(0L)).asKV().getValue(),
                TemporalTestRecord.of("key", "v-right", new Instant(0L)).asKV().getValue())));

    PCollection<KV<String, KV<String, String>>> output =
        Join.temporalInnerJoin(
            "Join", leftCollection, rightCollection, temporalBound, new FirstCharacterEqualsFn());
    PAssert.that(output).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  public void testTemporalJoinTemporalBoundRespectedStreaming() {
    Duration temporalBound = Duration.standardSeconds(2);

    TestStream<KV<String, String>> leftStream =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
            .advanceWatermarkTo(new Instant(0L))
            .addElements(
                TemporalTestRecord.of("key", "v-left", new Instant(0L)).asTimestampedKV(),
                TemporalTestRecord.of(
                        "key", "v-left", new Instant(0L).plus(Duration.standardSeconds(3)))
                    .asTimestampedKV())
            .advanceWatermarkToInfinity();
    PCollection<KV<String, String>> leftCollection = p.apply("LeftStream", leftStream);

    TestStream<KV<String, String>> rightStream =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
            .advanceWatermarkTo(new Instant(0L))
            .addElements(
                TemporalTestRecord.of(
                        "key", "v-right", new Instant(0L).plus(Duration.standardSeconds(4)))
                    .asTimestampedKV())
            .advanceWatermarkToInfinity();

    PCollection<KV<String, String>> rightCollection = p.apply("RightStream", rightStream);

    List<KV<String, KV<String, String>>> expected = new ArrayList<>();
    expected.add(
        KV.of(
            "key",
            KV.of(
                TemporalTestRecord.of(
                        "key", "v-left", new Instant(0L).plus(Duration.standardSeconds(3)))
                    .asKV()
                    .getValue(),
                TemporalTestRecord.of(
                        "key", "v-right", new Instant(0L).plus(Duration.standardSeconds(4)))
                    .asKV()
                    .getValue())));

    PCollection<KV<String, KV<String, String>>> output =
        Join.temporalInnerJoin(
            "Join", leftCollection, rightCollection, temporalBound, new FirstCharacterEqualsFn());
    PAssert.that(output).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  public void testTemporalJoinTemporalWatermarkExpirationStreaming() {
    Duration temporalBound = Duration.standardMinutes(5);

    TestStream<KV<String, String>> leftStream =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
            .advanceWatermarkTo(new Instant(0L))
            .addElements(
                TemporalTestRecord.of(
                        "key", "v-left", new Instant(0L).plus(Duration.standardMinutes(2)))
                    .asTimestampedKV())
            .advanceWatermarkTo(new Instant(0L).plus(Duration.standardMinutes(3)))
            .addElements(
                TemporalTestRecord.of(
                        "key", "v-left", new Instant(0L).plus(Duration.standardMinutes(30)))
                    .asTimestampedKV())
            .advanceWatermarkToInfinity();
    PCollection<KV<String, String>> leftCollection = p.apply("LeftStream", leftStream);

    TestStream<KV<String, String>> rightStream =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
            .advanceWatermarkTo(new Instant(0L))
            .addElements(
                TemporalTestRecord.of(
                        "key", "v-right", new Instant(0L).plus(Duration.standardMinutes(4)))
                    .asTimestampedKV())
            .advanceWatermarkToInfinity();

    PCollection<KV<String, String>> rightCollection = p.apply("RightStream", rightStream);

    PCollection<KV<String, KV<String, String>>> output =
        Join.temporalInnerJoin(
            "Join", leftCollection, rightCollection, temporalBound, new FirstCharacterEqualsFn());
    PAssert.that(output).empty();
    p.run();
  }
  // clean up of resident elements after elements drop out of temporal bound (e.g. watermark
  // advances)
  // test that loops through from [0, N] for both left/right and joins
}
