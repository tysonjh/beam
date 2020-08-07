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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.function.Function;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class with different versions of joins. All methods join two collections of key/value
 * pairs (KV).
 */
public class Join {

  /**
   * PTransform representing an inner join of two collections of KV elements.
   *
   * @param <K> Type of the key for both collections
   * @param <V1> Type of the values for the left collection.
   * @param <V2> Type of the values for the right collection.
   */
  public static class InnerJoin<K, V1, V2>
      extends PTransform<PCollection<KV<K, V1>>, PCollection<KV<K, KV<V1, V2>>>> {

    private transient PCollection<KV<K, V2>> rightCollection;

    private InnerJoin(PCollection<KV<K, V2>> rightCollection) {
      this.rightCollection = rightCollection;
    }

    public static <K, V1, V2> InnerJoin<K, V1, V2> with(PCollection<KV<K, V2>> rightCollection) {
      return new InnerJoin<>(rightCollection);
    }

    @Override
    public PCollection<KV<K, KV<V1, V2>>> expand(PCollection<KV<K, V1>> leftCollection) {
      checkNotNull(leftCollection);
      checkNotNull(rightCollection);

      final TupleTag<V1> v1Tuple = new TupleTag<>();
      final TupleTag<V2> v2Tuple = new TupleTag<>();

      PCollection<KV<K, CoGbkResult>> coGbkResultCollection =
          KeyedPCollectionTuple.of(v1Tuple, leftCollection)
              .and(v2Tuple, rightCollection)
              .apply("CoGBK", CoGroupByKey.create());

      return coGbkResultCollection
          .apply(
              "Join",
              ParDo.of(
                  new DoFn<KV<K, CoGbkResult>, KV<K, KV<V1, V2>>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      KV<K, CoGbkResult> e = c.element();

                      Iterable<V1> leftValuesIterable = e.getValue().getAll(v1Tuple);
                      Iterable<V2> rightValuesIterable = e.getValue().getAll(v2Tuple);

                      for (V1 leftValue : leftValuesIterable) {
                        for (V2 rightValue : rightValuesIterable) {
                          c.output(KV.of(e.getKey(), KV.of(leftValue, rightValue)));
                        }
                      }
                    }
                  }))
          .setCoder(
              KvCoder.of(
                  ((KvCoder<K, V1>) leftCollection.getCoder()).getKeyCoder(),
                  KvCoder.of(
                      ((KvCoder<K, V1>) leftCollection.getCoder()).getValueCoder(),
                      ((KvCoder<K, V2>) rightCollection.getCoder()).getValueCoder())));
    }
  }

  /**
   * PTransform representing a left outer join of two collections of KV elements.
   *
   * @param <K> Type of the key for both collections
   * @param <V1> Type of the values for the left collection.
   * @param <V2> Type of the values for the right collection.
   */
  public static class LeftOuterJoin<K, V1, V2>
      extends PTransform<PCollection<KV<K, V1>>, PCollection<KV<K, KV<V1, V2>>>> {

    private transient PCollection<KV<K, V2>> rightCollection;
    private V2 nullValue;

    private LeftOuterJoin(PCollection<KV<K, V2>> rightCollection, V2 nullValue) {
      this.rightCollection = rightCollection;
      this.nullValue = nullValue;
    }

    public static <K, V1, V2> LeftOuterJoin<K, V1, V2> with(
        PCollection<KV<K, V2>> rightCollection, V2 nullValue) {
      return new LeftOuterJoin<>(rightCollection, nullValue);
    }

    @Override
    public PCollection<KV<K, KV<V1, V2>>> expand(PCollection<KV<K, V1>> leftCollection) {
      checkNotNull(leftCollection);
      checkNotNull(rightCollection);
      checkNotNull(nullValue);
      final TupleTag<V1> v1Tuple = new TupleTag<>();
      final TupleTag<V2> v2Tuple = new TupleTag<>();

      PCollection<KV<K, CoGbkResult>> coGbkResultCollection =
          KeyedPCollectionTuple.of(v1Tuple, leftCollection)
              .and(v2Tuple, rightCollection)
              .apply("CoGBK", CoGroupByKey.create());

      return coGbkResultCollection
          .apply(
              "Join",
              ParDo.of(
                  new DoFn<KV<K, CoGbkResult>, KV<K, KV<V1, V2>>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      KV<K, CoGbkResult> e = c.element();

                      Iterable<V1> leftValuesIterable = e.getValue().getAll(v1Tuple);
                      Iterable<V2> rightValuesIterable = e.getValue().getAll(v2Tuple);

                      for (V1 leftValue : leftValuesIterable) {
                        if (rightValuesIterable.iterator().hasNext()) {
                          for (V2 rightValue : rightValuesIterable) {
                            c.output(KV.of(e.getKey(), KV.of(leftValue, rightValue)));
                          }
                        } else {
                          c.output(KV.of(e.getKey(), KV.of(leftValue, nullValue)));
                        }
                      }
                    }
                  }))
          .setCoder(
              KvCoder.of(
                  ((KvCoder<K, V1>) leftCollection.getCoder()).getKeyCoder(),
                  KvCoder.of(
                      ((KvCoder<K, V1>) leftCollection.getCoder()).getValueCoder(),
                      ((KvCoder<K, V2>) rightCollection.getCoder()).getValueCoder())));
    }
  }

  /**
   * PTransform representing a right outer join of two collections of KV elements.
   *
   * @param <K> Type of the key for both collections
   * @param <V1> Type of the values for the left collection.
   * @param <V2> Type of the values for the right collection.
   */
  public static class RightOuterJoin<K, V1, V2>
      extends PTransform<PCollection<KV<K, V1>>, PCollection<KV<K, KV<V1, V2>>>> {

    private transient PCollection<KV<K, V2>> rightCollection;
    private V1 nullValue;

    private RightOuterJoin(PCollection<KV<K, V2>> rightCollection, V1 nullValue) {
      this.rightCollection = rightCollection;
      this.nullValue = nullValue;
    }

    public static <K, V1, V2> RightOuterJoin<K, V1, V2> with(
        PCollection<KV<K, V2>> rightCollection, V1 nullValue) {
      return new RightOuterJoin<>(rightCollection, nullValue);
    }

    @Override
    public PCollection<KV<K, KV<V1, V2>>> expand(PCollection<KV<K, V1>> leftCollection) {
      checkNotNull(leftCollection);
      checkNotNull(rightCollection);
      checkNotNull(nullValue);

      final TupleTag<V1> v1Tuple = new TupleTag<>();
      final TupleTag<V2> v2Tuple = new TupleTag<>();

      PCollection<KV<K, CoGbkResult>> coGbkResultCollection =
          KeyedPCollectionTuple.of(v1Tuple, leftCollection)
              .and(v2Tuple, rightCollection)
              .apply("CoGBK", CoGroupByKey.create());

      return coGbkResultCollection
          .apply(
              "Join",
              ParDo.of(
                  new DoFn<KV<K, CoGbkResult>, KV<K, KV<V1, V2>>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      KV<K, CoGbkResult> e = c.element();

                      Iterable<V1> leftValuesIterable = e.getValue().getAll(v1Tuple);
                      Iterable<V2> rightValuesIterable = e.getValue().getAll(v2Tuple);

                      for (V2 rightValue : rightValuesIterable) {
                        if (leftValuesIterable.iterator().hasNext()) {
                          for (V1 leftValue : leftValuesIterable) {
                            c.output(KV.of(e.getKey(), KV.of(leftValue, rightValue)));
                          }
                        } else {
                          c.output(KV.of(e.getKey(), KV.of(nullValue, rightValue)));
                        }
                      }
                    }
                  }))
          .setCoder(
              KvCoder.of(
                  ((KvCoder<K, V1>) leftCollection.getCoder()).getKeyCoder(),
                  KvCoder.of(
                      ((KvCoder<K, V1>) leftCollection.getCoder()).getValueCoder(),
                      ((KvCoder<K, V2>) rightCollection.getCoder()).getValueCoder())));
    }
  }

  /**
   * PTransform representing a full outer join of two collections of KV elements.
   *
   * @param <K> Type of the key for both collections
   * @param <V1> Type of the values for the left collection.
   * @param <V2> Type of the values for the right collection.
   */
  public static class FullOuterJoin<K, V1, V2>
      extends PTransform<PCollection<KV<K, V1>>, PCollection<KV<K, KV<V1, V2>>>> {

    private transient PCollection<KV<K, V2>> rightCollection;
    private V1 leftNullValue;
    private V2 rightNullValue;

    private FullOuterJoin(
        PCollection<KV<K, V2>> rightCollection, V1 leftNullValue, V2 rightNullValue) {
      this.rightCollection = rightCollection;
      this.leftNullValue = leftNullValue;
      this.rightNullValue = rightNullValue;
    }

    public static <K, V1, V2> FullOuterJoin<K, V1, V2> with(
        PCollection<KV<K, V2>> rightCollection, V1 leftNullValue, V2 rightNullValue) {
      return new FullOuterJoin<>(rightCollection, leftNullValue, rightNullValue);
    }

    @Override
    public PCollection<KV<K, KV<V1, V2>>> expand(PCollection<KV<K, V1>> leftCollection) {
      checkNotNull(leftCollection);
      checkNotNull(rightCollection);
      checkNotNull(leftNullValue);
      checkNotNull(rightNullValue);

      final TupleTag<V1> v1Tuple = new TupleTag<>();
      final TupleTag<V2> v2Tuple = new TupleTag<>();

      PCollection<KV<K, CoGbkResult>> coGbkResultCollection =
          KeyedPCollectionTuple.of(v1Tuple, leftCollection)
              .and(v2Tuple, rightCollection)
              .apply("CoGBK", CoGroupByKey.create());

      return coGbkResultCollection
          .apply(
              "Join",
              ParDo.of(
                  new DoFn<KV<K, CoGbkResult>, KV<K, KV<V1, V2>>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      KV<K, CoGbkResult> e = c.element();

                      Iterable<V1> leftValuesIterable = e.getValue().getAll(v1Tuple);
                      Iterable<V2> rightValuesIterable = e.getValue().getAll(v2Tuple);
                      if (leftValuesIterable.iterator().hasNext()
                          && rightValuesIterable.iterator().hasNext()) {
                        for (V2 rightValue : rightValuesIterable) {
                          for (V1 leftValue : leftValuesIterable) {
                            c.output(KV.of(e.getKey(), KV.of(leftValue, rightValue)));
                          }
                        }
                      } else if (leftValuesIterable.iterator().hasNext()
                          && !rightValuesIterable.iterator().hasNext()) {
                        for (V1 leftValue : leftValuesIterable) {
                          c.output(KV.of(e.getKey(), KV.of(leftValue, rightNullValue)));
                        }
                      } else if (!leftValuesIterable.iterator().hasNext()
                          && rightValuesIterable.iterator().hasNext()) {
                        for (V2 rightValue : rightValuesIterable) {
                          c.output(KV.of(e.getKey(), KV.of(leftNullValue, rightValue)));
                        }
                      }
                    }
                  }))
          .setCoder(
              KvCoder.of(
                  ((KvCoder<K, V1>) leftCollection.getCoder()).getKeyCoder(),
                  KvCoder.of(
                      ((KvCoder<K, V1>) leftCollection.getCoder()).getValueCoder(),
                      ((KvCoder<K, V2>) rightCollection.getCoder()).getValueCoder())));
    }
  }

  /**
   * Inner join of two collections of KV elements.
   *
   * @param leftCollection Left side collection to join.
   * @param rightCollection Right side collection to join.
   * @param <K> Type of the key for both collections
   * @param <V1> Type of the values for the left collection.
   * @param <V2> Type of the values for the right collection.
   * @return A joined collection of KV where Key is the key and value is a KV where Key is of type
   *     V1 and Value is type V2.
   */
  public static <K, V1, V2> PCollection<KV<K, KV<V1, V2>>> innerJoin(
      final PCollection<KV<K, V1>> leftCollection, final PCollection<KV<K, V2>> rightCollection) {
    return innerJoin("InnerJoin", leftCollection, rightCollection);
  }

  /**
   * Inner join of two collections of KV elements.
   *
   * @param name Name of the PTransform.
   * @param leftCollection Left side collection to join.
   * @param rightCollection Right side collection to join.
   * @param <K> Type of the key for both collections
   * @param <V1> Type of the values for the left collection.
   * @param <V2> Type of the values for the right collection.
   * @return A joined collection of KV where Key is the key and value is a KV where Key is of type
   *     V1 and Value is type V2.
   */
  public static <K, V1, V2> PCollection<KV<K, KV<V1, V2>>> innerJoin(
      final String name,
      final PCollection<KV<K, V1>> leftCollection,
      final PCollection<KV<K, V2>> rightCollection) {
    return leftCollection.apply(name, InnerJoin.with(rightCollection));
  }

  public static class TemporalInnerJoin<K, V1, V2>
      extends PTransform<PCollection<KV<K, V1>>, PCollection<KV<K, KV<V1, V2>>>> {
    private final transient PCollection<KV<K, V2>> rightCollection;
    private final Duration temporalBound;
    private final SimpleFunction<KV<V1, V2>, Boolean> comparatorFn;

    private TemporalInnerJoin(
        final PCollection<KV<K, V2>> rightCollection,
        final Duration temporalBound,
        final SimpleFunction<KV<V1, V2>, Boolean> compareFn) {
      this.temporalBound = temporalBound;
      this.rightCollection = rightCollection;
      this.comparatorFn = compareFn;
    }

    public static <K, V1, V2> TemporalInnerJoin<K, V1, V2> with(
        PCollection<KV<K, V2>> rightCollection,
        Duration temporalBound, // Note: non-inclusive!!
        SimpleFunction<KV<V1, V2>, Boolean> compareFn) {
      return new TemporalInnerJoin<>(rightCollection, temporalBound, compareFn);
    }

    @Override
    public PCollection<KV<K, KV<V1, V2>>> expand(PCollection<KV<K, V1>> leftCollection) {
      // left        right
      // tag-left    tag-right (create union type)
      //   \         /
      //     flatten
      //     join

      Coder<K> keyCoder = ((KvCoder<K, V1>) leftCollection.getCoder()).getKeyCoder();
      Coder<V1> leftValueCoder = ((KvCoder<K, V1>) leftCollection.getCoder()).getValueCoder();
      Coder<V2> rightValueCoder = ((KvCoder<K, V2>) rightCollection.getCoder()).getValueCoder();

      PCollection<KV<K, KV<V1, V2>>> leftUnion =
          leftCollection
              .apply("LeftUnionTag", MapElements.via(new LeftUnionTagFn<K, V1, V2>()))
              .setCoder(
                  KvCoder.of(
                      keyCoder,
                      KvCoder.of(
                          NullableCoder.of(leftValueCoder), NullableCoder.of(rightValueCoder))));

      PCollection<KV<K, KV<V1, V2>>> rightUnion =
          rightCollection
              .apply("RightUnionTag", MapElements.via(new RightUnionTagFn<K, V1, V2>()))
              .setCoder(
                  KvCoder.of(
                      keyCoder,
                      KvCoder.of(
                          NullableCoder.of(leftValueCoder), NullableCoder.of(rightValueCoder))));

      return PCollectionList.of(leftUnion)
          .and(rightUnion)
          .apply(Flatten.pCollections())
          .apply(
              "JoinFn",
              ParDo.of(
                  new TemporalInnerJoinFn<>(
                      leftValueCoder, rightValueCoder, temporalBound, comparatorFn)));
    }
  }

  private static class LeftUnionTagFn<K, V1, V2>
      extends SimpleFunction<KV<K, V1>, KV<K, KV<V1, V2>>> {
    @Override
    public KV<K, KV<V1, V2>> apply(KV<K, V1> element) {
      return KV.of(element.getKey(), KV.of(element.getValue(), null));
    }
  }

  private static class RightUnionTagFn<K, V1, V2>
      extends SimpleFunction<KV<K, V2>, KV<K, KV<V1, V2>>> {
    @Override
    public KV<K, KV<V1, V2>> apply(KV<K, V2> element) {
      return KV.of(element.getKey(), KV.of(null, element.getValue()));
    }
  }

  private static class TemporalInnerJoinFn<K, V1, V2>
      extends DoFn<KV<K, KV<V1, V2>>, KV<K, KV<V1, V2>>> {
    private enum TimerState {
      UNINITIALIZED,
      UNSET,
      SET
    }

    private static final Logger LOG = LoggerFactory.getLogger(TemporalInnerJoinFn.class);

    @StateId("left")
    private final StateSpec<OrderedListState<V1>> leftStateSpec;

    @StateId("right")
    private final StateSpec<OrderedListState<V2>> rightStateSpec;

    @TimerId("eviction")
    private final TimerSpec evictionSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    private final Duration temporalBound;
    private final SimpleFunction<KV<V1, V2>, Boolean> compareFn;
    private transient Duration evictionFrequency;
    private transient TimerState evictionTimerState;
    private transient Instant temporalBoundWatermark;

    @Setup
    public void setup() {
      evictionFrequency = temporalBound.dividedBy(4);
      evictionTimerState = TimerState.UNINITIALIZED;
      temporalBoundWatermark = new Instant(0L);
    }

    protected TemporalInnerJoinFn(
        final Coder<V1> leftCoder,
        final Coder<V2> rightCoder,
        final Duration temporalBound,
        SimpleFunction<KV<V1, V2>, Boolean> compareFn) {
      this.leftStateSpec = StateSpecs.orderedList(leftCoder);
      this.rightStateSpec = StateSpecs.orderedList(rightCoder);
      this.temporalBound = temporalBound;
      this.compareFn = compareFn;
      this.evictionFrequency = temporalBound.dividedBy(4);
      this.evictionTimerState = TimerState.UNINITIALIZED;
      this.temporalBoundWatermark = new Instant(0L);
    }

    @Nullable
    private <Q, R> TimestampedValue<R> findMatch(Instant timestamp, Q input, OrderedListState<R> search, Function<TimestampedValue<R>, Boolean> func) {
      checkNotNull(input);
      Iterable<TimestampedValue<R>> searchIterable = search.readRange(timestamp.minus(temporalBound), timestamp.plus(temporalBound));
      if (searchIterable != null) {
        for (TimestampedValue<R> current : searchIterable) {
          if (new Duration(current.getTimestamp(), timestamp).abs().isShorterThan(temporalBound)
              && func.apply(current)) {
            return current;
          }
        }
      }
      return null;
    }

    @ProcessElement
    public void processElement(
        ProcessContext c,
        @StateId("left") OrderedListState<V1> leftState,
        @StateId("right") OrderedListState<V2> rightState,
        @Timestamp Instant timestamp,
        @TimerId("eviction") Timer evictionTimer) {
      // TODO: What's the idiomatic way set a callback for when the watermark advances N relative
      // seconds?
      switch (evictionTimerState) {
        case UNINITIALIZED:
          evictionTimerState = TimerState.SET;
          evictionTimer.set(timestamp.plus(evictionFrequency));
          break;
        case UNSET:
          evictionTimerState = TimerState.SET;
          evictionTimer.set(temporalBoundWatermark.plus(evictionFrequency));
          break;
        case SET: // do nothing
          break;
      }

      KV<K, KV<V1, V2>> e = c.element();
      K key = e.getKey();
      V1 left = e.getValue().getKey();
      V2 right = e.getValue().getValue();
      if (left != null) {
        TimestampedValue<V2> rightResult = findMatch(timestamp, left, rightState,  r -> compareFn.apply(KV.<V1, V2>of(left, r.getValue())));
        if (rightResult == null) {
          leftState.add(TimestampedValue.of(left, timestamp));
        } else {
          rightState.remove(rightResult);
          c.output(KV.of(key, KV.of(left, rightResult.getValue())));
        }
      } else {
        TimestampedValue<V1> leftResult = findMatch(timestamp, right, leftState,  l -> compareFn.apply(KV.of(l.getValue(), right)));
        if (leftResult == null) {
          rightState.add(TimestampedValue.of(right, timestamp));
        } else {
          leftState.remove(leftResult);
          c.output(KV.of(key, KV.of(leftResult.getValue(), right)));
        }
      }
    }

    @OnTimer("eviction")
    public void onEviction(@StateId("left") OrderedListState<V1> leftState, @StateId("right") OrderedListState<V2> rightState, @Timestamp Instant ts) {
      evictionTimerState = TimerState.UNSET;
      leftState.clearRange(new Instant(0L), ts);
      rightState.clearRange(new Instant(0L), ts);
    }
  }

  /**
   * Temporal inner join of two collections of KV elements.
   *
   * <p>A temporal join is like other joins but is able to operate on two unbounded streams in the
   * global window. A {@code temporalLimit} is used in the join predicate, along with the
   * {@code compareFn}, to satisfy the join condition. The {@code temporalLimit} bounds the amount
   * of state required to execute the join, allowing elements to be expired from the stateful
   * buffers.
   *
   * <p>TODO: tysonjh - do we need a duration for cleaning up the whole cache? Or is this derived
   * from an event-time-trigger update of the watermark?
   * <p>TODO: tysonjh - is the temporalLimit inclusive?
   *
   * @param <K> the join key type
   * @param <V1> the 'left' element type
   * @param <V2> the 'right' element type
   * @param name the name of this transform
   * @param leftCollection the left collection to be joined
   * @param rightCollection the right collection to be joined
   * @param temporalLimit the limit used when comparing elements, part of the join predicate
   * @param compareFn the function used for comparing elements, part of the join predicate
   */
  public static <K, V1, V2> PCollection<KV<K, KV<V1, V2>>> temporalInnerJoin(
      final String name,
      final PCollection<KV<K, V1>> leftCollection,
      final PCollection<KV<K, V2>> rightCollection,
      final Duration temporalLimit,
      final SimpleFunction<KV<V1, V2>, Boolean> compareFn) {
    return leftCollection
        .apply(name, TemporalInnerJoin.with(rightCollection, temporalLimit, compareFn))
        .setCoder(
            KvCoder.of(
                ((KvCoder<K, V1>) leftCollection.getCoder()).getKeyCoder(),
                KvCoder.of(
                    ((KvCoder<K, V1>) leftCollection.getCoder()).getValueCoder(),
                    ((KvCoder<K, V2>) rightCollection.getCoder()).getValueCoder())));
  }

  /**
   * Left Outer Join of two collections of KV elements.
   *
   * @param name Name of the PTransform.
   * @param leftCollection Left side collection to join.
   * @param rightCollection Right side collection to join.
   * @param nullValue Value to use as null value when right side do not match left side.
   * @param <K> Type of the key for both collections
   * @param <V1> Type of the values for the left collection.
   * @param <V2> Type of the values for the right collection.
   * @return A joined collection of KV where Key is the key and value is a KV where Key is of type
   *     V1 and Value is type V2. Values that should be null or empty is replaced with nullValue.
   */
  public static <K, V1, V2> PCollection<KV<K, KV<V1, V2>>> leftOuterJoin(
      final String name,
      final PCollection<KV<K, V1>> leftCollection,
      final PCollection<KV<K, V2>> rightCollection,
      final V2 nullValue) {
    return leftCollection.apply(name, LeftOuterJoin.with(rightCollection, nullValue));
  }

  public static <K, V1, V2> PCollection<KV<K, KV<V1, V2>>> leftOuterJoin(
      final PCollection<KV<K, V1>> leftCollection,
      final PCollection<KV<K, V2>> rightCollection,
      final V2 nullValue) {
    return leftOuterJoin("LeftOuterJoin", leftCollection, rightCollection, nullValue);
  }

  /**
   * Right Outer Join of two collections of KV elements.
   *
   * @param name Name of the PTransform.
   * @param leftCollection Left side collection to join.
   * @param rightCollection Right side collection to join.
   * @param nullValue Value to use as null value when left side do not match right side.
   * @param <K> Type of the key for both collections
   * @param <V1> Type of the values for the left collection.
   * @param <V2> Type of the values for the right collection.
   * @return A joined collection of KV where Key is the key and value is a KV where Key is of type
   *     V1 and Value is type V2. Values that should be null or empty is replaced with nullValue.
   */
  public static <K, V1, V2> PCollection<KV<K, KV<V1, V2>>> rightOuterJoin(
      final String name,
      final PCollection<KV<K, V1>> leftCollection,
      final PCollection<KV<K, V2>> rightCollection,
      final V1 nullValue) {
    return leftCollection.apply(name, RightOuterJoin.with(rightCollection, nullValue));
  }

  /**
   * Right Outer Join of two collections of KV elements.
   *
   * @param leftCollection Left side collection to join.
   * @param rightCollection Right side collection to join.
   * @param nullValue Value to use as null value when left side do not match right side.
   * @param <K> Type of the key for both collections
   * @param <V1> Type of the values for the left collection.
   * @param <V2> Type of the values for the right collection.
   * @return A joined collection of KV where Key is the key and value is a KV where Key is of type
   *     V1 and Value is type V2. Values that should be null or empty is replaced with nullValue.
   */
  public static <K, V1, V2> PCollection<KV<K, KV<V1, V2>>> rightOuterJoin(
      final PCollection<KV<K, V1>> leftCollection,
      final PCollection<KV<K, V2>> rightCollection,
      final V1 nullValue) {
    return rightOuterJoin("RightOuterJoin", leftCollection, rightCollection, nullValue);
  }

  /**
   * Full Outer Join of two collections of KV elements.
   *
   * @param name Name of the PTransform.
   * @param leftCollection Left side collection to join.
   * @param rightCollection Right side collection to join.
   * @param leftNullValue Value to use as null value when left side do not match right side.
   * @param rightNullValue Value to use as null value when right side do not match right side.
   * @param <K> Type of the key for both collections
   * @param <V1> Type of the values for the left collection.
   * @param <V2> Type of the values for the right collection.
   * @return A joined collection of KV where Key is the key and value is a KV where Key is of type
   *     V1 and Value is type V2. Values that should be null or empty is replaced with
   *     leftNullValue/rightNullValue.
   */
  public static <K, V1, V2> PCollection<KV<K, KV<V1, V2>>> fullOuterJoin(
      final String name,
      final PCollection<KV<K, V1>> leftCollection,
      final PCollection<KV<K, V2>> rightCollection,
      final V1 leftNullValue,
      final V2 rightNullValue) {
    return leftCollection.apply(
        name, FullOuterJoin.with(rightCollection, leftNullValue, rightNullValue));
  }

  /**
   * Full Outer Join of two collections of KV elements.
   *
   * @param leftCollection Left side collection to join.
   * @param rightCollection Right side collection to join.
   * @param leftNullValue Value to use as null value when left side do not match right side.
   * @param rightNullValue Value to use as null value when right side do not match right side.
   * @param <K> Type of the key for both collections
   * @param <V1> Type of the values for the left collection.
   * @param <V2> Type of the values for the right collection.
   * @return A joined collection of KV where Key is the key and value is a KV where Key is of type
   *     V1 and Value is type V2. Values that should be null or empty is replaced with
   *     leftNullValue/rightNullValue.
   */
  public static <K, V1, V2> PCollection<KV<K, KV<V1, V2>>> fullOuterJoin(
      final PCollection<KV<K, V1>> leftCollection,
      final PCollection<KV<K, V2>> rightCollection,
      final V1 leftNullValue,
      final V2 rightNullValue) {
    return fullOuterJoin(
        "FullOuterJoin", leftCollection, rightCollection, leftNullValue, rightNullValue);
  }
}
