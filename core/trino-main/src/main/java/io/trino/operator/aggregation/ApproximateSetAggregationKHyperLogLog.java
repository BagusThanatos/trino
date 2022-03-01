/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator.aggregation;

import io.airlift.slice.Slice;
import io.trino.operator.aggregation.state.KHyperLogLog;
import io.trino.operator.aggregation.state.KHyperLogLogState;
import io.trino.operator.aggregation.state.StateCompiler;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

@AggregationFunction("khyperloglog_agg")
public final class ApproximateSetAggregationKHyperLogLog
{
    private static final int NUMBER_OF_BUCKETS = 4096;
    private static final AccumulatorStateSerializer<KHyperLogLogState> SERIALIZER = StateCompiler.generateStateSerializer(KHyperLogLogState.class);

    private ApproximateSetAggregationKHyperLogLog() {}

    public static KHyperLogLog newKHyperLogLog()
    {
        return KHyperLogLog.newInstance(NUMBER_OF_BUCKETS);
    }

    @InputFunction
    public static void input(@AggregationState KHyperLogLogState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.DOUBLE) double value2)
    {
        KHyperLogLog hll = getOrCreateKHyperLogLog(state);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(Double.doubleToLongBits(value));
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    @InputFunction
    public static void input(@AggregationState KHyperLogLogState state, @SqlType(StandardTypes.VARCHAR) Slice value, @SqlType(StandardTypes.DOUBLE) double value2)
    {
        KHyperLogLog hll = getOrCreateKHyperLogLog(state);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(value);
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    @InputFunction
    public static void input(@AggregationState KHyperLogLogState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.VARCHAR) Slice value2)
    {
        KHyperLogLog hll = getOrCreateKHyperLogLog(state);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(Double.doubleToLongBits(value));
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    @InputFunction
    public static void input(@AggregationState KHyperLogLogState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.VARCHAR) Slice value2)
    {
        KHyperLogLog hll = getOrCreateKHyperLogLog(state);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(value);
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    @InputFunction
    public static void input(@AggregationState KHyperLogLogState state, @SqlType(StandardTypes.VARCHAR) Slice value, @SqlType(StandardTypes.BIGINT) long value2)
    {
        KHyperLogLog hll = getOrCreateKHyperLogLog(state);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(value);
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    @InputFunction
    @LiteralParameters({"x", "y"})
    public static void input(@AggregationState KHyperLogLogState state, @SqlType("varchar(x)") Slice value, @SqlType("varchar(y)") Slice valueY)
    {
        KHyperLogLog hll = getOrCreateKHyperLogLog(state);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(value);
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    @InputFunction
    public static void input(@AggregationState KHyperLogLogState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.BIGINT) long value2)
    {
        KHyperLogLog hll = getOrCreateKHyperLogLog(state);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(value);
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    @InputFunction
    public static void input(@AggregationState KHyperLogLogState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.BIGINT) long value2)
    {
        KHyperLogLog hll = getOrCreateKHyperLogLog(state);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(Double.doubleToLongBits(value));
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    private static KHyperLogLog getOrCreateKHyperLogLog(@AggregationState KHyperLogLogState state)
    {
        KHyperLogLog hll = state.getKHyperLogLog();
        if (hll == null) {
            hll = newKHyperLogLog();
            state.setKHyperLogLog(hll);
            state.addMemoryUsage(hll.estimatedInMemorySize());
        }
        return hll;
    }

    @CombineFunction
    public static void combineState(@AggregationState KHyperLogLogState state, @AggregationState KHyperLogLogState otherState)
    {
        KHyperLogLog input = otherState.getKHyperLogLog();

        KHyperLogLog previous = state.getKHyperLogLog();
        if (previous == null) {
            state.setKHyperLogLog(input);
            state.addMemoryUsage(input.estimatedInMemorySize());
        }
        else {
            state.addMemoryUsage(-previous.estimatedInMemorySize());
            previous.mergeWith(input);
            state.addMemoryUsage(previous.estimatedInMemorySize());
        }
    }

    @OutputFunction(StandardTypes.KHYPER_LOG_LOG)
    public static void evaluateFinal(@AggregationState KHyperLogLogState state, BlockBuilder out)
    {
        SERIALIZER.serialize(state, out);
    }
}
