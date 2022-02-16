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
package io.trino.operator.aggregation.state;

import io.trino.array.ObjectBigArray;
import io.trino.spi.function.AccumulatorStateFactory;
import org.openjdk.jol.info.ClassLayout;

import static java.util.Objects.requireNonNull;

public class KHyperLogLogStateFactory
        implements AccumulatorStateFactory<KHyperLogLogState>
{
    @Override
    public KHyperLogLogState createSingleState()
    {
        return new SingleKHyperLogLogState();
    }

    @Override
    public KHyperLogLogState createGroupedState()
    {
        return new GroupedKHyperLogLogState();
    }

    public static class GroupedKHyperLogLogState
            extends AbstractGroupedAccumulatorState
            implements KHyperLogLogState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedKHyperLogLogState.class).instanceSize();
        private final ObjectBigArray<KHyperLogLog> hlls = new ObjectBigArray<>();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            hlls.ensureCapacity(size);
        }

        @Override
        public KHyperLogLog getKHyperLogLog()
        {
            return hlls.get(getGroupId());
        }

        @Override
        public void setKHyperLogLog(KHyperLogLog value)
        {
            requireNonNull(value, "value is null");
            hlls.set(getGroupId(), value);
        }

        @Override
        public void addMemoryUsage(int value)
        {
            size += value;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + hlls.sizeOf();
        }
    }

    public static class SingleKHyperLogLogState
            implements KHyperLogLogState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleKHyperLogLogState.class).instanceSize();
        private KHyperLogLog hll;

        @Override
        public KHyperLogLog getKHyperLogLog()
        {
            return hll;
        }

        @Override
        public void setKHyperLogLog(KHyperLogLog value)
        {
            hll = value;
        }

        @Override
        public void addMemoryUsage(int value)
        {
            // noop
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            if (hll != null) {
                estimatedSize += hll.estimatedInMemorySize();
            }
            return estimatedSize;
        }
    }
}
