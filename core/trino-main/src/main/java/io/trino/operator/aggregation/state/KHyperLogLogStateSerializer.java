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

import io.airlift.stats.cardinality.HyperLogLog;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;

import static io.trino.spi.type.KHyperLogLogType.KHYPER_LOG_LOG;

public class KHyperLogLogStateSerializer
        implements AccumulatorStateSerializer<KHyperLogLogState>
{
    @Override
    public Type getSerializedType()
    {
        return KHYPER_LOG_LOG;
    }

    @Override
    public void serialize(KHyperLogLogState state, BlockBuilder out)
    {
        if (state.getKHyperLogLog() == null) {
            out.appendNull();
        }
        else {
            KHYPER_LOG_LOG.writeSlice(out, state.getKHyperLogLog().serialize());
        }
    }

    @Override
    public void deserialize(Block block, int index, KHyperLogLogState state)
    {
        state.setKHyperLogLog(KHyperLogLog.newInstance(KHYPER_LOG_LOG.getSlice(block, index)));
    }
}
