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

import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.Murmur3Hash128;
import io.airlift.slice.Slice;
import io.airlift.stats.cardinality.HyperLogLog;
import org.openjdk.jol.info.ClassLayout;

//import static com.google.common.base.Preconditions.checkArgument;
//import static io.airlift.stats.cardinality.Utils.indexBitLength;

public class KHyperLogLog
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(KHyperLogLog.class).instanceSize();
    private static final int MAX_NUMBER_OF_BUCKETS = 65536;
//    private HllInstance instance;

//    private KHyperLogLog(HllInstance instance)
//    {
//        this.instance = instance;
//    }
    private HyperLogLog holder;

    private KHyperLogLog(HyperLogLog holder)
    {
        this.holder = holder;
    }

    public static KHyperLogLog newInstance(int numberOfBuckets)
    {
//        checkArgument(numberOfBuckets <= MAX_NUMBER_OF_BUCKETS, "numberOfBuckets must be <= %s, actual: %s", MAX_NUMBER_OF_BUCKETS, numberOfBuckets);

        return new KHyperLogLog(HyperLogLog.newInstance(numberOfBuckets));
    }

    public static KHyperLogLog newInstance(Slice serialized)
    {
//        checkArgument(serialized.getByte(0) != Format.SPARSE_V1.getTag(), "Sparse v1 encoding no longer supported");
//
//        if (SparseHll.canDeserialize(serialized)) {
//            return new HyperLogLog(new SparseHll(serialized));
//        }
//        else if (DenseHll.canDeserialize(serialized)) {
//            return new HyperLogLog(new DenseHll(serialized));
//        }
//
//        throw new IllegalArgumentException("Cannot deserialize HyperLogLog");
        return new KHyperLogLog(HyperLogLog.newInstance(serialized));
    }

    public void add(long value)
    {
        addHash(Murmur3Hash128.hash64(value));
    }

    public void add(Slice value)
    {
        addHash(Murmur3Hash128.hash64(value));
    }

    /**
     * Adds a value that has already been hashed to the set of values tracked by this HyperLogLog instance.
     *
     * @param hash The hash should be the 64 least significant bits of the murmur3_128 hash of the value.
     * For example: io.airlift.slice.Murmur3.hash64(value).
     */
    public void addHash(long hash)
    {
//        instance.insertHash(hash);
//
//        if (instance instanceof SparseHll) {
//            instance = makeDenseIfNecessary((SparseHll) instance);
//        }
        this.holder.add(hash);
    }

    public void mergeWith(KHyperLogLog other)
    {
//        if (instance instanceof SparseHll && other.instance instanceof SparseHll) {
//            ((SparseHll) instance).mergeWith((SparseHll) other.instance);
//            instance = makeDenseIfNecessary((SparseHll) instance);
//        }
//        else if (instance instanceof DenseHll && other.instance instanceof SparseHll) {
//            ((DenseHll) instance).mergeWith((SparseHll) other.instance);
//        }
//        else {
//            DenseHll dense = instance.toDense();
//            dense.mergeWith(other.instance.toDense());
//
//            instance = dense;
//        }
        this.holder.mergeWith(other.holder);
    }

    public long cardinality()
    {
        return this.holder.cardinality();
    }

    public int estimatedInMemorySize()
    {
        return this.holder.estimatedInMemorySize() + INSTANCE_SIZE;
    }

    public int estimatedSerializedSize()
    {
        return this.holder.estimatedSerializedSize();
    }

    public Slice serialize()
    {
        return this.holder.serialize();
    }

    public void makeDense()
    {
//        instance = instance.toDense();
        this.holder.makeDense();
    }

    @VisibleForTesting
    void verify()
    {
//        this.holder.verify();
    }
//
//    private static HllInstance makeDenseIfNecessary(SparseHll instance)
//    {
//        if (instance.estimatedInMemorySize() > DenseHll.estimatedInMemorySize(instance.getIndexBitLength())) {
//            return instance.toDense();
//        }
//
//        return instance;
//    }
}
