/*
 *
 *  Copyright 2014-2015 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package pigpen;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.util.UDFContext;

import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;

/**
 * The base class for partitioners in PigPen.
 *
 * @author mbossenbroek
 *
 */
public abstract class PigPenPartitioner extends Partitioner<PigNullableWritable, Writable> {

    private static final IFn EVAL_STRING, GET_PARTITION;

    static {
        final Var require = RT.var("clojure.core", "require");
        require.invoke(Symbol.intern("pigpen.pig.runtime"));
        EVAL_STRING = RT.var("pigpen.pig.runtime", "eval-string");
        GET_PARTITION = RT.var("pigpen.pig.runtime", "get-partition");
    }

    private final String type;
    private final Object func;

    public PigPenPartitioner() {
        final Configuration jobConf = UDFContext.getUDFContext().getJobConf();
        type = jobConf.get(getClass().getSimpleName() + "_type");
        final String init = jobConf.get(getClass().getSimpleName() + "_init");
        final String funcString = jobConf.get(getClass().getSimpleName() + "_func");

        EVAL_STRING.invoke(init);
        this.func = EVAL_STRING.invoke(funcString);
    }

    @Override
    public int getPartition(final PigNullableWritable key, final Writable value, final int numPartitions) {
        return (Integer) GET_PARTITION.invoke(type, func, key.getValueAsPigType(), numPartitions);
    }

    // Hadoop doesn't allow for configuration of partitioners, so we make a lot of them
    public static class PigPenPartitioner0 extends PigPenPartitioner {}
    public static class PigPenPartitioner1 extends PigPenPartitioner {}
    public static class PigPenPartitioner2 extends PigPenPartitioner {}
    public static class PigPenPartitioner3 extends PigPenPartitioner {}
    public static class PigPenPartitioner4 extends PigPenPartitioner {}
    public static class PigPenPartitioner5 extends PigPenPartitioner {}
    public static class PigPenPartitioner6 extends PigPenPartitioner {}
    public static class PigPenPartitioner7 extends PigPenPartitioner {}
    public static class PigPenPartitioner8 extends PigPenPartitioner {}
    public static class PigPenPartitioner9 extends PigPenPartitioner {}
    public static class PigPenPartitioner10 extends PigPenPartitioner {}
    public static class PigPenPartitioner11 extends PigPenPartitioner {}
    public static class PigPenPartitioner12 extends PigPenPartitioner {}
    public static class PigPenPartitioner13 extends PigPenPartitioner {}
    public static class PigPenPartitioner14 extends PigPenPartitioner {}
    public static class PigPenPartitioner15 extends PigPenPartitioner {}
    public static class PigPenPartitioner16 extends PigPenPartitioner {}
    public static class PigPenPartitioner17 extends PigPenPartitioner {}
    public static class PigPenPartitioner18 extends PigPenPartitioner {}
    public static class PigPenPartitioner19 extends PigPenPartitioner {}
    public static class PigPenPartitioner20 extends PigPenPartitioner {}
    public static class PigPenPartitioner21 extends PigPenPartitioner {}
    public static class PigPenPartitioner22 extends PigPenPartitioner {}
    public static class PigPenPartitioner23 extends PigPenPartitioner {}
    public static class PigPenPartitioner24 extends PigPenPartitioner {}
    public static class PigPenPartitioner25 extends PigPenPartitioner {}
    public static class PigPenPartitioner26 extends PigPenPartitioner {}
    public static class PigPenPartitioner27 extends PigPenPartitioner {}
    public static class PigPenPartitioner28 extends PigPenPartitioner {}
    public static class PigPenPartitioner29 extends PigPenPartitioner {}
    public static class PigPenPartitioner30 extends PigPenPartitioner {}
    public static class PigPenPartitioner31 extends PigPenPartitioner {}
}
