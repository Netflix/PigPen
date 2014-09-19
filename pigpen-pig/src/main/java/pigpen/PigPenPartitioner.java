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
        require.invoke(Symbol.intern("pigpen.runtime"));
        require.invoke(Symbol.intern("pigpen.pig"));
        EVAL_STRING = RT.var("pigpen.runtime", "eval-string");
        GET_PARTITION = RT.var("pigpen.pig", "get-partition");
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
}
