package pigpen.cascading;

import org.apache.hadoop.io.BytesWritable;

import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;

public class OperationUtil {

    public static IFn getVar(final String name) {
        final Var require = RT.var("clojure.core", "require");
        require.invoke(Symbol.intern("pigpen.runtime"));
        require.invoke(Symbol.intern("pigpen.cascading.runtime"));
        return RT.var("pigpen.cascading.runtime", name);
    }

    public static byte[] getBytes(final BytesWritable bw) {
        if (bw.getCapacity() == bw.getLength()) {
            return bw.getBytes();
        } else {
            return copyBytes(bw);
        }
    }

    public static byte[] copyBytes(final BytesWritable bw) {
        final byte[] ret = new byte[bw.getLength()];
        System.arraycopy(bw.getBytes(), 0, ret, 0, bw.getLength());
        return ret;
    }
}
