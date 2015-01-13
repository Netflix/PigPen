package pigpen.cascading;

import clojure.lang.IFn;
import clojure.lang.LazySeq;
import clojure.lang.PersistentVector;
import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;
import org.apache.hadoop.io.BytesWritable;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

public class OperationUtil {

  private static final IFn EVAL_STRING = RT.var("pigpen.runtime", "eval-string");

  public static void init(String initCode) {
    Var require = RT.var("clojure.core", "require");
    require.invoke(Symbol.intern("pigpen.runtime"));
    require.invoke(Symbol.intern("pigpen.cascading.runtime"));
    require.invoke(Symbol.intern("pigpen.extensions.core"));
    EVAL_STRING.invoke(initCode);
  }

  public static IFn getFn(String funcCode) {
    return (IFn)EVAL_STRING.invoke(funcCode);
  }

  public static byte[] getBytes(BytesWritable bw) {
    if (bw.getCapacity() == bw.getLength()) {
      return bw.getBytes();
    } else {
      return copyBytes(bw);
    }
  }

  public static byte[] copyBytes(BytesWritable bw) {
    byte[] ret = new byte[bw.getLength()];
    System.arraycopy(bw.getBytes(), 0, ret, 0, bw.getLength());
    return ret;
  }
}
