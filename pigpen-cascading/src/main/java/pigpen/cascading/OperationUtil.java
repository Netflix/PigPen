package pigpen.cascading;

import java.util.ArrayList;
import java.util.List;

import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;
import org.apache.hadoop.io.BytesWritable;

import cascading.tuple.Tuple;

public class OperationUtil {

  public static final String SENTINEL_VALUE = "39c63d213f5cba3c7";
  private final static Var SERIALIZER_FN = RT.var("pigpen.cascading.runtime", "cs-freeze");
  private final static Var DESERIALIZER_FN = RT.var("pigpen.cascading.runtime", "hybrid->clojure");
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

  public static Object deserialize(Object raw) {
    return DESERIALIZER_FN.invoke(raw);
  }

  public static BytesWritable serialize(Object obj) {
    return (BytesWritable)SERIALIZER_FN.invoke(obj);
  }

  public static List deserialize(Tuple tuple) {
    List list = new ArrayList(tuple.size());
    for (int i = 0; i < tuple.size(); i++) {
      list.add(OperationUtil.deserialize(tuple.getObject(i)));
    }
    return list;
  }
}
