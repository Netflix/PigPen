package pigpen.cascading;

import java.util.ArrayList;
import java.util.List;

import clojure.lang.IFn;
import clojure.lang.RT;
import org.apache.hadoop.io.BytesWritable;
import org.apache.pig.data.DataByteArray;

import cascading.tuple.TupleEntry;

public class OperationUtil {

  private static final IFn EVAL_STRING = RT.var("pigpen.pig", "eval-string");

  public static void init(String initCode) {
    EVAL_STRING.invoke(initCode);
  }

  public static IFn getFn(String funcCode) {
    return (IFn)EVAL_STRING.invoke(funcCode);
  }

  // TODO: this should not be necessary once we handle serialization without depending on Pig classes.
  public static Iterable getTupleValues(TupleEntry tupleEntry) {
    List objs = new ArrayList();
    for (Object o : tupleEntry.getTuple()) {
      objs.add(getValue(o));
    }
    return objs;
  }

  public static Object getValue(Object obj) {
    if (obj instanceof  BytesWritable) {
      BytesWritable bw = (BytesWritable)obj;
      return new DataByteArray(getBytes(bw));
    }
    return obj;
  }

  private static byte[] getBytes(BytesWritable bw) {
    if (bw.getCapacity() == bw.getLength()) {
      return bw.getBytes();
    } else {
      return copyBytes(bw);
    }
  }

  private static byte[] copyBytes(BytesWritable bw) {
    byte[] ret = new byte[bw.getLength()];
    System.arraycopy(bw.getBytes(), 0, ret, 0, bw.getLength());
    return ret;
  }
}
