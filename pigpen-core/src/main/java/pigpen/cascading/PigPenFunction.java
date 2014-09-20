package pigpen.cascading;

import java.util.ArrayList;
import java.util.List;

import clojure.lang.IFn;
import clojure.lang.RT;
import org.apache.hadoop.io.BytesWritable;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class PigPenFunction extends BaseOperation implements Function {

  private final String init;
  private final String func;

  public PigPenFunction(String init, String func, Fields fields) {
    super(fields);
    this.init = init;
    this.func = func;
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
    super.prepare(flowProcess, operationCall);
    OperationUtil.init(init);
    IFn fn = OperationUtil.getFn(func);
    operationCall.setContext(fn);
  }

  @Override
  public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
    IFn fn = (IFn)functionCall.getContext();
    TupleEntry tupleEntry = functionCall.getArguments();
    Object result = fn.invoke(OperationUtil.getTupleValues(tupleEntry));
    emitOutput(functionCall, result);
  }

  // TODO: this should not use DataBag or any other Pig class.
  private void emitOutput(FunctionCall functionCall, Object result) {
    for (org.apache.pig.data.Tuple tuple : (DataBag)result) {
      try {
        Object[] objs = new Object[tuple.size()];
        for (int i = 0; i < tuple.size(); i++) {
          Object obj = tuple.get(i);
          if (obj instanceof DataByteArray) {
            objs[i] = new BytesWritable(((DataByteArray) obj).get());
          } else {
            objs[i] = tuple.get(i);
          }
        }
        functionCall.getOutputCollector().add(new Tuple(objs));
      } catch (ExecException e) {
        throw new RuntimeException();
      }
    }
  }
}
