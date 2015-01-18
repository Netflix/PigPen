package pigpen.cascading;

import java.util.List;

import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Var;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class PigPenAggregateBy extends AggregateBy {

  private static final Var GET_SEED_VALUE_FN = RT.var("pigpen.cascading.runtime", "get-seed-value");

  private static class Partial implements Functor {

    private final String init;
    private final String func;
    private final String argField;
    private transient IFn fn;
    private static final Var COMPUTE_PARTIAL_FN = RT.var("pigpen.cascading.runtime", "compute-partial-mapper");

    private Partial(String init, String func, String argField) {
      this.init = init;
      this.func = func;
      this.argField = argField;
    }

    @Override
    public Fields getDeclaredFields() {
      return getFields(argField);
    }

    @Override
    public Tuple aggregate(FlowProcess flowProcess, TupleEntry args, Tuple context) {
      if (fn == null) {
        OperationUtil.init(init);
        fn = OperationUtil.getFn(func);
      }
      if (context == null) {
        context = new Tuple(GET_SEED_VALUE_FN.invoke(fn));
      }
      Object val = args.getObject(argField);
      return OperationUtil.SENTINEL_VALUE.equals(val) ?
          context :
          (Tuple)COMPUTE_PARTIAL_FN.invoke(fn, OperationUtil.deserialize(val), context.getObject(0));
    }

    @Override
    public Tuple complete(FlowProcess flowProcess, Tuple context) {
      return context;
    }
  }

  public static class Final extends BaseOperation<Final.Context> implements Aggregator<Final.Context> {

    private static final Var COMPUTE_PARTIAL_FN = RT.var("pigpen.cascading.runtime", "compute-partial-reducer");
    private final String init;
    private final String func;
    private final String argField;

    public Final(String init, String func, String argField) {
      super(getFields(argField));
      this.init = init;
      this.func = func;
      this.argField = argField;
    }

    protected static class Context {
      private final IFn fn;
      private Object acc;

      public Context(IFn fn) {
        this.fn = fn;
      }

      public void reset() {
        acc = GET_SEED_VALUE_FN.invoke(fn);
      }
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall<Context> operationCall) {
      super.prepare(flowProcess, operationCall);
      OperationUtil.init(init);
      IFn fn = OperationUtil.getFn(func);
      operationCall.setContext(new Context(fn));
    }

    @Override
    public void start(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {
      aggregatorCall.getContext().reset();
    }

    @Override
    public void aggregate(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {
      Context context = aggregatorCall.getContext();
      TupleEntry args = aggregatorCall.getArguments();
      Object val = args.getObject(getFields(argField));
      if (!OperationUtil.SENTINEL_VALUE.equals(val)) {
        Object newAcc = COMPUTE_PARTIAL_FN.invoke(context.fn, OperationUtil.deserialize(val), context.acc);
        context.acc = newAcc;
      }
    }

    @Override
    public void complete(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {
      aggregatorCall.getOutputCollector().add(new Tuple(aggregatorCall.getContext().acc));
    }
  }

  private static Fields getFields(String fieldName) {
    return new Fields("agg_result" + fieldName);
  }

  private PigPenAggregateBy(String name, String init, String func) {
    super(new Fields(name), new Partial(init, func, name), new Final(init, func, name));
  }

  public static AggregateBy buildAssembly(String name, Pipe[] pipes, Fields keyField, List<String> inits, List<String> funcs) {
    AggregateBy[] assemblies = new AggregateBy[pipes.length];
    for (int i = 0; i < pipes.length; i++) {
      assemblies[i] = new PigPenAggregateBy(pipes[i].getName(), inits.get(i), funcs.get(i));
    }
    return new AggregateBy(name, pipes, keyField, assemblies);
  }
}
