package pigpen.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import clojure.lang.IFn;

public class PigPenAggregateBy extends AggregateBy {

    public PigPenAggregateBy(final String context, final Pipe pipe, final Fields groupingFields, final Fields argFields) {
        super(null, new Pipe[] { pipe }, groupingFields, argFields, new Partial(context, argFields), new Final(context, argFields), 0);
    }

    private static class Partial implements Functor {

        private static final IFn PREPARE = OperationUtil.getVar("prepare");
        private static final IFn AGGREGATE = OperationUtil.getVar("aggregate-partial-aggregate");
        private static final IFn COMPLETE = OperationUtil.getVar("aggregate-partial-complete");

        private final String context;
        private final Fields fields;

        public Partial(final String context, final Fields fields) {
            this.context = context;
            this.fields = fields;
        }

        @Override
        public Fields getDeclaredFields() {
            return this.fields;
        }

        @Override
        public Tuple aggregate(final FlowProcess flowProcess, final TupleEntry args, final Tuple agg) {
            return new Tuple(AGGREGATE.invoke(PREPARE.invoke(this.context), args, agg));
        }

        @Override
        public Tuple complete(final FlowProcess flowProcess, final Tuple agg) {
            return (Tuple) COMPLETE.invoke(agg);
        }
    }

    public static class Final extends BaseOperation implements Aggregator {

        private static final IFn PREPARE = OperationUtil.getVar("prepare");
        private static final IFn START = OperationUtil.getVar("aggregate-final-start");
        private static final IFn AGGREGATE = OperationUtil.getVar("aggregate-final-aggregate");
        private static final IFn COMPLETE = OperationUtil.getVar("aggregate-final-complete");

        private final String context;

        public Final(final String context, final Fields fields) {
            super(fields);
            this.context = context;
        }

        @Override
        public void start(final FlowProcess flowProcess, final AggregatorCall aggregatorCall) {
            aggregatorCall.setContext(START.invoke(PREPARE.invoke(this.context), aggregatorCall));
        }

        @Override
        public void aggregate(final FlowProcess flowProcess, final AggregatorCall aggregatorCall) {
            aggregatorCall.setContext(AGGREGATE.invoke(PREPARE.invoke(this.context), aggregatorCall));
        }

        @Override
        public void complete(final FlowProcess flowProcess, final AggregatorCall aggregatorCall) {
            COMPLETE.invoke(PREPARE.invoke(this.context), aggregatorCall);
        }
    }
}
