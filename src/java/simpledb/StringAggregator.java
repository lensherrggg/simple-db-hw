package simpledb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /* It seems that String Aggregator is the same as Integer Aggregator */

    /* group by field index */
    private int gbField;
    /* group by field type */
    private Type gbFieldType;
    /* aggregate field index */
    private int aField;
    /* aggregation handler */
    private AggregateHandler handler;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // Done
        gbField = gbfield;
        gbFieldType = gbfieldtype;
        aField = afield;
        switch (what) {
            case COUNT: handler = new CountHandler(); break;
            default:
                throw new UnsupportedOperationException("Unsupported aggregation operator");
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // Done
        if (null != gbFieldType && (!tup.getField(gbField).getType().equals(gbFieldType))) {
            throw new IllegalArgumentException("Given tuple has wrong type");
        }
        Field key = gbField == NO_GROUPING ? DUMMY_FIELD : tup.getField(gbField);
        handler.handle(key, tup.getField(aField));
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // Done
        Map<Field, Integer> results = handler.getResults();
        Type[] types;
        String[] names;
        TupleDesc td;
        List<Tuple> tuples = new ArrayList<>();
        if (gbField == NO_GROUPING) {
            types = new Type[]{Type.INT_TYPE};
            names = new String[]{"aggregateVal"};
            td = new TupleDesc(types, names);
            for (Integer val : results.values()) {
                Tuple tuple = new Tuple(td);
                tuple.setField(0, new IntField(val));
                tuples.add(tuple);
            }
        } else {
            types = new Type[]{gbFieldType, Type.INT_TYPE};
            names = new String[]{"groupVal", "aggregateVal"};
            td = new TupleDesc(types, names);
            for (Map.Entry<Field, Integer> e : results.entrySet()) {
                Tuple tuple = new Tuple(td);
                tuple.setField(0, e.getKey());
                tuple.setField(1, new IntField(e.getValue()));
                tuples.add(tuple);
            }
        }
        return new TupleIterator(td, tuples);
    }

    private abstract class AggregateHandler {
        public ConcurrentHashMap<Field, Integer> results;
        public abstract void handle(Field key, Field field);

        public AggregateHandler() {
            results = new ConcurrentHashMap<>();
        }

        public Map<Field, Integer> getResults() {
            return results;
        }
    }

    private class CountHandler extends AggregateHandler {
        @Override
        public void handle(Field key, Field field) {
            results.put(key, results.getOrDefault(key, 0) + 1);
        }
    }
}
