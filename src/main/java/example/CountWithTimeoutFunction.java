package example;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Int;

import java.util.*;

public class CountWithTimeoutFunction extends KeyedProcessFunction<Tuple, Tuple4<String, String, Double, Long>, Tuple4<String, String, Double, String>> {
    /**
     * The state that is maintained by this process function
     */

    private static int MINUTE = 5;
    Logger LOG = LoggerFactory.getLogger(CountWithTimeoutFunction.class);

    private ValueState<CardTypeObject> state_card_type;
    private ValueState<Map<String, Double>> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state_card_type = getRuntimeContext().getState(new ValueStateDescriptor("typeState", CardTypeObject.class));
        state = getRuntimeContext().getState(new ValueStateDescriptor("sumState", Types.MAP(Types.STRING,Types.DOUBLE)));
    }

    @Override
    public void processElement(Tuple4<String, String, Double, Long> value, Context ctx, Collector<Tuple4<String, String, Double, String>> out) throws Exception {

        // retrieve the current count
        Map<String, Double> current = state.value();
        if (current == null) {
            current = new HashMap<String, Double>();
            current.put(value.f0, value.f2);
            CardTypeObject state_card_type_object = new CardTypeObject();
            state_card_type_object.CARD_TYPE = value.f1;
            state_card_type_object.firstModified = value.f3;
            state_card_type.update(state_card_type_object);
            ////////////////// TIMER SETTING
            // schedule the next timer 5 minutes from the current processing time
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + (60000 * MINUTE));
        }else {
            if (current.containsKey(value.f0)) {
                double total_amount = current.get(value.f0) + value.f2;
                current.remove(value.f0);
                current.put(value.f0, total_amount);
            }else{
                current.put(value.f0, value.f2);
            }
        }

        // write the state back
        state.update(current);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple4<String, String, Double, String>> out) throws Exception {

        // get the state for the key that scheduled the timer
        Map<String, Double> result = state.value();
        CardTypeObject cardTypeObject = state_card_type.value();
        result = sortByValue(result);

//        LOG.info("==================== TIMEOUT: " + result.key);
        // emit the state on timeout
        int count = 0;
        for(Map.Entry<String, Double> entry : result.entrySet()) {
            if(count == 5) break;
            out.collect(new Tuple4<String, String, Double, String>(entry.getKey(), cardTypeObject.CARD_TYPE, entry.getValue(), cardTypeObject.firstModified+"_"+(cardTypeObject.firstModified + 60000 * MINUTE)));
            count++;
        }
        state_card_type.clear();
        state.clear();
    }

    private static Map<String, Double> sortByValue(Map<String, Double> unsortMap) {

        // 1. Convert Map to List of Map
        List<Map.Entry<String, Double>> list =
                new LinkedList<Map.Entry<String, Double>>(unsortMap.entrySet());

        // 2. Sort list with Collections.sort(), provide a custom Comparator
        //    Try switch the o1 o2 position for a different order
        Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
            public int compare(Map.Entry<String, Double> o1,
                               Map.Entry<String, Double> o2) {
                int i = (o1.getValue()).compareTo(o2.getValue());
                if(i != 0) return -i;
                return (o1.getValue()).compareTo(o2.getValue());
            }
        });

        // 3. Loop the sorted list and put it into a new insertion order Map LinkedHashMap
        Map<String, Double> sortedMap = new LinkedHashMap<String, Double>();
        for (Map.Entry<String, Double> entry : list) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }
}
