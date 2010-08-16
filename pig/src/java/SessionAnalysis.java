import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

public class SessionAnalysis extends EvalFunc<Integer> {

    static final long sessionTimeout = 600;

    @Override
    public Integer exec(Tuple input) throws IOException {
        // We'll actually get a bag of strings and bag of longs
        DataBag actions = (DataBag)input.get(0);
        DataBag timestamps = (DataBag)input.get(1);
        
        // if we were handed an empty bag, return NULL
        if(actions.size() == 0 || timestamps.size() == 0) {
            return null;
        }

        // Open both bags and walk through them together.  We want to count how
        // many steps it took us to get to a purchase.  If we never see a
        // purchase, we will return null.  We only count two actions as in the
        // same session if they are 600 seconds or less apart in time.  We assume
        // the input is sorted by timestamp.
        Iterator<Tuple> aIter = actions.iterator();
        Iterator<Tuple> tIter = timestamps.iterator();

        long lastTimestamp = 0;
        int countedSteps = 0;
        while (aIter.hasNext() && tIter.hasNext()) {
            String action = (String)aIter.next().get(0);
            Long timestamp = (Long)tIter.next().get(0);

            if (timestamp > lastTimestamp + sessionTimeout) {
                countedSteps = 0; // reset
            }
            lastTimestamp = timestamp;
            if (action.equals("purchase")) return countedSteps;
            else countedSteps++;
        }

        // If we got here we never saw a purchase
        return null;
    }

}

