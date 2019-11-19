package course.predictions;

import java.util.HashMap;
import java.util.Map;


/**
 * Simple Prediction with a learning Rate
 * X_old + (X_old - X_new) * learningrate
 */
public class SimplePrediction {

    private double values[];
    private static Map<String, Integer> tags;
    private static int NUM_TERMS_BUCKETS = 5;
    private double learningrate;


    public SimplePrediction(double learningrate) {
        this.learningrate = learningrate;
        values = new double[NUM_TERMS_BUCKETS];
        for (int i = 0; i < NUM_TERMS_BUCKETS; i++) {
            values[i] = 0d;
        }
        tags = new HashMap<String, Integer>() {{
            put("Google", 0);
            put("Facebook", 1);
            put("Apple", 2);
            put("Intel", 3);
            put("Huawei", 4);
        }};
    }

    public int predict(double newValue, String tag){
        double prediction = this.values[tags.get(tag)];
        if (Double.isNaN(prediction)) {
            return -1;
        }
        else {
            return (int) prediction;
        }
    }

    public void refineModel(double newValue, String tag){
        double x = newValue - this.values[tags.get(tag)];
        this.values[tags.get(tag)] = x * this.learningrate + this.values[tags.get(tag)];
    }


}
