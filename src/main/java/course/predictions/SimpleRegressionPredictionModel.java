package course.predictions;

import org.apache.commons.math3.stat.regression.SimpleRegression;


import java.util.HashMap;
import java.util.Map;


public class SimpleRegressionPredictionModel {

	private static Map<String, Integer> tags;
	private static int NUM_TERMS_BUCKETS = 5;

	public SimpleRegression models[];

	public SimpleRegressionPredictionModel() {
		models = new SimpleRegression[NUM_TERMS_BUCKETS];
		for (int i = 0; i < NUM_TERMS_BUCKETS; i++) {
			models[i] = new SimpleRegression(false);
		}
		tags = new HashMap<String, Integer>() {{
			put("Google", 0);
			put("Facebook", 1);
			put("Apple", 2);
			put("Intel", 3);
			put("Huawei", 4);
		}};
	}

	/**
	 * Predicts Data with Simple Regression
	 *
	 * @param input
	 * @param tag
	 * @return prediction
	 */
	public int predict(int input, String tag) {
		double prediction = models[tags.get(tag)].predict((double) input);
		if (Double.isNaN(prediction)) {
			return -1;
		}
		else {
			return (int)prediction;
		}
	}

	/**
	 * update Model
	 *
	 * @param input
	 * @param valueToPredict
	 * @param tag
	 */
	public void refineModel(int input, int valueToPredict, String tag) {
		models[tags.get(tag)].addData(input, valueToPredict);
	}
}
