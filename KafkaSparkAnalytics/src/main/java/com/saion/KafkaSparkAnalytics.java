
package com.saion;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.mllib.feature.StandardScaler;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import scala.Array;
import scala.Function1;
import scala.Some;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.parallel.ParIterableLike.Foreach;
import scala.math.Ordering;
import scala.reflect.ClassTag;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.mllib.feature.StandardScalerModel;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.optimization.LBFGS;
import org.apache.spark.mllib.optimization.LeastSquaresGradient;
import org.apache.spark.mllib.optimization.SimpleUpdater;
import org.apache.spark.mllib.optimization.SquaredL2Updater;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.rdd.RDD;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount. Usage:
 * KafkaWordCount <brokers> <topics> <brokers> is a list of one or more Kafka
 * brokers <topics> is a list of one or more kafka topics to consume from
 *
 * Example: $ bin/run-example streaming.JavaDirectKafkaWordCount
 * broker1-host:port,broker2-host:port \ topic1,topic2
 */

public final class KafkaSparkAnalytics {
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws Exception {
		if (args.length < 8) {
			System.err.println("Usage: KafkaSparkAnalytics <brokers> <topics>\n"
					+ "  <brokers> is a list of one or more Kafka brokers\n"
					+ "  <topics> is a list of one or more kafka topics to consume from\n"
					+ "  <numIter> is the number of iterations for LinearRegression\n"
					+ "  <stepSize> is the stepSize for LinearRegression\n"
					+ "  <convergenceTol> is the convergence tolerance for LinearRegression\n"
					+ "  <initWeight> is the initial weight vector for LinearRegression\n"
					+ "  <regParam> is the regularization parameter for LinearRegression\n"
					+ "  <numCorrections> is the number of corrections for LBFGS Linear Reg\n\n");
			System.exit(1);
		}
 
		// StreamingExamples.setStreamingLogLevels();

		String brokers = args[0];
		String topics = args[1];
		String numIterationsArgs = args[2];
		String stepSizeArgs = args[3];
		String convergenceTolArgs = args[4];
		String initWeightArgs = args[5];
		String regParamArgs = args[6];
		String numCorrectionsArgs = args[7];

		// Create context with a 10 seconds batch interval
		SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaSpark");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(10));

		Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", brokers);

		// Initialise Hive Query variables
		System.out.println("INITIALIZING HIVE VARS...................");

		@SuppressWarnings("deprecation")
		HiveContext hc = new HiveContext(jsc);

		//Loading DataFrames
		DataFrame reviews_orc = hc.read().format("orc").load("/apps/hive/warehouse/restaurants.db/reviews");
		DataFrame restaurants_orc = hc.read().format("orc").load("/apps/hive/warehouse/restaurants.db/restaurants");

		reviews_orc.registerTempTable("reviews");

		restaurants_orc.registerTempTable("restaurants");

		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

		// Get the lines, split them into words and print
		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
			@Override
			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		});

		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String x) {
				return Arrays.asList(SPACE.split(x));
			}
		});

		words.print();

		//Defining Kafka Broker Params for Publishing
		HashMap<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

		//ForeachRDD
		words.foreachRDD(record -> {
			if (!record.isEmpty()) { //Filtering out empty rdd's
				//System.out.println("Input found!!! " + record.collect());
				//System.out.println("First: " + record.first());


				/* Needed in case of Feature Scaling
				 StandardScaler scaler = new StandardScaler();

				StandardScalerModel scalerModel = scaler.fit(JavaRDD.toRDD(parseddata.map(x -> x.features())));
				// JavaRDD<Vector> scaledFeatures =
				// scalerModel.transform(JavaRDD.toRDD(parseddata.map(x ->
				// x.features())))
				// .toJavaRDD();
				
				
				// Recreating scaledData with scaled values
				JavaRDD<LabeledPoint> scaledData = parseddata.map(new Function<LabeledPoint, LabeledPoint>() {
					public LabeledPoint call(LabeledPoint point) {
						return new LabeledPoint(point.label(), scalerModel.transform(point.features()));
					}
				});

				System.out.println("Scaled data: ");
				System.out.println(scaledData.collect());
				*/
				//scaledData.cache();

				//For analyzing statistics on incoming data
				/*
				JavaRDD<Vector> LatData = record.map(new Function<String, Vector>() {

					@Override
					public Vector call(String lat) throws Exception {
						String[] parts = lat.split(",");
						//String pointsStr = parts[1].trim();
						double[] points = new double[1];
						
						//for (int i = 0; i < points.length; i++)
							//points[i] = Double.valueOf(pointsStr);
						points[0] = Double.valueOf(parts[0]);

						return Vectors.dense(points); 								
					}
				});

				System.out.println("Mean: " + Statistics.colStats(LatData.rdd()).mean());
				
				
				//Collect stats
				JavaRDD<Vector> inputData = record.map(new Function<String, Vector>() {

					@Override
					public Vector call(String lat) throws Exception {
						String[] parts = lat.split(",");
						//String pointsStr = parts[1].trim();
						double[] points = new double[2];
						
						//for (int i = 0; i < points.length; i++)
							//points[i] = Double.valueOf(pointsStr);
						points[0] = Double.valueOf(parts[0]);
						points[1] = Double.valueOf(parts[1]);

						return Vectors.dense(points); 								
					}
				});

				System.out.println("Printing Stats: ");
				System.out.println(Statistics.corr(inputData.rdd()));
				
				System.out.println("Parsed data: ");
				System.out.println(parseddata.collect());
				System.out.println(parseddata.rdd());
*/
				
				// ML linear regression - For Training on Streaming
				// Data in micro-batches 


				int numIterations;
				double stepSize;
				double convergenceTol;
				double initWeight;

				numIterations = Integer.parseInt(numIterationsArgs);
				stepSize = Double.parseDouble(stepSizeArgs);
				convergenceTol = Double.parseDouble(convergenceTolArgs);
				initWeight = Double.parseDouble(initWeightArgs);
				
				//Parsing for LBFGS
				
				JavaRDD<Tuple2<Object,Vector>> lbfdata = record.map(new Function<String, Tuple2<Object,Vector>>() {

					@Override
					public Tuple2<Object,Vector> call(String lat) throws Exception {
						String[] parts = lat.split(",");
						String pointsStr = parts[1].trim();
						// Here assume I publish all training values to Kafka
						// topic together in a single message with comma
						// separated

						double[] points = new double[1];
						
						points[0] = Double.parseDouble(pointsStr);

						return new Tuple2<Object,Vector> (Double.parseDouble(parts[0]), MLUtils.appendBias(Vectors.dense(points))); 
								//Assume first data sent is pos, timepoint
								
					}
				});
				
				lbfdata.cache();
				
				//JavaRDD<Tuple2<Object, Vector>>[] splitInput = lbfdata.randomSplit(new double[] {0.9,0.1});
				
				JavaRDD<Tuple2<Object, Vector>> trainData = lbfdata; //splitInput[0].cache();
				JavaRDD<Tuple2<Object, Vector>> testData = lbfdata; //splitInput[1].cache();

				//Building the test data
				
				Tuple2<Object, Vector> a = lbfdata.collect().get(lbfdata.collect().size()-1);
				
				//Building the model
				
				int maxNumIterations = numIterations;
				double regParam = Double.parseDouble(regParamArgs);
				int numCorrections = Integer.parseInt(numCorrectionsArgs);
				
				Tuple2<Vector, double[]> lbfConf = LBFGS.runLBFGS(trainData.rdd(),
						new LeastSquaresGradient(),
						new SquaredL2Updater(),
						numCorrections,
						convergenceTol,
						maxNumIterations,
						regParam,
						Vectors.dense(0.0,0.0));//initialWeightsWithIntercept
				
				Vector weightsWithIntercept = lbfConf._1;
				double[] loss = lbfConf._2;
				
				System.out.println("WeightswithIntercept: "+ weightsWithIntercept);
				System.out.println("loss: "+ loss.toString());
				
				LinearRegressionModel lbfModel = new LinearRegressionModel(
						Vectors.dense(Arrays.copyOfRange(weightsWithIntercept.toArray(),0,
								weightsWithIntercept.toArray().length-1)),
						weightsWithIntercept.toArray()[weightsWithIntercept.toArray().length - 1]			
						);
				//End of LBFGS
			
				//If LR with SGD - uncomment below lines
				/*
				LinearRegressionWithSGD lr = new LinearRegressionWithSGD();
				lr.setIntercept(true);
				
				lr.optimizer().setNumIterations(numIterations)
						.setStepSize(stepSize)
						.setUpdater(new SquaredL2Updater())
						//.setMiniBatchFraction(0.9);
						.setConvergenceTol(convergenceTol)
						.setRegParam(0.000001);
				
						
				Vector initialWeights = Vectors.dense(initWeight);
				
				LinearRegressionModel model = lr.run(JavaRDD.toRDD(parseddata), initialWeights);
				
				System.out.println("NumFeatures: "+lr.getNumFeatures());
				*/
				
				//LinearRegressionModel model = LinearRegressionWithSGD.train(JavaRDD.toRDD(parseddata), numIterations);
					 // notice the .rdd()

				// JavaRDD<LabeledPoint> testData = new JavaRDD

				// USE StandardScaler for scaling/normalizing input data
				// CHECK whether model.predict() returns weights or not

				// Evaluate model on training examples and compute training
				// error
				/*JavaRDD<Tuple3<Double, Double, Vector>> valuesPredAndFeatures = parseddata
						.map(new Function<LabeledPoint, Tuple3<Double, Double, Vector>>() {
							public Tuple3<Double, Double, Vector> call(LabeledPoint point) {
								double prediction = model.predict(point.features());
								return new Tuple3<Double, Double, Vector>(prediction, point.label(), point.features());
							}
						});
				*/
				
				JavaRDD<Tuple2<Double, Double>> valuesAndPred = testData
						.map(new Function<Tuple2<Object,Vector>, Tuple2<Double, Double>>() {
							public Tuple2<Double, Double> call(Tuple2<Object,Vector> point) {
								double prediction = lbfModel.predict(Vectors.dense(point._2.apply(0)));
								return new Tuple2<Double, Double>(prediction, (Double) point._1);
							}
						});
				
								
				System.out.println("Intercept: " + lbfModel.intercept());
				System.out.println("Weights: "+ lbfModel.weights());
				System.out.println("Predictions: "+ valuesAndPred.collect());				
				
				/* In order to print out statistics of predictions - uncomment below lines
				JavaRDD<Tuple2<Object, Object>> valuesAndPredObj = testData
				.map(new Function<Tuple2<Object,Vector>, Tuple2<Object, Object>>() {
					public Tuple2<Object, Object> call(Tuple2<Object,Vector> point) {
						double prediction = lbfModel.predict(Vectors.dense(point._2.apply(0)));
						return new Tuple2<Object, Object>(prediction, point._1);
					}
				});*/
				/*
				DataFrame dfPred = hc.createDataFrame(valuesAndPred, Tuple2.class);
				DataFrame dfEval= dfPred.select("prediction","label");
				
				RegressionEvaluator eval = new RegressionEvaluator().setMetricName("r2").
						setLabelCol("label").setPredictionCol("prediction");
				System.out.println("R2: " + eval.evaluate(dfEval));				
				*/
				
				/*JavaRDD<Tuple2<Object, Object>> valuesAndPredObj = parseddata
						.map(new Function<LabeledPoint, Tuple2<Object, Object>>() {
							public Tuple2<Object, Object> call(LabeledPoint point) {
								double prediction = lbfModel.predict(point.features());
								// double prediction =
								// model.predict(Vectors.dense([5.0]));
								return new Tuple2<Object, Object>(prediction, point.label());
							}
						});
				*/
				
				/*RegressionMetrics eval2 = new RegressionMetrics(valuesAndPredObj.rdd());
				
				System.out.println("r2: "+ eval2.r2());
				System.out.println("RMSE: "+eval2.rootMeanSquaredError());
				*/
				/*
				 * JavaRDD<Tuple2<Double, Double>> valuesAndPred = parseddata
				 * .map(point -> new Tuple2<Double, Double>(point.label(), model
				 * .predict(point.features())));
				 */
				// try using model.intercept and model.weights


				/*
				 * double MSE = new JavaDoubleRDD(valuesAndPred.map( new
				 * Function<Tuple3<Double, Double, Vector>, Object>() { public
				 * Object call(Tuple3<Double, Double, Vector> pair) { return
				 * Math.pow(pair._1() - pair._2(), 2.0); } } ).rdd()).mean();
				 * System.out.println("training Mean Squared Error = " + MSE);
				 */

				// Next is Querying data with result of ML
				
				  double lat = valuesAndPred.collect().get(0)._1; 
				  double lat1 = lat - 0.0005; //Adjusting latitude values in order to scan for nearby restaurants
				  double lat2 = lat + 0.0005;
				   
				  
				  hc.setConf("LAT1", String.valueOf(lat1)); 
				  hc.setConf("LAT2", String.valueOf(lat2));
				  
				  DataFrame resultRow = hc.
				  sql("select reviews.reviewer_id, restaurants.category, restaurants.lat, restaurants.lng,"
				  +" avg(reviews.rating) as avg_rating from restaurants.reviews,"
				  +" restaurants.restaurants where reviews.restaurant_id == restaurants.id"
				  +" and reviews.rating> '3'"
				  +" and restaurants.lat between '${hiveconf:LAT1}' and '${hiveconf:LAT2}' and"
				  +" restaurants.lng between 13.3 and 13.4"
				  +" group by reviews.reviewer_id, restaurants.lat, restaurants.lng, restaurants.category LIMIT 5"
				  );
				  
				  List<String> result = null;
				  
				  resultRow.write().mode("append").saveAsTable("restaurants.results");
				  
				  if(!resultRow.rdd().isEmpty()) { 
					  result = resultRow.javaRDD().map(new Function<Row, String>() {
						  public String call(Row x) throws Exception { 
							  return x.getString(0);
						  	} 
						  }).collect(); 
					  }
				  
				  System.out.println("Result: " + result);
				  
				 

				// Pushing output to KafkaProducer
				KafkaProducer<String, String> producer = new KafkaProducer<>(props);
				// ProducerRecord<String, String> output = new ProducerRecord<String, String>("MyTest", result.get(0));
				ProducerRecord<String, String> output = 
						new ProducerRecord<String, String>("MyTest", valuesAndPred.collect().get(0).toString());

				// Now send/push to kafka output topic
				producer.send(output);

				 
			}
		});

		// Start the computation
		jssc.start();

		jssc.awaitTermination();

	}
}