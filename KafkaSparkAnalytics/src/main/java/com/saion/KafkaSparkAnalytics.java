
package com.saion;

import java.util.Arrays;
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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;
import scala.Tuple3;
import scala.reflect.ClassTag;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD;

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
		if (args.length < 2) {
			System.err.println("Usage: KafkaWordCount <brokers> <topics>\n"
					+ "  <brokers> is a list of one or more Kafka brokers\n"
					+ "  <topics> is a list of one or more kafka topics to consume from\n\n");
			System.exit(1);
		}

		// StreamingExamples.setStreamingLogLevels();

		String brokers = args[0];
		String topics = args[1];

		// Create context with a 5 seconds batch interval
		SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(5));

		Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
	    Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", brokers);


		//Initialise Hive Query variables
	 	System.out.println("STARTING HIVE QUERY...................!!!!!!!!!!!!!");
		
		@SuppressWarnings("deprecation")
		HiveContext hc = new HiveContext(jsc);

		DataFrame reviews_orc = hc.read().format("orc").load("/apps/hive/warehouse/restaurants.db/reviews");
		DataFrame restaurants_orc = hc.read().format("orc").load("/apps/hive/warehouse/restaurants.db/restaurants");

		reviews_orc.registerTempTable("reviews");

		restaurants_orc.registerTempTable("restaurants");
		

		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

		// Get the lines, split them into words, count the words and print
		 JavaDStream<String> lines = messages.map(new Function<Tuple2<String,String>, String>() {
		 @Override
		 public String call(Tuple2<String, String> tuple2) {
			 return tuple2._2();
		 	}
		 });
		 
		 JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			 @Override
			 public Iterable<String> call(String x){
				 return Arrays.asList(SPACE.split(x));
			 }
		});

	    words.print();

	    //ML For Prediction on Streaming Data
	    /*
	    int numFeatures = 2;
   
	    StreamingLinearRegressionWithSGD model = new StreamingLinearRegressionWithSGD().setInitialWeights(Vectors.zeros(numFeatures));	    
	    	
	    JavaDStream<LabeledPoint> trainingData = words.map(new Function<String, LabeledPoint>() {
	    	@Override
	    	public LabeledPoint call(String w){
	    		double lat = Double.parseDouble(w);
	    		return new LabeledPoint(1.0, Vectors.dense(1.0,lat));
	    	}
		});
	    
	    model.trainOn(trainingData);
	    //model.predictOn(Vectors.dense(5.0));
	    
	    //JavaDStream<String> javaDStream = new JavaDStream<String>(dstream, scala.reflect.ClassTag$.MODULE$.apply(String.class));
	    		
	    model.pr
	    
	    model.predictOn(trainingData.dstream().map(new Function<LabeledPoint, Vector>() {

			@Override
			public Vector call(LabeledPoint arg0){
				// TODO Auto-generated method stub
				return null;
			}
	    	
	    }, ClassTag<Vector>));
	    
	    
	    */

	    words.foreachRDD(record -> {
	    	if(!record.isEmpty())
	    		{
	    			System.out.println("Input found!!! " + record.collect() );
	    			System.out.println("First: " + record.first() );
	    			
	    			
	    			//ML Streaming linear regression - For Training on Streaming Data in micro-batches but Prediction on Static Data at time-point n	    			    			
	    		    				
	    			JavaRDD<LabeledPoint> parseddata = record
	    		            .map(new Function<String, LabeledPoint>() {
	    		            
	    		                @Override
	    		                public LabeledPoint call(String lat) throws Exception {
	    		                    String[] parts = lat.split(",");
	    		                    String[] pointsStr = parts[1].split(" ");	    		                	 
	    		                	// Here assume I publish all training values to Kafka topic together in a single message with comma separated
	    		                    double[] points = new double[pointsStr.length];
	    		                
	    		                    for (int i = 0; i < points.length; i++)
	   		                    		points[i] = Double.valueOf(pointsStr[i])/50;
	  	    		                    
	    		                    return new LabeledPoint(Double.valueOf(parts[0]), //Assume first data sent is timepoint, followed by lat
	    		                            Vectors.dense(points));
	    		                }
	    		            });
	    			
	    			parseddata.cache();

	                 
	    			System.out.println("Parsed data: ");
	    			System.out.println(parseddata.collect());
	    			System.out.println(parseddata.rdd());
	    			
	    			// Building the model - gives good values with numIter = 10 and stepSize = 0.1
	    		    int numIterations = 10;
	    		    double stepSize = 0.1;
	    		    LinearRegressionModel model = LinearRegressionWithSGD.train(
	    		    		JavaRDD.toRDD(parseddata), numIterations, stepSize); // notice the .rdd()
	    		    
	    		    //JavaRDD<LabeledPoint> testData = new JavaRDD
	    		    
	    		    // Evaluate model on training examples and compute training error
	    		    JavaRDD<Tuple3<Double, Double, Vector>> valuesAndPred = parseddata.map(
	    		    	     new Function<LabeledPoint, Tuple3<Double, Double, Vector>>() {
	    		    	        public Tuple3<Double, Double, Vector> call(LabeledPoint point) {
		    		    	          double prediction = model.predict(point.features());
	    		    	        	  //double prediction = model.predict(Vectors.dense([5.0]));
		    		    	          return new Tuple3<Double, Double, Vector>(prediction*50, point.label(), point.features());
		    		    	     }
	    		    	      }
	    		    	    );
	    		    
	    		    /*JavaRDD<Tuple2<Double, Double>> valuesAndPred = parseddata
	    		            .map(point -> new Tuple2<Double, Double>(point.label(), model
	    		                    .predict(point.features())));
	    		    */
	    		    
	    		    System.out.println(valuesAndPred.collect());
	    		    
	    		    double MSE = new JavaDoubleRDD(valuesAndPred.map(
	    		    	      new Function<Tuple3<Double, Double, Vector>, Object>() {
	    		    	        public Object call(Tuple3<Double, Double, Vector> pair) {
	    		    	          return Math.pow(pair._1() - pair._2(), 2.0);
	    		    	        }
	    		    	      }
	    		    	    ).rdd()).mean();
	    		    	    System.out.println("training Mean Squared Error = " + MSE);
	    		    
	    		    
	    			//Next is Querying data with result of ML	
	    		    /*
	    			double lat = Double.parseDouble(record.first());
	    			double lat1 = lat - 0.05;
	    			double lat2 = lat + 0.05;
	    			
	    			hc.setConf("LAT1", String.valueOf(lat1));
	    			hc.setConf("LAT2", String.valueOf(lat2));
	    		
	    			DataFrame resultRow = hc.sql("select reviews.reviewer_id, restaurants.category, restaurants.lat, restaurants.lng,"
			x			+" avg(reviews.rating) as avg_rating from restaurants.reviews,"
						+" restaurants.restaurants where reviews.restaurant_id == restaurants.id"
						+" and reviews.rating> '4'" 
						+" and restaurants.lat between '${hiveconf:LAT1}' and '${hiveconf:LAT2}' and"
						+" restaurants.lng between 13.3 and 13.4"
						+" group by reviews.reviewer_id, restaurants.lat, restaurants.lng, restaurants.category LIMIT 5");
   			
	    			List<String> result = resultRow.javaRDD().map(new Function<Row, String>() {
	    				public String call(Row x) throws Exception {
	    					return x.getString(0);
	    				}
	    			}).collect();
	    			
	    			System.out.println(result);
	    			*/
	    		    
	    			/*
	    			//Pushing output to KafkaProducer	    			
	    			
	    			HashMap<String,Object> props = new HashMap<>();
                	props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
                	props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
                	props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
                	KafkaProducer<String, String> producer = new KafkaProducer<>(props);
	    			
                	ProducerRecord<String, String> output = new ProducerRecord<>("MyTest", result.get(0)); 
                	//Now send/push to kafka output topic
                	producer.send(output);
                	producer.close();
                	*/
	    		}
	    });
	    
	    
	    
	 // Start the computation
	 		jssc.start();
	 		
	 		jssc.awaitTermination();
		
		
	}
}