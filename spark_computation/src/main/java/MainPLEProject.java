
import java.util.ArrayList;

import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

public class MainPLEProject
{
	private static String phasesInputPath = "/user/nsentout/PhaseSequenceFiles/*";
	//private static String phasesInputPath = "/user/nsentout/PhaseSequenceFiles/part-m-0020*";
	
	private static String outputPath = "hdfs://froment:9000/user/nsentout/output-project";

	public static void main(String[] args)
	{	
		if (args.length > 0) {
			SparkConf conf = new SparkConf().setAppName("Projet PLE");
			JavaSparkContext context = new JavaSparkContext(conf);
			
			ArrayList<String> output = new ArrayList<String>();

			JavaRDD<PhaseWritable> phasesRdd = context.sequenceFile(phasesInputPath, NullWritable.class, PhaseWritable.class)
					.values().persist(StorageLevel.DISK_ONLY());
			
			switch (args[0]) {
				case "q1a":
					Questions.question1a(phasesRdd, output);
					break;
				case "q1b":
					Questions.question1b(phasesRdd, output);
					break;
				case "q1c":
					Questions.question1c(phasesRdd, output);
					break;
				case "q2":
					Questions.question2(phasesRdd, output);
					break;
				case "q3":
					Questions.question3(phasesRdd, output);
					break;
				case "q4":
					Questions.question4(phasesRdd, output);
					break;
				case "q5":
					Questions.question5(phasesRdd, output);
					break;
				case "q6":
					Questions.question6(phasesRdd, output, context);
					break;
				case "q7":
					if (args.length > 1) {
						String patternsInput = args[1];
						Questions.question7(patternsInput, context, outputPath);
					}
					else {
						System.err.println("ERREUR: IL MANQUE UN ARGUMENT");
						System.err.println("Vous devez préviser les 4 patterns que les phases doivent comporter en séparant chaque pattern par une virgule"
								+ ". Exemple : 0,5,14,21");
						System.exit(-1);
					}
					
					break;
				default:
					System.err.println("ERREUR: ARGUMENT INCONNU");
					System.err.println("Seulement les commandes suivantes sont acceptées : q1a, q1b, q1c, q2, q3, q4, q5, q6, q7");
					System.exit(-1);
			}
			
			if (!output.isEmpty()) {
				context.parallelize(output).repartition(1).saveAsTextFile(outputPath);
			}
		}
		else {
			System.err.println("ERREUR: PAS D'ARGUMENT");
			System.err.println("Vous devez donner en argument le numéro de la question. Voici les commandes disponibles : "
					+ "q1a, q1b, q1c, q2, q3, q4, q5, q6, q7");
			System.exit(-2);
		}
	}
	
}
