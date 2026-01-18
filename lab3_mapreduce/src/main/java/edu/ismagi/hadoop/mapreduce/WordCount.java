package edu.ismagi.hadoop.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    // 1. LE MAPPER : Découpe le texte en mots
    // <Object, Text> : Types d'entrée (décalage du fichier, ligne de texte)
    // <Text, IntWritable> : Types de sortie (le mot, le chiffre 1)
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1); // On crée un "1" Hadoop
        private Text word = new Text(); // On prépare un objet texte Hadoop

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // StringTokenizer sépare la ligne en mots individuels
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken()); // On prend le mot suivant
                context.write(word, one); // On émet le couple (mot, 1)
            }
        }
    }

    // 2. LE REDUCER : Additionne les "1" pour chaque mot
    // <Text, IntWritable> : Types d'entrée (le mot, liste de "1")
    // <Text, IntWritable> : Types de sortie (le mot, total final)
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get(); // On additionne tous les 1 reçus pour ce mot
            }
            result.set(sum);
            context.write(key, result); // On écrit le résultat final (mot, total)
        }
    }

    // 3. LE DRIVER : Configure et lance le travail (Job)
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count"); // Création du job

        job.setJarByClass(WordCount.class); // Définit la classe principale
        job.setMapperClass(TokenizerMapper.class); // Définit le Mapper
        job.setCombinerClass(IntSumReducer.class); // Optimisation (facultatif)
        job.setReducerClass(IntSumReducer.class); // Définit le Reducer

        job.setOutputKeyClass(Text.class); // Type de la clé de sortie (Mot)
        job.setOutputValueClass(IntWritable.class); // Type de la valeur de sortie (Nombre)

        // args[0] est le dossier d'entrée, args[1] le dossier de sortie sur HDFS
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1); // Lance l'exécution
    }
}