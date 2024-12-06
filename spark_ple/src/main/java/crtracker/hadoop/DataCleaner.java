package crtracker.hadoop;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * DataCleaner:
 * - Lecture et parsing JSON
 * - Validation du format
 * - Validation du nombre de cartes par deck (exactement 8)
 * - Création d'une clé unique basée sur les joueurs (triés par ordre lexical) et la date arrondie à la minute.
 * - Élimination des doublons exacts et parties répétées (même clé).
 * - Sortie au format JSON : {"id":"<clé>","game": {...jeu original...}}
 */
public class DataCleaner extends Configured implements Tool {

    public static class DataMapper extends Mapper<Object, Text, Text, Text> {
        private ObjectMapper objectMapper;
        private SimpleDateFormat inputFormat;
        private SimpleDateFormat roundedFormat;
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void setup(Context context) {
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            objectMapper = new ObjectMapper();
            inputFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            inputFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
            roundedFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
            roundedFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            JsonNode root;
            try {
                root = objectMapper.readTree(line);
            } catch (Exception e) {
                // Pas un JSON valide
                return;
            }

            // Vérification des champs requis
            JsonNode players = root.get("players");
            JsonNode dateNode = root.get("date");
            if (players == null || players.size() != 2 || dateNode == null) {
                return; 
            }

            // Extraction des informations nécessaires
            String dateStr = dateNode.asText();
            String playerA = players.get(0).has("utag") ? players.get(0).get("utag").asText() : "";
            String playerB = players.get(1).has("utag") ? players.get(1).get("utag").asText() : "";

            // Vérification des decks
            if (!checkDeck(players.get(0)) || !checkDeck(players.get(1))) {
                return;
            }

            // Normalisation de la date à la minute près
            Date date;
            try {
                date = inputFormat.parse(dateStr);
            } catch (ParseException e) {
                return;
            }
            String roundedDate = roundedFormat.format(date);

            // Tri des joueurs pour clé stable (indépendamment de l'ordre)
            String[] sortedPlayers = {playerA, playerB};
            Arrays.sort(sortedPlayers);

            // Clé composite
            String compositeKey = sortedPlayers[0] + "_" + sortedPlayers[1] + "_" + roundedDate;
            outKey.set(compositeKey);
            outValue.set(line);

            // Émettre la ligne validée
            context.write(outKey, outValue);
        }

        /**
         * Vérifie que le deck d'un joueur contient exactement 8 cartes.
         * Ici, on suppose que 'deck' est une chaîne où chaque carte est représentée par 2 caractères.
         * Donc 8 cartes = 16 caractères.
         */
        private boolean checkDeck(JsonNode playerNode) {
            if (!playerNode.has("deck")) return false;
            String deck = playerNode.get("deck").asText();
            // Vérification de la longueur
            return deck.length() == 16; 
        }
    }

    /**
     * Combiner (optionnel):
     * Peut être identique au Reducer. Son rôle serait de ne garder qu'un enregistrement par clé localement.
     * Si vous le souhaitez, vous pouvez décommenter et l'utiliser.
     */
    /*
    public static class DataCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // On ne prend que la première occurrence
            for (Text v : values) {
                context.write(key, v);
                break;
            }
        }
    }
    */

    public static class DataReducer extends Reducer<Text, Text, NullWritable, Text> {
        private ObjectMapper objectMapper = new ObjectMapper();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Nous avons potentiellement plusieurs valeurs pour la même clé.
            // Pour éliminer les doublons exacts et les parties répétées (même joueurs, même date),
            // on ne prend que la première occurrence.
            Text firstValue = null;
            for (Text v : values) {
                firstValue = v;
                break;
            }

            if (firstValue != null) {
                // Construire l'objet final JSON
                JsonNode gameNode = objectMapper.readTree(firstValue.toString());
                ObjectNode outputNode = objectMapper.createObjectNode();
                outputNode.put("id", key.toString());
                outputNode.set("game", gameNode);

                context.write(NullWritable.get(), new Text(outputNode.toString()));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: DataCleaner <input> <output>");
            return -1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Data Cleaning");
        job.setJarByClass(DataCleaner.class);

        job.setMapperClass(DataMapper.class);
        // Si vous souhaitez utiliser un combiner, décommentez :
        // job.setCombinerClass(DataCombiner.class);
        job.setReducerClass(DataReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        try{
            FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path(args[1]));
        }
        catch (Exception e){
            System.out.println("Erreur lors de la configuration des entrées/sorties : " + e.getMessage());
            return -1;
        }
        
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new DataCleaner(), args));
    }
}