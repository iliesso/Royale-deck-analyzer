package crtracker.hadoop;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
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
        private ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public void setup(Context context) {
			Configuration conf = context.getConfiguration();
		}

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            JsonNode root;
            try {
                root = objectMapper.readTree(line);
            } catch (Exception e) {
                return;
            }

            // Vérification des champs requis
            JsonNode players = root.get("players");
            JsonNode dateNode = root.get("date");
            if (players == null || players.size() != 2 || dateNode == null) {
                return; 
            }

            // Extraction des informations nécessaires
            String playerAstr;
            String playerBstr;
            String dateStr = dateNode.asText();
            if (players.get(0).has("utag") && players.get(1).has("utag")) {
                playerAstr = players.get(0).get("utag").asText();
                playerBstr = players.get(1).get("utag").asText();
            }
            else {
                return;
            }
            

            // Vérification des decks
            if (!checkDeck(players.get(0)) || !checkDeck(players.get(1))) {
                return;
            }
            
            PlayerResume playerA = new PlayerResume(players.get(0).get("utag").asText(), players.get(0).get("ctag").asText(), players.get(0).get("trophies").asInt(), players.get(0).get("exp").asInt(), players.get(0).get("league").asInt(), players.get(0).get("bestleague").asInt(), players.get(0).get("deck").asText(), players.get(0).get("evo").asText(), players.get(0).get("tower").asText(), players.get(0).get("strength").asDouble(), players.get(0).get("crown").asInt(), players.get(0).get("elixir").asDouble(), players.get(0).get("touch").asInt(), players.get(0).get("score").asInt());
            PlayerResume playerB = new PlayerResume(players.get(1).get("utag").asText(), players.get(1).get("ctag").asText(), players.get(1).get("trophies").asInt(), players.get(1).get("exp").asInt(), players.get(1).get("league").asInt(), players.get(1).get("bestleague").asInt(), players.get(1).get("deck").asText(), players.get(1).get("evo").asText(), players.get(1).get("tower").asText(), players.get(1).get("strength").asDouble(), players.get(1).get("crown").asInt(), players.get(1).get("elixir").asDouble(), players.get(1).get("touch").asInt(), players.get(1).get("score").asInt());

            // Émettre la ligne validée
            GameResume gameResume = new GameResume(dateStr, root.get("game").asText(), root.get("mode").asText(), root.get("round").asInt(), root.get("type").asText(), root.get("winner").asInt(), playerA, playerB);
            context.write(new Text(UUID.randomUUID().toString()), new Text(objectMapper.writeValueAsString(gameResume)));
        }

        private boolean checkDeck(JsonNode playerNode) {
            if (!playerNode.has("deck")) return false;
            String deck = playerNode.get("deck").asText();
            return deck.length() == 16; 
        }
    }

    public static class DataReducer extends Reducer<Text, Text, NullWritable, Text> {
        private ObjectMapper objectMapper = new ObjectMapper();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
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