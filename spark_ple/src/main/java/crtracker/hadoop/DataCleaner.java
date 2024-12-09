package crtracker.hadoop;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
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

import crtracker.ResumeCities.CityResume;

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

    public static class DataMapper extends Mapper<Object, Text, Text, GameResume> {
        private ObjectMapper objectMapper = new ObjectMapper();


        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.isEmpty()) return;
            JsonNode root;
            try {
                root = objectMapper.readTree(line);
            } catch (Exception e) {
                return;
            }

            JsonNode players = root.get("players");
            JsonNode dateNode = root.get("date");
            if (players == null || players.size() != 2 || dateNode == null) {
                return; 
            }

            String dateStr = dateNode.asText();

            // Vérification des decks
            if (!checkDeck(players.get(0)) || !checkDeck(players.get(1))) {
                return;
            }

            //Création des player
            // Création des PlayerResume avec valeurs par défaut si nécessaire
        PlayerResume playerA = createPlayerResume(players.get(0));
        PlayerResume playerB = createPlayerResume(players.get(1));

        // Création de GameResume avec valeurs par défaut
        String game = root.path("game").asText("unknown_game");
        String mode = root.path("mode").asText("unknown_mode");
        int round = root.path("round").asInt(0);
        String type = root.path("type").asText("unknown_type");
        int winner = root.path("winner").asInt(-1);

        GameResume gameResume = new GameResume(dateStr, game, mode, round, type, winner, playerA, playerB);
        String normalizedKey = game + "_" + mode + "_" + round + "_" 
        + (playerA.getUtag().compareTo(playerB.getUtag()) < 0 
           ? playerA.getUtag() + "_" + playerB.getUtag() 
           : playerB.getUtag() + "_" + playerA.getUtag());
        
        context.write(new Text(normalizedKey), gameResume);
        }

        private PlayerResume createPlayerResume(JsonNode playerNode) {
            String utag = playerNode.path("utag").asText("unknown_utag");
            String ctag = playerNode.path("ctag").asText("unknown_ctag");
            int trophies = playerNode.path("trophies").asInt(0);
            int exp = playerNode.path("exp").asInt(0);
            int league = playerNode.path("league").asInt(0);
            int bestleague = playerNode.path("bestleague").asInt(0);
            String deck = playerNode.path("deck").asText("default_deck");
            String evo = playerNode.path("evo").asText("");
            String tower = playerNode.path("tower").asText("");
            double strength = playerNode.path("strength").asDouble(0.0);
            int crown = playerNode.path("crown").asInt(0);
            double elixir = playerNode.path("elixir").asDouble(0.0);
            int touch = playerNode.path("touch").asInt(0);
            int score = playerNode.path("score").asInt(0);
    
            return new PlayerResume(utag, ctag, trophies, exp, league, bestleague, deck, evo, tower, strength, crown, elixir, touch, score);
        }

        private boolean checkDeck(JsonNode playerNode) {
            if (!playerNode.has("deck")) return false;
            String deck = playerNode.get("deck").asText();
            return deck.length() == 16; 
        }
    }

    public static class DataCombiner extends Reducer<Text, GameResume, Text, GameResume> {
        public static GameResume filterDateDuplicates(Iterable<GameResume> values){
            GameResume gameResume = null;
            Iterator<GameResume> it = values.iterator();
            if (it.hasNext()){
                gameResume = it.next().clone();
                while (it.hasNext()){
                    if (!gameResume.compareSeconds(it.next())){
                        return null;
                    }
                }
            }
            return gameResume;
        }
        
        @Override
        public void reduce(Text key, Iterable<GameResume> values, Context context)
                throws IOException, InterruptedException {
            GameResume gameResume = filterDateDuplicates(values);
            if (gameResume != null){
                context.write(key, gameResume);
            }
        }
    }
    
    public static class DataReducer extends Reducer<Text, GameResume, Text, Text> {
        public static GameResume filterDateDuplicates(Iterable<GameResume> values){
            GameResume gameResume = null;
            Iterator<GameResume> it = values.iterator();
            if (it.hasNext()){
                gameResume = it.next().clone();
                while (it.hasNext()){
                    if (!gameResume.compareSeconds(it.next())){
                        return null;
                    }
                }
            }
            return gameResume;
        }
        
        @Override
        public void reduce(Text key, Iterable<GameResume> values, Context context)
                throws IOException, InterruptedException {
            GameResume gameResume = filterDateDuplicates(values);
            if (gameResume != null){
                context.write(key, new Text(gameResume.toJsonString()));
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
        job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(GameResume.class);
        job.setCombinerClass(DataCombiner.class);
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