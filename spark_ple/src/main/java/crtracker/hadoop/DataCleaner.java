package crtracker.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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

            if (!checkDeck(players.get(0)) || !checkDeck(players.get(1))) {
                return;
            }

        PlayerResume playerA = createPlayerResume(players.get(0));
        PlayerResume playerB = createPlayerResume(players.get(1));

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
        @Override
        public void reduce(Text key, Iterable<GameResume> values, Context context)
                throws IOException, InterruptedException {
    
            List<GameResume> gameList = new ArrayList<>();
            for (GameResume g : values) {
                gameList.add(g.clone());
            }
    
            // Trier par date
            gameList.sort(Comparator.comparing(GameResume::getDateAsInstant));
    
            List<GameResume> filtered = new ArrayList<>();
            for (GameResume current : gameList) {
                if (filtered.isEmpty()) {
                    filtered.add(current);
                } else {
                    GameResume last = filtered.get(filtered.size() - 1);
                    if (!current.compareSeconds(last)) { 
                        filtered.add(current);
                    }
                }
            }
    
            for (GameResume g : filtered) {
                context.write(key, g);
            }
        }
    }
    
    public static class DataReducer extends Reducer<Text, GameResume, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<GameResume> values, Context context)
                throws IOException, InterruptedException {
            List<GameResume> gameList = new ArrayList<>();
            for (GameResume g : values) {
                gameList.add(g.clone());
            }
    
            gameList.sort(Comparator.comparing(GameResume::getDateAsInstant));
    
            List<GameResume> filtered = new ArrayList<>();
            for (GameResume current : gameList) {
                if (filtered.isEmpty()) {
                    filtered.add(current);
                } else {
                    GameResume last = filtered.get(filtered.size() - 1);
                    if (!current.compareSeconds(last)) {
                        filtered.add(current);
                    }
                }
            }
    
            for (GameResume g : filtered) {
                context.write(key, new Text(g.toJsonString())); 
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