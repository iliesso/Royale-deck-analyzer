package crtracker.hadoop;

import java.io.IOException;
import java.util.UUID;

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

/**
 * DataCleaner:
 * - Lecture et parsing JSON
 * - Validation du format
 * - Validation du nombre de cartes par deck (exactement 8)
 * - Création d'une clé unique basée sur les joueurs (triés par ordre lexical)
 * et la date arrondie à la minute.
 * - Élimination des doublons exacts et parties répétées (même clé).
 * - Sortie au format JSON : {"id":"<clé>","game": {...jeu original...}}
 */
public class DataCleaner extends Configured implements Tool {

    public static class DataMapper extends Mapper<Object, Text, Text, GameResume> {
        private ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty())
                return;
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

            String dateStr = dateNode.asText();

            PlayerResume playerA = new PlayerResume(players.get(0).get("utag").asText(),
                    players.get(0).get("ctag").asText(), players.get(0).get("trophies").asInt(),
                    players.get(0).get("exp").asInt(), players.get(0).get("league").asInt(),
                    players.get(0).get("bestleague").asInt(), players.get(0).get("deck").asText(),
                    players.get(0).get("evo").asText(), players.get(0).get("tower").asText(),
                    players.get(0).get("strength").asDouble(), players.get(0).get("crown").asInt(),
                    players.get(0).get("elixir").asDouble(), players.get(0).get("touch").asInt(),
                    players.get(0).get("score").asInt());
            PlayerResume playerB = new PlayerResume(players.get(1).get("utag").asText(),
                    players.get(1).get("ctag").asText(), players.get(1).get("trophies").asInt(),
                    players.get(1).get("exp").asInt(), players.get(1).get("league").asInt(),
                    players.get(1).get("bestleague").asInt(), players.get(1).get("deck").asText(),
                    players.get(1).get("evo").asText(), players.get(1).get("tower").asText(),
                    players.get(1).get("strength").asDouble(), players.get(1).get("crown").asInt(),
                    players.get(1).get("elixir").asDouble(), players.get(1).get("touch").asInt(),
                    players.get(1).get("score").asInt());

            // Vérification des decks
            if (!checkDeck(playerA) || !checkDeck(playerB)) {
                return;
            }

            // Émettre la ligne validée
            GameResume gameResume = new GameResume(dateStr, root.get("game").asText(), root.get("mode").asText(),
                    root.get("round").asInt(), root.get("type").asText(), root.get("winner").asInt(), playerA, playerB);
            context.write(new Text(UUID.randomUUID().toString()), gameResume);
        }

        // Retourne true si le deck a 8 cartes
        private boolean checkDeck(PlayerResume playerResume) {
            if (playerResume.getDeck().isEmpty())
                return false;
            return playerResume.getDeck().length() == 16;
        }
    }

    public static class DataCombiner extends Reducer<Text, GameResume, Text, GameResume> {
        // parcourir toutes les parties
        // pour chaque partie, vérifier si elle est identique à une autre partie
        // si elle est identique, ne pas l'ajouter
        public void reduce(Text key, Iterable<GameResume> values, Context context)
                throws IOException, InterruptedException {
            GameResume game = values.iterator().next();
            boolean isDuplicate = false;
            for (GameResume other : values) {
                if (game.compareTo(other)) {
                    isDuplicate = true;
                    break;
                }
            }
            if (!isDuplicate) {
                context.write(key, game);
            }
        }
    }

    public static class DataReducer extends Reducer<Text, GameResume, Text, GameResume> {
        // A nouveau eliminer les doublons
        public void reduce(Text key, Iterable<GameResume> values, Context context)
                throws IOException, InterruptedException {
            GameResume game = values.iterator().next();
            boolean isDuplicate = false;
            for (GameResume other : values) {
                if (game.compareTo(other)) {
                    isDuplicate = true;
                    break;
                }
            }
            if (!isDuplicate) {
                context.write(new Text(key.toString()), game);
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
        job.setCombinerClass(DataCombiner.class);
        job.setReducerClass(DataReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(GameResume.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        try {
            FileInputFormat.setInputPaths(job, args[0]);
            FileOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path(args[1]));
        } catch (Exception e) {
            System.out.println("Erreur lors de la configuration des entrées/sorties : " + e.getMessage());
            return -1;
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new DataCleaner(), args));
    }
}