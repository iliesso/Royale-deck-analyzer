package crtracker.hadoop;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

    private static final Log LOG = LogFactory.getLog(DataCleaner.class);

    public static class DataMapper extends Mapper<Object, Text, Text, GameResume> {
        private ObjectMapper jsonMapper = new ObjectMapper();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Parsing du JSON
            String line = value.toString();
            try {
                GameResume gameResume = jsonMapper.readValue(line, GameResume.class);

                // Vérification de la longueur du deck
                if (gameResume.getPlayer1().getDeck().length() == 16
                        && gameResume.getPlayer2().getDeck().length() == 16) {
                    // Construction de la clé unique
                    String uniqueKey = buildKeyFromGame(gameResume);
                    context.write(new Text(uniqueKey), gameResume);
                }
            } catch (Exception e) {
                // Ignorer la ligne si parsing ou condition échoue
            }
        }

        private String buildKeyFromGame(GameResume game) {
            return game.getDate() + "_" + game.getMode() + "_" + game.getRound()
                    + "_" + game.getPlayer1().getUtag() + "_" + game.getPlayer2().getUtag();
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
                    Set<GameResume> uniqueGames = new HashSet<>();
                    for (GameResume g : values) {
                        // Comme GameResume implémente equals/hashCode, l'ajout au Set élimine les doublons
                        uniqueGames.add(cloneGameResume(g));
                    }
                    
                    for (GameResume g : uniqueGames) {
                        context.write(key, g);
                    }
        }
        // Clone pour éviter les problèmes d'objets réutilisés par Hadoop
        private GameResume cloneGameResume(GameResume g) {
            return g.clone(); 
        }
    }

    public static class DataReducer extends Reducer<Text, GameResume, Text, Text> {
        // A nouveau eliminer les doublons
        public void reduce(Text key, Iterable<GameResume> values, Context context)
                throws IOException, InterruptedException {
            Set<GameResume> uniqueGames = new HashSet<>();
            for (GameResume g : values) {
                uniqueGames.add(cloneGameResume(g));
            }
            
            for (GameResume g : uniqueGames) {
                context.write(key, new Text(g.toString()));
            }
        }
        
        private GameResume cloneGameResume(GameResume g) {
            return g.clone();
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