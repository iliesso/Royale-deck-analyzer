package crtracker.hadoop;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * DataCleaner:
 * - Lecture et parsing JSON
 * - Validation du nombre de cartes par deck (exactement 8)
 * - Création d'une clé unique basée sur les joueurs (triés par ordre alphabetique).
 */

public class DataCleaner extends Configured implements Tool {

    public static class DataMapper extends Mapper<Object, Text, Text, GameResume> {
        private ObjectMapper jsonMapper = new ObjectMapper();


        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            try {
                GameResume gameResume = jsonMapper.readValue(line, GameResume.class);
                PlayerResume p1 = gameResume.getPlayer1();
                PlayerResume p2 = gameResume.getPlayer2();

                if (!checkDeck(p1.getDeck()) || !checkDeck(p2.getDeck())) {

                    String uniqueKey = buildKeyFromGame(gameResume);
                    context.write(new Text(uniqueKey), gameResume);
                }
            } catch (Exception e) {
                // On logue la ligne pour comprendre pourquoi elle ne passe pas
                System.err.println("Erreur parsing JSON line: " + line);
                System.err.println("Cause: " + e.getMessage());
            }
        }

        private String buildKeyFromGame(GameResume gameResume){
            return gameResume.getGame() + "_" + gameResume.getMode() + "_" + gameResume.getRound() + "_"
            + (gameResume.getPlayer1().getUtag().compareTo(gameResume.getPlayer2().getUtag()) < 0
                ? gameResume.getPlayer1().getUtag() + "_" + gameResume.getPlayer2().getUtag()
                : gameResume.getPlayer2().getUtag() + "_" + gameResume.getPlayer1().getUtag());
        }

        private boolean checkDeck(String deck) {
            return deck.length() == 16; 
        }
    }

    public static class DataCombiner extends Reducer<Text, GameResume, Text, GameResume> {
        @Override
        public void reduce(Text key, Iterable<GameResume> values, Context context)
                throws IOException, InterruptedException {
    
            // Copier les valeurs dans une liste pour les trier
            List<GameResume> gameList = new ArrayList<>();
            for (GameResume g : values) {
                // Cloner l'objet si nécessaire, car Hadoop réutilise les instances
                gameList.add(g.clone());
            }
    
            // Trier par date
            gameList.sort(Comparator.comparing(GameResume::getDateAsInstant));
    
            // Filtrer les doublons très rapprochés dans le temps
            List<GameResume> filtered = new ArrayList<>();
            for (GameResume current : gameList) {
                if (filtered.isEmpty()) {
                    filtered.add(current);
                } else {
                    GameResume last = filtered.get(filtered.size() - 1);
                    if (!isDuplicateWithinTimeRange(last, current, 10)) { // 10 secondes par exemple
                        filtered.add(current);
                    }
                }
            }
    
            // Émettre les parties filtrées
            for (GameResume g : filtered) {
                context.write(key, g);
            }
        }
    
        // Méthode pour vérifier si deux parties sont considérées comme des doublons temporels
        private boolean isDuplicateWithinTimeRange(GameResume g1, GameResume g2, int secondsRange) {
            // Vérifier ici que g1 et g2 sont "identiques" sur les attributs de la partie
            // On suppose que si on est dans le même reducer pour la même clé, c'est déjà
            // le même game/mode/round/players, etc.
            // Il ne reste qu'à vérifier la date
    
            long diff = Math.abs(g1.getDateAsInstant().getEpochSecond() - g2.getDateAsInstant().getEpochSecond());
            return diff <= secondsRange;
        }
    }
    
    public static class DataReducer extends Reducer<Text, GameResume, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<GameResume> values, Context context)
                throws IOException, InterruptedException {
            // Même logique que le combiner
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
                    if (!isDuplicateWithinTimeRange(last, current, 10)) {
                        filtered.add(current);
                    }
                }
            }
    
            // Émettre la valeur finale en JSON par exemple
            for (GameResume g : filtered) {
                context.write(key, new Text(g.toJsonString())); 
            }
        }
    
        private boolean isDuplicateWithinTimeRange(GameResume g1, GameResume g2, int secondsRange) {
            long diff = Math.abs(g1.getDateAsInstant().getEpochSecond() - g2.getDateAsInstant().getEpochSecond());
            return diff <= secondsRange;
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