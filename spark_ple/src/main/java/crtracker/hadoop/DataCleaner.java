package crtracker.hadoop;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
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

public class DataCleaner extends Configured implements Tool {

    //Map
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
            return gameResume.getDateAsInstant() + "_" + gameResume.getGame() + "_" + gameResume.getMode() + "_" + gameResume.getRound() + "_"
            + (gameResume.getPlayer1().getUtag().compareTo(gameResume.getPlayer2().getUtag()) < 0
                ? gameResume.getPlayer1().getUtag() + "_" + gameResume.getPlayer2().getUtag()
                : gameResume.getPlayer2().getUtag() + "_" + gameResume.getPlayer1().getUtag());
        }

        private boolean checkDeck(String deck) {
            return deck.length() == 16; 
        }
    }

    //Combiner
    public static class DataCombiner extends Reducer<Text, GameResume, Text, GameResume> {
        
        public static GameResume filterDateDuplicates(Iterable<GameResume> values){
            GameResume gameResume = null;
            Iterator<GameResume> it = values.iterator();
            if (it.hasNext()){
                gameResume = it.next().clone();
                while (it.hasNext()){
                    if (!gameResume.compareDate(it.next())){
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
    
    //Reduce
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