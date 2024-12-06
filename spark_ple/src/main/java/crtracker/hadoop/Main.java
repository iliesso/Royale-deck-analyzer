package crtracker.hadoop;

import org.apache.hadoop.util.ProgramDriver;

import crtracker.hadoop.DataCleaner;

public class Main {
    public static void main(String[] args) throws Exception {
        ProgramDriver pgd = new ProgramDriver();
        int exitCode = -1;
        try {
            pgd.addClass("filter", crtracker.hadoop.DataCleaner.class, "filter cities");
            exitCode = pgd.run(args);
        } catch (Throwable e1) {
            e1.printStackTrace();
        }
        System.exit(exitCode);
    }
}
