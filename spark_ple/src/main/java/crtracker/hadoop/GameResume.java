package crtracker.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

public class GameResume implements Writable, Cloneable {
    private String date; 
    private String game;
    private String mode;
    private int round;
    private String type;
    private int winner;
    private PlayerResume player1;
    private PlayerResume player2;

    // Constructeurs
    public GameResume() {}

    public GameResume(String date, String game, String mode, int round, String type, int winner, PlayerResume player1, PlayerResume player2) {
        this.date = date;
        this.game = game;
        this.mode = mode;
        this.round = round;
        this.type = type;
        this.winner = winner;
        this.player1 = player1;
        this.player2 = player2;
    }

    //Comparer à une autre game
    public boolean compareTo(GameResume other){
        if(this.game.equals(other.game) && this.mode.equals(other.mode) && this.round == other.round && this.type.equals(other.type) && compareDate(other.date) && comparePlayers(other.player1, other.player2)){
            return true;
        }
        return false;
    }

    public boolean compareDate(String otherDate) {
        try {
            Instant thisInstant = Instant.parse(this.date);
            Instant otherInstant = Instant.parse(otherDate);

            long differenceInSeconds = Math.abs(ChronoUnit.SECONDS.between(thisInstant, otherInstant));

            return differenceInSeconds <= 10;
        } catch (Exception e) {
            System.err.println("Erreur lors de la comparaison des dates : " + e.getMessage());
            return false;
        }
    }

    public boolean comparePlayers(PlayerResume other1, PlayerResume other2){
        if((this.player1.compareTo(other1) || this.player1.compareTo(other2)) && (this.player2.compareTo(other1) || this.player2.compareTo(other2))){
            return true;
        }
        return false;
    }

    // Implémentation de Writable
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(date);
        out.writeUTF(game);
        out.writeUTF(mode);
        out.writeInt(round);
        out.writeUTF(type);
        out.writeInt(winner);
        player1.write(out);
        player2.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        date = in.readUTF();
        game = in.readUTF();
        mode = in.readUTF();
        round = in.readInt();
        type = in.readUTF();
        winner = in.readInt();
        player1.readFields(in);
        player2.readFields(in);
    }

    @Override
    public String toString() {
        return "GameResume [date=" + date + ", game=" + game + ", mode=" + mode + ", round=" + round
                + ", type=" + type + ", winner=" + winner + ", player1=" + player1 + ", player2=" + player2 + "]";
    }
}
