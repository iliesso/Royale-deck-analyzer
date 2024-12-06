package crtracker.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

public class GameResume implements Writable, Cloneable {
    private long date; // Stockée en millisecondes depuis l'époque
    private Text game;
    private Text mode;
    private IntWritable round;
    private Text type;
    private IntWritable winner;
    private PlayerResume player1;
    private PlayerResume player2;

    // Constructeurs
    public GameResume() {
        this.game = new Text();
        this.mode = new Text();
        this.round = new IntWritable();
        this.type = new Text();
        this.winner = new IntWritable();
        this.player1 = new PlayerResume();
        this.player2 = new PlayerResume();
    }

    public GameResume(Date date, String game, String mode, int round, String type, int winner, PlayerResume player1, PlayerResume player2) {
        this.date = date.getTime();
        this.game = new Text(game);
        this.mode = new Text(mode);
        this.round = new IntWritable(round);
        this.type = new Text(type);
        this.winner = new IntWritable(winner);
        this.player1 = player1;
        this.player2 = player2;
    }

    //Comparer à une autre game
    public boolean CompareTo(GameResume other){
        if(this.game.equals(other.game) && this.mode.equals(other.mode) && this.round.equals(other.round) && this.type.equals(other.type) && this.winner.equals(other.winner) && this.player1.CompareTo(other.player1) && this.player2.CompareTo(other.player2)){
            return true;
        }
        return false;
    }

    // Implémentation de Writable
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(date);
        game.write(out);
        mode.write(out);
        round.write(out);
        type.write(out);
        winner.write(out);
        player1.write(out);
        player2.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        date = in.readLong();
        game.readFields(in);
        mode.readFields(in);
        round.readFields(in);
        type.readFields(in);
        winner.readFields(in);
        player1.readFields(in);
        player2.readFields(in);
    }

    @Override
    public String toString() {
        return "GameResume [date=" + new Date(date) + ", game=" + game + ", mode=" + mode + ", round=" + round
                + ", type=" + type + ", winner=" + winner + ", player1=" + player1 + ", player2=" + player2 + "]";
    }
}
