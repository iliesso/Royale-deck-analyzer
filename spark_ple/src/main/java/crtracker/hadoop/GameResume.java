package crtracker.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;


@JsonIgnoreProperties(ignoreUnknown = true)
public class GameResume implements Writable, Cloneable {
    private String date = ""; //required 
    private String game = ""; //required
    private String mode = ""; //required
    private int round = -1;
    private String type = "";
    private int winner = -1;
    private PlayerResume player1; //required
    private PlayerResume player2; //required

    // Getters
    public PlayerResume getPlayer1() {
        return player1;
    }

    public PlayerResume getPlayer2() {
        return player2;
    }

    // Constructeurs
    public GameResume() {
        this.player1 = new PlayerResume();
        this.player2 = new PlayerResume();
    }

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
        return "date:" + date + ", game:" + game + ", mode:" + mode + ", round:" + round
                + ", type:" + type + ", winner:" + winner + ", players[{" + player1.toString() + "}, {" + player2.toString() + "]";
    }

    
    /*{
    "date":"2024-09-23T16:04:46Z","game":"pathOfLegend","mode":"Ranked1v1_NewArena","round":0,"type":"pathOfLegend","winner":1,
    "players":[
    {"utag":"#U82CQ9C8Q","ctag":"#QYPVC8RG","trophies":5498,"exp":32,"league":1,"bestleague":2,"deck":"00010512213c5b5c","evo":"","tower":"6e","strength":10.75,"crown":0,"elixir":12.41,"touch":1,"score":0},
    {"utag":"#8QRCGQJC","trophies":7109,"exp":43,"league":1,"bestleague":5,"deck":"080c111416235b66","evo":"08","tower":"70","strength":11.1875,"crown":1,"elixir":2.74,"touch":1,"score":0}
    ]
    }

    */

    }
