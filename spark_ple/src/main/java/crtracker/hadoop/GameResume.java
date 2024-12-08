package crtracker.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

public class GameResume implements Writable, Cloneable {
    private String date;  
    private String game;  
    private String mode;  
    private int round;
    private String type;
    private int winner;
    private PlayerResume[] players;

    // Getters
    public PlayerResume[] getPlayers() {
        return players;
    }

    public PlayerResume getPlayer1() {
        return players[0];
    }

    public PlayerResume getPlayer2() {
        return players[1];
    }

    public String getDate() {
        return date;
    }

    public String getMode() {
        return mode;
    }

    public int getRound(){
        return round;
    }

    public GameResume clone(){
        try {
            return (GameResume)super.clone();
        }
        catch (Exception e) {
            System.err.println(e.getStackTrace());
            System.exit(-1);
        }
        return null;
    }



    // Constructeurs
    public GameResume() {
        PlayerResume player1 = new PlayerResume();
        PlayerResume player2 = new PlayerResume();
        this.players = new PlayerResume[]{player1, player2};
    }

    public GameResume(String date, String game, String mode, int round, String type, int winner, PlayerResume player1,
            PlayerResume player2) {
        this.date = date;
        this.game = game;
        this.mode = mode;
        this.round = round;
        this.type = type;
        this.winner = winner;
        this.players = new PlayerResume[]{player1, player2};
    }

    // Comparer à une autre game
    public boolean compareTo(GameResume other) {
        if (this.game.equals(other.game) && this.mode.equals(other.mode) && this.round == other.round
                && this.type.equals(other.type) && compareDate(other.date)
                && comparePlayers(other.players[0], other.players[1])) {
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

    public boolean comparePlayers(PlayerResume other1, PlayerResume other2) {
        return ((this.players[0].compareTo(other1) || this.players[0].compareTo(other2))
                && (this.players[1].compareTo(other1) || this.players[1].compareTo(other2)));
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
        players[0].write(out);
        players[1].write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        date = in.readUTF();
        game = in.readUTF();
        mode = in.readUTF();
        round = in.readInt();
        type = in.readUTF();
        winner = in.readInt();
        players[0].readFields(in);
        players[1].readFields(in);
    }

    @Override
    public String toString() {
        return "date:" + date + ", game:" + game + ", mode:" + mode + ", round:" + round
                + ", type:" + type + ", winner:" + winner + ", players[{" + players[0].toString() + "}, {"
                + players[1].toString() + "}]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GameResume other = (GameResume) o;
        return round == other.round
            && winner == other.winner
            && Objects.equals(date, other.date)
            && Objects.equals(game, other.game)
            && Objects.equals(mode, other.mode)
            && Objects.equals(type, other.type)
            && Objects.equals(players[0], other.players[0])
            && Objects.equals(players[1], other.players[1]);
    }
    @Override
    public int hashCode() {
        return Objects.hash(game, mode, round, type, date, players[0], players[1]);
    }

    /*
     * {
     * "date":"2024-09-23T16:04:46Z","game":"pathOfLegend","mode":
     * "Ranked1v1_NewArena","round":0,"type":"pathOfLegend","winner":1,
     * "players":[
     * {"utag":"#U82CQ9C8Q","ctag":"#QYPVC8RG","trophies":5498,"exp":32,"league":1,
     * "bestleague":2,"deck":"00010512213c5b5c","evo":"","tower":"6e","strength":10.
     * 75,"crown":0,"elixir":12.41,"touch":1,"score":0},
     * {"utag":"#8QRCGQJC","trophies":7109,"exp":43,"league":1,"bestleague":5,"deck"
     * :"080c111416235b66","evo":"08","tower":"70","strength":11.1875,"crown":1,
     * "elixir":2.74,"touch":1,"score":0}
     * ]
     * }
     * 
     */

}
