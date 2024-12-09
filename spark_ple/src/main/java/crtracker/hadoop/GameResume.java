package crtracker.hadoop;

import org.apache.hadoop.io.Writable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GameResume implements Writable, Cloneable {
    @JsonProperty("date")
    private String date;

    @JsonProperty("game")
    private String game;

    @JsonProperty("mode")
    private String mode;

    @JsonProperty("round")
    private int round;

    @JsonProperty("type")
    private String type;

    @JsonProperty("winner")
    private int winner;

    @JsonProperty("players")
    private PlayerResume[] players;

    public PlayerResume[] getPlayers() {
        return players;
    }

    public PlayerResume getPlayer1() {
        return players[0];
    }

    public PlayerResume getPlayer2() {
        return players[1];
    }

    public String getGame() {
        return game;
    }

    public String getMode() {
        return mode;
    }

    public int getRound(){
        return round;
    }


    public Instant getDateAsInstant() {
        return Instant.parse(this.date); 
        // en supposant que la date soit au format ISO-8601, sinon adapter le parsing
    }
    
    public String toJsonString() {
        // Convertir l'objet en JSON (utiliser un ObjectMapper Jackson par exemple)
        // ou implémenter votre propre logique
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return "";
        }
    }
    
    public GameResume clone() {
        try {
            GameResume copy = (GameResume) super.clone();
            PlayerResume p1copy = players[0].clone();
            PlayerResume p2copy = players[1].clone();
            copy.players = new PlayerResume[] { p1copy, p2copy };
            return copy;
        } catch (Exception e) {
            System.err.println(e.getStackTrace());
            System.exit(-1);
        }
        return null;
    }

    // Constructeurs
    public GameResume() {
        players = new PlayerResume[] { new PlayerResume(), new PlayerResume() };
    }

    public GameResume(String date, String game, String mode, int round, String type, int winner, PlayerResume player1,
            PlayerResume player2) {
        this.date = date;
        this.game = game;
        this.mode = mode;
        this.round = round;
        this.type = type;
        this.winner = winner;
        this.players[0] = player1;
        this.players[1] = player2;
    }
    
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
        return "{\"date\":\"" + date + "\", \"game\":\"" + game + "\", \"mode\":\"" + mode + "\", \"round\":" + round
                + ", \"type\":\"" + type + "\", \"winner\":" + winner + ", \"players\":[{" + players[0].toString()
                + "}, {"
                + players[1].toString() + "}]}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof GameResume))
            return false;
        GameResume other = (GameResume) o;

        // Comparaison des champs simples
        if (this.round != other.round || this.winner != other.winner)
            return false;
        if (!Objects.equals(this.game, other.game))
            return false;
        if (!Objects.equals(this.mode, other.mode))
            return false;
        if (!Objects.equals(this.type, other.type))
            return false;

        // Comparaison des joueurs (déjà triés par setPlayers)
        if (!Objects.equals(this.players[0], other.players[0]))
            return false;
        if (!Objects.equals(this.players[1], other.players[1]))
            return false;

        // Comparaison des dates avec une tolérance de 10 secondes
        try {
            Instant thisInstant = Instant.parse(this.date);
            Instant otherInstant = Instant.parse(other.date);
            long differenceInSeconds = Math.abs(ChronoUnit.SECONDS.between(thisInstant, otherInstant));
            if (differenceInSeconds > 10) {
                return false;
            }
        } catch (Exception e) {
            // Si la date ne peut être parsée, on considère que ce n'est pas égal
            return false;
        }

        return true;
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
