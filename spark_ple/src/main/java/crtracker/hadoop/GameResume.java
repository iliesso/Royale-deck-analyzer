package crtracker.hadoop;

import org.apache.hadoop.io.Writable;

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

    public GameResume() {
        players = new PlayerResume[] { new PlayerResume(), new PlayerResume() };
    }

    // Getters & Setters
    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getGame() {
        return game;
    }

    public void setGame(String game) {
        this.game = game;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public int getRound() {
        return round;
    }

    public void setRound(int round) {
        this.round = round;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getWinner() {
        return winner;
    }

    public void setWinner(int winner) {
        this.winner = winner;
    }

    public PlayerResume[] getPlayers() {
        return players;
    }

    public void setPlayers(PlayerResume[] players) {
        this.players = players;
        if (players != null && players.length == 2 && players[0].getUtag() != null && players[1].getUtag() != null) {
            if (players[0].getUtag().compareTo(players[1].getUtag()) > 0) {
                PlayerResume temp = players[0];
                players[0] = players[1];
                players[1] = temp;
            }
        }
    }

    public PlayerResume getPlayer1() {
        return players[0];
    }

    public PlayerResume getPlayer2() {
        return players[1];
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

    @Override
    public int hashCode() {
        // Normalisation de la date sur un intervalle de 10 secondes
        long normalizedTime = 0;
        try {
            Instant thisInstant = Instant.parse(this.date);
            long epochSeconds = thisInstant.getEpochSecond();
            normalizedTime = (epochSeconds / 10L) * 10L; // arrondi à la dizaine de secondes
        } catch (Exception e) {
            // En cas de problème de parsing, normalizedTime reste 0
        }

        return Objects.hash(game, mode, round, type, normalizedTime, players[0], players[1]);
    }
}