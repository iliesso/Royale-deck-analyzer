package crtracker.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PlayerResume implements Writable, Cloneable {

    @JsonProperty("utag")
    private String utag;

    @JsonProperty("ctag")
    private String ctag;

    @JsonProperty("trophies")
    private int trophies;

    @JsonProperty("exp")
    private int exp;

    @JsonProperty("league")
    private int league;

    @JsonProperty("bestleague")
    private int bestleague;

    @JsonProperty("deck")
    private String deck;

    @JsonProperty("evo")
    private String evo;

    @JsonProperty("tower")
    private String tower;

    @JsonProperty("strength")
    private double strength;

    @JsonProperty("crown")
    private int crown;

    @JsonProperty("elixir")
    private double elixir;

    @JsonProperty("touch")
    private int touch;

    @JsonProperty("score")
    private int score;

    public PlayerResume() {
    }

    public String getUtag() {
        return utag;
    }

    public void setUtag(String utag) {
        this.utag = utag;
    }

    public String getCtag() {
        return ctag;
    }

    public void setCtag(String ctag) {
        this.ctag = ctag;
    }

    public int getTrophies() {
        return trophies;
    }

    public void setTrophies(int trophies) {
        this.trophies = trophies;
    }

    public int getExp() {
        return exp;
    }

    public void setExp(int exp) {
        this.exp = exp;
    }

    public int getLeague() {
        return league;
    }

    public void setLeague(int league) {
        this.league = league;
    }

    public int getBestleague() {
        return bestleague;
    }

    public void setBestleague(int bestleague) {
        this.bestleague = bestleague;
    }

    public String getDeck() {
        return deck;
    }

    public void setDeck(String deck) {
        this.deck = deck;
    }

    public String getEvo() {
        return evo;
    }

    public void setEvo(String evo) {
        this.evo = evo;
    }

    public String getTower() {
        return tower;
    }

    public void setTower(String tower) {
        this.tower = tower;
    }

    public double getStrength() {
        return strength;
    }

    public void setStrength(double strength) {
        this.strength = strength;
    }

    public int getCrown() {
        return crown;
    }

    public void setCrown(int crown) {
        this.crown = crown;
    }

    public double getElixir() {
        return elixir;
    }

    public void setElixir(double elixir) {
        this.elixir = elixir;
    }

    public int getTouch() {
        return touch;
    }

    public void setTouch(int touch) {
        this.touch = touch;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(utag == null ? "" : utag);
        out.writeUTF(ctag == null ? "" : ctag);
        out.writeInt(trophies);
        out.writeInt(exp);
        out.writeInt(league);
        out.writeInt(bestleague);
        out.writeUTF(deck == null ? "" : deck);
        out.writeUTF(evo == null ? "" : evo);
        out.writeUTF(tower == null ? "" : tower);
        out.writeDouble(strength);
        out.writeInt(crown);
        out.writeDouble(elixir);
        out.writeInt(touch);
        out.writeInt(score);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        utag = in.readUTF();
        ctag = in.readUTF();
        trophies = in.readInt();
        exp = in.readInt();
        league = in.readInt();
        bestleague = in.readInt();
        deck = in.readUTF();
        evo = in.readUTF();
        tower = in.readUTF();
        strength = in.readDouble();
        crown = in.readInt();
        elixir = in.readDouble();
        touch = in.readInt();
        score = in.readInt();
    }

    public PlayerResume clone() {
        try {
            return (PlayerResume) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "\"utag\":\"" + utag + "\", \"ctag\":\"" + ctag + "\", \"trophies\":" + trophies + ", \"exp\":" + exp
                + ", \"league\":" + league + ", \"bestleague\":" + bestleague + ", \"deck\":\"" + deck
                + "\", \"evo\":\"" + evo
                + "\", \"tower\":\"" + tower + "\", \"strength\":" + strength + ", \"crown\":" + crown + ", \"elixir\":"
                + elixir
                + ", \"touch\":" + touch + ", \"score\":" + score;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof PlayerResume))
            return false;
        PlayerResume that = (PlayerResume) o;
        return trophies == that.trophies
                && exp == that.exp
                && league == that.league
                && bestleague == that.bestleague
                && Double.compare(that.strength, strength) == 0
                && crown == that.crown
                && Double.compare(that.elixir, elixir) == 0
                && touch == that.touch
                && score == that.score
                && Objects.equals(utag, that.utag)
                && Objects.equals(ctag, that.ctag)
                && Objects.equals(deck, that.deck)
                && Objects.equals(evo, that.evo)
                && Objects.equals(tower, that.tower);
    }

    @Override
    public int hashCode() {
        return Objects.hash(utag, ctag, trophies, exp, league, bestleague, deck, evo, tower, strength, crown, elixir,
                touch, score);
    }
}
