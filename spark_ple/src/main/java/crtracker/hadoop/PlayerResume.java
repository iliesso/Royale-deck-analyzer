package crtracker.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class PlayerResume implements Writable, Cloneable {
    private String utag;
    private String ctag;
    private int trophies;
    private int exp;
    private int league;
    private int bestleague;
    private String deck;
    private String evo;
    private String tower;
    private double strength;
    private int crown;
    private double elixir;
    private int touch;
    private int score;

    public PlayerResume() {
    }

    public PlayerResume(String utag, String ctag, int trophies, int exp, int league, int bestleague, String deck,
            String evo, String tower, double strength, int crown, double elixir, int touch, int score) {
        this.utag = utag;
        this.ctag = ctag;
        this.trophies = trophies;
        this.exp = exp;
        this.league = league;
        this.bestleague = bestleague;
        this.deck = deck;
        this.evo = evo;
        this.tower = tower;
        this.strength = strength;
        this.crown = crown;
        this.elixir = elixir;
        this.touch = touch;
        this.score = score;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(utag);
        out.writeUTF(ctag);
        out.writeInt(trophies);
        out.writeInt(exp);
        out.writeInt(league);
        out.writeInt(bestleague);
        out.writeUTF(deck);
        out.writeUTF(evo);
        out.writeUTF(tower);
        out.writeDouble(strength);
        out.writeInt(crown);
        out.writeDouble(elixir);
        out.writeInt(touch);
        out.writeInt(score);
    }

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

    public String getDeck() {
        return deck;
    }

    public String getUtag() {
        return utag;
    }

    @Override
    public PlayerResume clone() {
        try {
            return (PlayerResume) super.clone();
        } catch (Exception e) {
            System.err.println(e.getStackTrace());
            System.exit(-1);
        }
        return null;
    }

    @Override
    public String toString() {
        return "utag:" + utag + ", ctag:" + ctag + ", trophies:" + trophies + ", exp:" + exp
                + ", league:" + league + ", bestleague:" + bestleague + ", deck:" + deck + ", evo:" + evo
                + ", tower:" + tower + ", strength:" + strength + ", crown:" + crown + ", elixir:" + elixir
                + ", touch:" + touch + ", score:" + score;
    }

    public boolean compareTo(PlayerResume player1) {
        if (this.utag.equals(player1.utag) && this.deck.equals(player1.deck) && this.evo.equals(player1.evo)
                && this.tower.equals(player1.tower)) {
            return true;
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
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
        return Objects.hash(utag, ctag, trophies, exp, league, bestleague, deck, evo, tower, strength, crown, elixir, touch, score);
    }
}