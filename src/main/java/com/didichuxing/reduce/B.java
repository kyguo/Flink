package com.didichuxing.reduce;

public class B {
    public String word;
    public Integer counts;
    public B(){};
    public B(String word,Integer counts){
        this.word = word;
        this.counts = counts;
    }

    @Override
    public String toString() {
        return "B( " + word + ", " + counts + " )";
    }
}
