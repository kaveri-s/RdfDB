package labelheap;

import java.io.*;
import java.lang.*;
import global.*;
import heap.Tuple;

public class Label extends Tuple {

    private LID ID;
    private String name;
    Label() {
        super(5);
        ID= new LID();
        name="";
    }

    String getLabel() {
        return name;
    }

    Label setLabel(String label) {
        this.name=label;
        return this;
    }

    void print() {
        System.out.println(ID);
        System.out.println(name);
    }
}
