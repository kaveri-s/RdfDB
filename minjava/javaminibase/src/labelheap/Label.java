package labelheap;

import java.io.*;
import java.lang.*;
import global.*;

public class Label {

    private LID ID;
    private String name;
    Label() {
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
