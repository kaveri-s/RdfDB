package labelheap;

import java.io.*;
import java.lang.*;
import global.*;
import heap.Tuple;

public class Label extends Tuple {

    private String name;

    public Label() {
        super();
        name="";
    }

    public Label (byte[] alabel, int offset, int length) throws IOException {
        super(alabel, offset, length);
        this.name = new String(alabel);
    }

    public String getLabel() {
        return name;
    }

    public Label setLabel(String label) {
        this.name=label;
        return this;
    }

    public void print() {
        System.out.println(name);
    }
}
