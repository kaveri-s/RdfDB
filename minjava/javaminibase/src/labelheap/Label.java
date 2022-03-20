package labelheap;

import java.io.*;
import java.lang.*;
import global.*;
import heap.Tuple;

public class Label extends Tuple {

    private LID ID;
    private String name;
    public Label() {
        super();
        ID= new LID();
        name="";
    }

    public Label (byte[] alabel, int offset, int length) throws IOException {
        super(alabel, offset, length);
        this.ID.slotNo=Convert.getIntValue(offset,alabel);
        this.ID.pageNo=new PageId(Convert.getIntValue(offset+4,alabel));
        this.name = Convert.getStrValue(offset+8, alabel, length - 8);
    }

    public String getLabel() {
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
