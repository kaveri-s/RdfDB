/* File Quadruple.java */

package quadrupleheap;

import java.io.*;
import java.lang.*;
import global.*;
import heap.Tuple;
import labelheap.Label;
import labelheap.LabelHeapFile;

public class Quadruple extends Tuple implements GlobalConst {

    private EID subject;
    private PID predicate;
    private EID object;
    private float value;

    /**
     * Class constructor
     * Creat a new quadruple with length = max_size,quadruple offset = 0.
     */

    public Quadruple(){
        super(MINIBASE_QUADRUPLESIZE);
        subject= new EID();
        predicate=new PID();
        object=new EID();
    }

    /** Constructor
     * @param aquadruple a byte array which contains the quadruple
     */

    public Quadruple (byte[] aquadruple, int offset) throws IOException {
        super(aquadruple, offset, MINIBASE_QUADRUPLESIZE);
        subject=new EID();
        predicate=new PID();
        object=new EID();
        this.subject.slotNo=Convert.getIntValue(offset,aquadruple);
        this.subject.pageNo=new PageId(Convert.getIntValue(offset+4,aquadruple));
        this.predicate.slotNo=Convert.getIntValue(offset+8,aquadruple);
        this.predicate.pageNo=new PageId(Convert.getIntValue(offset+12,aquadruple));
        this.object.slotNo=Convert.getIntValue(offset+16,aquadruple);
        this.object.pageNo=new PageId(Convert.getIntValue(offset+20,aquadruple));
        this.value=Convert.getFloValue(offset+24,aquadruple);
    }

    /** Constructor(used as quadruple copy)
     * @param fromQuadruple   a byte array which contains the quadruple
     * @throws IOException
     */
    public Quadruple(Quadruple fromQuadruple) throws IOException{
        super(fromQuadruple.getQuadrupleByteArray(), 0, MINIBASE_QUADRUPLESIZE);
        subject=fromQuadruple.getSubjecqid();
        predicate=fromQuadruple.getPredicateID();
        object=fromQuadruple.getObjecqid();
        value= (float) fromQuadruple.getConfidence();
    }

    public EID getSubjecqid(){
        return subject;
    }

    public PID getPredicateID(){
        return predicate;
    }

    public EID getObjecqid(){
        return object;
    }

    public double getConfidence(){
        return value;
    }

    public Quadruple setSubjecqid(EID subjecqid) throws IOException {
        subject=subjecqid;
        Convert.setIntValue(subject.slotNo, 0, returnTupleByteArray());
        Convert.setIntValue(subject.pageNo.pid, 4, returnTupleByteArray());
        return this;
    }

    public Quadruple setPredicateid(PID predicateID) throws IOException {
        predicate=predicateID;
        Convert.setIntValue(predicate.slotNo, 8, returnTupleByteArray());
        Convert.setIntValue(predicate.pageNo.pid, 12, returnTupleByteArray());
        return this;
    }

    public Quadruple setObjecqid(EID objecqid) throws IOException {
        object=objecqid;
        Convert.setIntValue(object.slotNo, 16, returnTupleByteArray());
        Convert.setIntValue(object.pageNo.pid, 20, returnTupleByteArray());
        return this;
    }

    public Quadruple setConfidence(double confidence) throws IOException {
        value = (float)confidence;
        Convert.setFloValue((float)confidence, 24, returnTupleByteArray());
        return this;
    }

    public byte[] getQuadrupleByteArray()throws IOException{
        return getTupleByteArray();
    }

    public void print() throws Exception {
        double confidence = this.getConfidence();
        LabelHeapFile entityhandle = SystemDefs.JavabaseDB.getEntityHandle();
        LabelHeapFile predicatehandle = SystemDefs.JavabaseDB.getPredicateHandle();
        Label subject = entityhandle.getLabel(this.getSubjecqid().returnLID());
        Label object = entityhandle.getLabel(this.getObjecqid().returnLID());
        Label predicate = predicatehandle.getLabel(this.getPredicateID().returnLID());
        System.out.println(subject == null);
        System.out.println(subject.getLabel() +" "+ predicate.getLabel() + " " +object.getLabel()+ " " +confidence);
    }
    @Override
    public short size(){
        return MINIBASE_QUADRUPLESIZE;
    }

    /** Copy a quadruple to the current quadruple position
     * @param fromQuadruple the quadruple being copied
     */
    public void quadrupleCopy(Quadruple fromQuadruple)
    {
        tupleCopy(fromQuadruple);
        this.subject=fromQuadruple.subject;
        this.predicate=fromQuadruple.predicate;
        this.object=fromQuadruple.object;
        this.value=fromQuadruple.value;
    }

    /** This is used when you don't want to use the constructor
     * @param aquadruple  a byte array which contains the quadruple
     * @param offset the pos of quad in byte array
     */
    public void quadrupleInit(byte[] aquadruple, int offset) throws IOException {
        this.subject.slotNo=Convert.getIntValue(offset,aquadruple);
        this.subject.pageNo=new PageId(Convert.getIntValue(offset+4,aquadruple));
        this.predicate.slotNo=Convert.getIntValue(offset+8,aquadruple);
        this.predicate.pageNo=new PageId(Convert.getIntValue(offset+12,aquadruple));
        this.object.slotNo=Convert.getIntValue(offset+16,aquadruple);
        this.object.pageNo=new PageId(Convert.getIntValue(offset+20,aquadruple));
        this.value=Convert.getFloValue(offset+24,aquadruple);
    }

    /**
     * Set a quadruple with the given quadruple length and offset
     * @param aquadruple  a byte array which contains the quadruple
     * @param offset the pos of quad in byte array
     */
    public void quadrupleSet(byte[] aquadruple, int offset)throws IOException {
        this.subject.slotNo=Convert.getIntValue(offset,aquadruple);
        this.subject.pageNo=new PageId(Convert.getIntValue(offset+4,aquadruple));
        setSubjecqid(this.subject);
        this.predicate.slotNo=Convert.getIntValue(offset+8,aquadruple);
        this.predicate.pageNo=new PageId(Convert.getIntValue(offset+12,aquadruple));
        setPredicateid(this.predicate);
        this.object.slotNo=Convert.getIntValue(offset+16,aquadruple);
        this.object.pageNo=new PageId(Convert.getIntValue(offset+20,aquadruple));
        setObjecqid(this.object);
        this.value=Convert.getFloValue(offset+24,aquadruple);
        setConfidence(this.value);

    }

}

