/* File Quadruple.java */

package heap;

import java.io.*;
import java.lang.*;
import global.*;


//add value fld
//global length?
public class Quadruple implements GlobalConst {

    private EID subject;
    private PID predicate;
    private EID object;
    private double value;

    /**
     * Class constructor
     * Creat a new quadruple with length = max_size,quadruple offset = 0.
     */

    public Quadruple(){
        subject= new EID();
        predicate=new PID();
        object=new EID();
    }

    /** Constructor
     * @param aquadruple a byte array which contains the quadruple
     */

    public Quadruple (byte[] aquadruple, int offset) throws IOException {
        this.subject.slotNo=Convert.getIntValue(offset,aquadruple);
        this.subject.pageNo=new PageId(Convert.getIntValue(offset+4,aquadruple));
        this.predicate.slotNo=Convert.getIntValue(offset+8,aquadruple);
        this.predicate.pageNo=new PageId(Convert.getIntValue(offset+12,aquadruple));
        this.object.slotNo=Convert.getIntValue(offset+16,aquadruple);
        this.object.pageNo=new PageId(Convert.getIntValue(offset+20,aquadruple));
        //add value fld
    }

    /** Constructor(used as quadruple copy)
     * @param fromQuadruple   a byte array which contains the quadruple
     */
    public Quadruple(Quadruple fromQuadruple){
        subject=fromQuadruple.getSubjecqid();
        predicate=fromQuadruple.getPredicateID();
        object=fromQuadruple.getObjecqid();
        value=fromQuadruple.getConfidence();
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

    public Quadruple setSubjecqid(EID subjecqid){
        subject=subjecqid;
        return this;
    }

    public Quadruple setPredicateid(PID predicateID){
        predicate=predicateID;
        return this;
    }

    public Quadruple setObjecqid(EID objecqid){
        object=objecqid;
        return this;
    }

    public Quadruple setConfidence(double confidence){
        value=confidence;
        return this;
    }

    public byte[] getQuadrupleByteArray()throws IOException{

        byte[] quadruplecopy= new byte[28];
        Convert.setIntValue (subject.slotNo, 0, quadruplecopy);
        Convert.setIntValue(subject.pageNo.pid,4,quadruplecopy);
        Convert.setIntValue (predicate.slotNo, 8, quadruplecopy);
        Convert.setIntValue(predicate.pageNo.pid,12,quadruplecopy);
        Convert.setIntValue (object.slotNo, 16, quadruplecopy);
        Convert.setIntValue(object.pageNo.pid,20,quadruplecopy);
        //add value fld
        return quadruplecopy;
    }

    public void print(){
        System.out.print(subject.pageNo);
        System.out.print(subject.slotNo);
        System.out.print(", ");
        System.out.print(predicate.pageNo);
        System.out.print(predicate.slotNo);
        System.out.print(", ");
        System.out.print(object.pageNo);
        System.out.print(object.slotNo);
        System.out.print(", ");
        System.out.print(value);
    }

    public int size(){
        return MINIBASE_QUADRUPLESIZE;
    }

    /** Copy a quadruple to the current quadruple position
     *  you must make sure the quadruple lengths must be equal
     * @param fromQuadruple the quadruple being copied
     */
    public void quadrupleCopy(Quadruple fromQuadruple)
    {
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
        //add value fld
    }

    /**
     * Set a quadruple with the given quadruple length and offset
     * @param aquadruple  a byte array which contains the quadruple
     * @param offset the pos of quad in byte array
     */
    public void quadrupleSet(byte[] aquadruple, int offset)throws IOException {
        this.subject.slotNo=Convert.getIntValue(offset,aquadruple);
        this.subject.pageNo=new PageId(Convert.getIntValue(offset+4,aquadruple));
        this.predicate.slotNo=Convert.getIntValue(offset+8,aquadruple);
        this.predicate.pageNo=new PageId(Convert.getIntValue(offset+12,aquadruple));
        this.object.slotNo=Convert.getIntValue(offset+16,aquadruple);
        this.object.pageNo=new PageId(Convert.getIntValue(offset+20,aquadruple));
        //add value fld
    }

}

