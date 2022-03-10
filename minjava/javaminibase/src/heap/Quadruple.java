/* File Quadruple.java */

package heap;

import java.io.*;
import java.lang.*;
import global.*;


public class Quadruple implements GlobalConst{

    private EID subject;
    private PID predicate;
    private EID object;
    private double value;

    /**
     * Maximum size of any quadruple
     */
    public static final int max_size = MINIBASE_PAGESIZE;

    /**
     * a byte array to hold data
     */
    private byte [] data;

    /**
     * length of this quadruple
     */
    private int quadruple_length;

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

    public Quadruple(byte [] aquadruple, int length)
    {
        data = aquadruple;
        quadruple_length = length;
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

    /**
     * Class constructor
     * Creat a new quadruple with length = size,quadruple offset = 0.
     */

    public Quadruple(int size)
    {
       // Create a new quadruple
       data = new byte[size];
       quadruple_length = size;
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
     * @param length the length of the quadruple
     */

    public void quadrupleInit(byte [] aquadruple, int length)
    {
        data = aquadruple;
        quadruple_length = length;
    }

    /**
     * Set a quadruple with the given quadruple length and offset
     * @param	record	a byte array contains the quadruple
     * @param	length	the length of the quadruple
     */
    public void quadrupleSet(byte [] record, int length)
    {
        System.arraycopy(record, 0, data, 0, length);
        quadruple_length = length;
    }

    public void size(){

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

    /** get the length of a quadruple, call this method if you did not
     *  call setHdr () before
     * @return 	length of this quadruple in bytes
     */
    public int getLength()
    {
        return quadruple_length;
    }

    public byte [] getQuadrupleByteArray()
    {
        byte [] quadruplecopy = new byte [quadruple_length];
        System.arraycopy(data, 0, quadruplecopy, 0, quadruple_length);
        return quadruplecopy;
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

    /**
     * Print out the quadruple
     * @Exception IOException I/O exception
     */
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

}

