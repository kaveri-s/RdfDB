package heap;

import java.io.*;
import java.lang.*;
import global.*;

public class Quadruple implements GlobalConst {

    private EID subject;
    private PID predicate;
    private EID object;
    private double value;

    public Quadruple(){
        subject= new EID();
        predicate=new PID();
        object=new EID();
    }

    public Quadruple(byte[] aquadruple, int offset){

    }

    public Quadruple(Quadruple fromQuadruple){
        subject=fromQuadruple.getSubjecqid();
        predicate=fromQuadruple.getPredicateID();
        object=fromQuadruple.getObjecqid();
        //value=fromQuadruple.getgetConfidence();
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

    public byte[] getQuadrupleByteArray(){

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

    public void size(){

    }


    public void quadrupleCopy(Quadruple fromQuadruple){
        this.subject=fromQuadruple.subject;
        this.predicate=fromQuadruple.predicate;
        this.object=fromQuadruple.object;
        this.value=fromQuadruple.value;
    }

    public void quadrupleInit(byte[] aquadruple, int offset){

    }

    public void quadrupleSet(byte[] aquadruple, int offset){

    }
}
