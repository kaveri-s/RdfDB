package basicpattern;

import global.*;
import heap.*;
import labelheap.*;

import java.io.IOException;

public class BasicPattern extends Tuple implements GlobalConst {
    private int numNodes;
    private float confidence;
    private EID [] nodeIDs;



    public BasicPattern(int size){
        super((size+1)*8 +4);
        numNodes =size;
        nodeIDs= new EID[numNodes];
    }

    public BasicPattern (byte[] abp, int offset) throws IOException{
        // ToDo: Set from byte array
        super(abp, offset, abp.length);
        this.numNodes = Convert.getIntValue(offset,abp);
        this.confidence=Convert.getFloValue(offset+4,abp);
        this.nodeIDs= new EID[numNodes];
        for(int i = 0; i< numNodes; i++){
            nodeIDs[i].slotNo=Convert.getIntValue(12+offset+(8*i),abp);
            nodeIDs[i].pageNo=new PageId(Convert.getIntValue(16+offset+(8*i),abp));
        }
    }

    public BasicPattern( BasicPattern fromBasicPattern) throws FieldNumberOutOfBoundException, IOException {
        super(fromBasicPattern.getBPByteArray(), 0, (fromBasicPattern.numNodes +1)*8 +4);
        this.numNodes =fromBasicPattern.numNodes;
        this.nodeIDs= new EID[numNodes];
        this.confidence= (float) fromBasicPattern.getConfidence();
        for(int i = 0; i< numNodes; i++){
            this.nodeIDs[i]=fromBasicPattern.getNodeID(i);
        }
    }

    public EID getNodeID(int fldNo) throws FieldNumberOutOfBoundException {
        if(fldNo>=0 && fldNo< numNodes){
            return nodeIDs[fldNo];
        }
        else{
            throw new FieldNumberOutOfBoundException (null, "BASICPATTERN:BASICPATTERN_FLDNO_OUT_OF_BOUND");
        }
    }

    public BasicPattern setNodeID(int fldNo,EID val) throws FieldNumberOutOfBoundException, IOException {
        if(fldNo>=0 && fldNo< numNodes){
            nodeIDs[fldNo]=val;
            int pos= (fldNo*8)+12;
            Convert.setIntValue(nodeIDs[fldNo].slotNo, pos, returnTupleByteArray());
            Convert.setIntValue(nodeIDs[fldNo].pageNo.pid, pos+4, returnTupleByteArray());
            return this;
        }
        else{
            throw new FieldNumberOutOfBoundException (null, "BASICPATTERN:BASICPATTERN_FLDNO_OUT_OF_BOUND");
        }
    }

    public EID[] getNodeIDs(){
        return this.nodeIDs;
    }

    public BasicPattern setNodeIDs(int length, EID[] vals) throws Exception, IOException {
        if(length==this.numNodes){
            for(int i = 0; i< numNodes; i++){
                setNodeID(i,vals[i]);
            }
            return this;
        }
        else{
            throw new Exception("Basic patterns are of different sizes");        }
    }

    public double getConfidence(){
        return this.confidence;
    }

    public BasicPattern setConfidence(double val) throws IOException {
        confidence= (float) val;
        Convert.setFloValue((float)confidence, 4, returnTupleByteArray());
        return this;
    }

    public byte[] getBPByteArray()throws IOException{
        return getTupleByteArray();
    }

    public int getNodeIDCount(){
        return this.numNodes;
    }

    public void BPCopy( BasicPattern fromBP) throws Exception {
        if(numNodes ==fromBP.numNodes){
            tupleCopy(fromBP);
            for(int i = 0; i< numNodes; i++){
                this.nodeIDs[i]=fromBP.getNodeID(i);
            }
            this.confidence= (float) fromBP.getConfidence();
        }
        else{
            throw new Exception("Basic patterns are of different sizes");
        }

    }

    public void print() throws IOException
    {
        System.out.print("[");
        try {
            for (int i = 0; i < numNodes; i++)
            {
                Label subject = SystemDefs.JavabaseDB.getEntityHandle().getLabel(this.nodeIDs[i].returnLID());
                System.out.printf("%30s  ",subject.getLabel());
            }
            System.out.print(getConfidence());
            System.out.println("]");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void printIDs()
    {
        System.out.print("[");
        try {
            for(int i = 0; i < numNodes; i++)
            {
                System.out.print("(" + this.nodeIDs[i].pageNo.pid + "," + this.nodeIDs[i].slotNo + ")");
            }
            System.out.print("Confidence:: " + getConfidence());
            System.out.println("]");

        } catch (Exception e) {
            System.out.println("Error printing BP"+e);
        }
    }
}
