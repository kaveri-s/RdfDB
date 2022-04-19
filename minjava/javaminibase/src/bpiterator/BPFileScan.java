package bpiterator;
   

import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;


import java.lang.*;
import java.io.*;

import basicpattern.*;
import iterator.*;

/**
 *open a heapfile and according to the condition expression to get
 *output file, call get_next to get all tuples
 */
public class BPFileScan extends  BPIterator
{
  private AttrType[] _in1;
  private short in1_len;
  private short[] s_sizes; 
  private Heapfile f;
  private Scan scan;
  private Tuple     tuple1;
  private int        t1_size;
  private int nOutFlds;
  private CondExpr[]  OutputFilter;
  public FldSpec[] perm_mat;

 

  /**
   *constructor
   *@param f heapfile to be opened
   *@param n_out_flds  number of fields in the out tuple
   *@exception IOException some I/O fault
   *@exception FileScanException exception from this class
   *@exception TupleUtilsException exception from this class
   *@exception InvalidRelation invalid relation 
   */
  public  BPFileScan (Heapfile f, int n_out_flds)
    throws IOException,
          FileScanException,
          BPUtilsException,
          InvalidRelation
    {
//      tuple1 =  new Tuple();
//      in1_len = ((n_out_flds - 1) * 2);
//      _in1 = new AttrType[in1_len +1];
//      for(int i = 0; i < in1_len; i++)
//      {
//        _in1[i] = new AttrType(AttrType.attrInteger);
//      }
//      _in1[in1_len] = new AttrType(AttrType.attrFloat);
//      s_sizes = new short[1];
//      s_sizes[0] = ((in1_len * 4) + (1 * 8));
//      try {
//        tuple1.setHdr(in1_len + 1, _in1, s_sizes);
//      } catch (InvalidTypeException e1) {
//        e1.printStackTrace();
//      } catch (InvalidTupleSizeException e1) {
//        e1.printStackTrace();
//      }

      try {
	scan = f.openScan();
      }
      catch(Exception e){
	throw new FileScanException(e, "openScan() failed");
      }
    }
  
  
  /**
   *@return the result tuple
   *@exception JoinsException some join exception
   *@exception IOException I/O errors
   *@exception InvalidTupleSizeException invalid tuple size
   *@exception InvalidTypeException tuple type not valid
   *@exception PageNotReadException exception from lower layer
   *@exception PredEvalException exception from PredEval class
   *@exception UnknowAttrType attribute type unknown
   *@exception FieldNumberOutOfBoundException array out of bounds
   *@exception WrongPermat exception for wrong FldSpec argument
   */
  public BasicPattern get_next()
    throws JoinsException,
	   IOException,
	   InvalidTupleSizeException,
	   InvalidTypeException,
	   PageNotReadException, 
	   PredEvalException,
	   UnknowAttrType,
	   FieldNumberOutOfBoundException,
          WrongPermat
    {     
      RID rid = new RID();;
      
      while(true) {
        if((tuple1 =  scan.getNext(rid)) == null) {
          return null;
        }
//        tuple1.setHdr(in1_len + 1, _in1, s_sizes);
//        short bp_flds = tuple1.noOfFlds();

        BasicPattern bp = new BasicPattern(tuple1.getTupleByteArray(), 0);
//        int bp_flds = bp.getNodeIDCount();
//        try {
//          bp.setHdr((bp_flds / 2) + 1);
//        } catch (InvalidBasicPatternSizeException e) {
//          e.printStackTrace();
//        }
//        int j = 1;
//        for (int i = 0; i < bp_flds; i++)
//        {
//          int slotno = bp.getNodeID(bp_flds).slotNo;
//          PageId pageno = bp.getNodeID(bp_flds).pageNo;
//
//          LID lid = new LID(pageno, slotno);
//          EID eid = lid.returnEID();
//          bp.setNodeID(i+1, eid);
//        }
//        float lConfidence = tuple1.getFloFld(j);
//        bp.setConfidence(lConfidence);
        return bp;
      }
    }

  /**
   *implement the abstract method close() from super class Iterator
   *to finish cleaning up
   */
  public void close() 
    {
     
      if (!closeFlag) {
	scan.closescan();
	closeFlag = true;
      } 
    }
  
}


