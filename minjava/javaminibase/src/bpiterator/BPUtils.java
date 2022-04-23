package bpiterator;


import heap.*;
import global.*;
import java.io.*;

import basicpattern.BasicPattern;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.LabelUtils;
import labelheap.*;

/**
 *some useful method when processing Tuple 
 */
public class BPUtils
{
  
  /**
   * This function compares a tuple with another tuple in respective field, and
   *  returns:
   *
   *    0        if the two are equal,
   *    1        if the tuple is greater,
   *   -1        if the tuple is smaller,
   *
   *@param    b1        one basicpattern.
   *@param    b2        another basicpattern.
   *@param    b1_fld_no the field numbers in the basicpatterns to be compared.
   *@param    b2_fld_no the field numbers in the basicpatterns to be compared. 
   *@exception UnknowAttrType don't know the attribute type
   *@exception IOException some I/O fault
   *@exception TupleUtilsException exception from this class
   *@exception FieldNumberOutOfBoundException
   *@return   0        if the two are equal,
   *          1        if the basicpattern is greater,
   *         -1        if the basicpattern is smaller,                              
   */
  public static int CompareBPWithBP(
    BasicPattern b1, int b1_fld_no,
    BasicPattern b2, int b2_fld_no)
    throws IOException, 
    UnknowAttrType, 
    TupleUtilsException, 
    FieldNumberOutOfBoundException,
    InvalidSlotNumberException, 
    InvalidTupleSizeException, 
    HFException, 
    HFDiskMgrException, 
    HFBufMgrException, 
    Exception
    {
      int b1_n = b1.getNodeIDCount();
      int b2_n = b2.getNodeIDCount();

      if ((b1_fld_no > 0) && (b1_fld_no <= b1_n)
      && (b2_fld_no > 0) && (b2_fld_no <= b2_n))
      {
        b1_fld_no = b1_fld_no - 1;
        b2_fld_no = b2_fld_no - 1;
        EID e1 = b1.getNodeID(b1_fld_no);
        EID e2 = b2.getNodeID(b2_fld_no);
        if (e1 == e2)
        {
          return 0;
        }
        else if (e1.pageNo.pid == -1)
        {
          return -1;
        }
        else if (e2.pageNo.pid == -1)
        {
          return 1;
        }
        else if (e1.pageNo.pid == -2)
        {
          return 1;
        }
        else if (e2.pageNo.pid == -2)
        {
          return -1;
        }
        Label ll1 = SystemDefs.JavabaseDB.getEntityHandle().getLabel(e1.returnLID());
        Label ll2 = SystemDefs.JavabaseDB.getEntityHandle().getLabel(e2.returnLID());
        return LabelUtils.CompareLabelWithLabel(ll1, ll2);
      }
      else if ((b1_fld_no == (b1_n + 1)) && (b2_fld_no == (b2_n + 1)))
      {
        float b1_c = (float) b1.getConfidence();
        float b2_c = (float) b2.getConfidence();
        if (b1_c > b2_c)
        {
          return 1;
        }
        else if (b1_c < b2_c)
        {
          return -1;
        }
        else
        {
          return 0;
        }
      }
      return 0;
    }
  
  
  
  /**
   * This function  compares  tuple1 with another tuple2 whose
   * field number is same as the tuple1
   *
   *@param    b1        one basicpattern
   *@param    value     another basicpattern.
   *@param    b1_fld_no the field numbers in the basicpatterns to be compared.  
   *@return   0        if the two are equal,
   *          1        if the tuple is greater,
   *         -1        if the tuple is smaller,  
   *@exception UnknowAttrType don't know the attribute type   
   *@exception IOException some I/O fault
   * @throws FieldNumberOutOfBoundException
   * @throws TupleUtilsException
   */            
  public static int CompareBPWithValue(
    BasicPattern b1, int b1_fld_no,
    BasicPattern value)
    throws IOException,
    UnknowAttrType,
    TupleUtilsException,
    FieldNumberOutOfBoundException,
    InvalidSlotNumberException, 
    InvalidTupleSizeException, 
    HFException, 
    HFDiskMgrException, 
    HFBufMgrException, 
    Exception
    {
      return CompareBPWithBP(b1, b1_fld_no, value, b1_fld_no);
    }
  
  /**
   *This function Compares two Tuple inn all fields 
   * @param b1 the first basicpattern
   * @param b2 the secocnd basicpattern
   * @param len the field numbers
   * @return  0        if the two are not equal,
   *          1        if the two are equal,
   *@exception UnknowAttrType don't know the attribute type
   *@exception IOException some I/O fault
   *@exception TupleUtilsException exception from this class
   * @throws FieldNumberOutOfBoundException
   */            
  
  public static boolean Equal(BasicPattern b1, BasicPattern b2)
    throws IOException,
    UnknowAttrType,
    TupleUtilsException, FieldNumberOutOfBoundException,
    InvalidSlotNumberException, 
    InvalidTupleSizeException, 
    HFException, 
    HFDiskMgrException, 
    HFBufMgrException, 
    Exception
    {
      int b1_l = b1.getNodeIDCount();
      int b2_l = b2.getNodeIDCount();
      if (b1_l != b2_l)
      {
        return false;
      }

      for (int i = 1; i <= (b1_l + 1); i++)
      {
        if (CompareBPWithBP(b1, i, b2, i) != 0)
        {
          return false;
        }
      }
      return true;
    }
   
 /**
  *set up a basicpattern in specified field from a basicpattern
  *@param value the basicpattern to be set 
  *@param bp the given basicpattern
  *@param fld_no the field number
  *@param fldType the basicpattern attr type
  *@exception UnknowAttrType don't know the attribute type
  *@exception IOException some I/O fault
  *@exception TupleUtilsException exception from this class
  */  
  public static void SetValue(BasicPattern value, BasicPattern  bp, int fld_no)
  throws IOException,
          UnknowAttrType,
          TupleUtilsException
  {
    try {
      int v_n = value.getNodeIDCount();
      int b_n = bp.getNodeIDCount();
      if ((fld_no > 0) && (fld_no <= v_n) && (fld_no <= b_n))
      {
        fld_no = fld_no - 1;
        EID e1 = value.getNodeID(fld_no);
        bp.setNodeID(fld_no, e1);
      }
      else if ((fld_no == (b_n + 1)) && (fld_no == (v_n + 1)))
      {
        bp.setConfidence(value.getConfidence());
      }
      else
      {
        throw new TupleUtilsException("BPUtils.java : Field numbers not passed correctly in SetValue");
      }
    } catch (FieldNumberOutOfBoundException e) {
      throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by BPUtils.java");
    }
  }


}




