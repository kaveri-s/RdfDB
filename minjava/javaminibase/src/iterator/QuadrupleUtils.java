package iterator;


import heap.*;
import global.*;
import java.io.*;
import java.lang.*;
import quadrupleheap.*;
import labelheap.*;

/**
 *some useful method when processing Quadruple 
 */
public class QuadrupleUtils
{
  
  /**
   * This function compares a quadruple with another quadruple in respective field, and
   *  returns:
   *
   *    0        if the two are equal,
   *    1        if the quadruple is greater,
   *   -1        if the quadruple is smaller,
   *
   *@param    q1        one quadruple.
   *@param    q2        another quadruple.
   *@param    quadruple_fld_no the field numbers in the quadruples to be compared.
   *@return   0        if the two are equal,
   *          1        if the quadruple is greater,
   *         -1        if the quadruple is smaller,                              
   *
   * @throws InvalidSlotNumberException
   * @throws InvalidTupleSizeException
   * @throws HFException
   * @throws HFDiskMgrException
   * @throws HFBufMgrException
   * @throws Exception
   */
  public static int CompareQuadrupleWithQuadruple(
    Quadruple q1,
    Quadruple q2, 
    int quadruple_fld_no)
    throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception
    {
      switch (quadruple_fld_no)
      {
        case 1:                // Compare subject
        EID s1 = q1.getSubjecqid();
        EID s2 = q2.getSubjecqid();
        if(s1 == s2)
        {
          return 0;
        }
        LID sl1 = s1.returnLID();
        LID sl2 = s2.returnLID();
        if(sl1 == sl2)
        {
          return 0;
        }
        //get label from ID and compare
        Label sll1 = SystemDefs.JavabaseDB.getEntityHandle().getLabel(sl1);
        Label sll2 = SystemDefs.JavabaseDB.getEntityHandle().getLabel(sl2);
        return LabelUtils.CompareLabelWithLabel(sll1, sll2);
        break;
        
        case 2:                // Compare predicate
        PID p1 = q1.getPredicateID();
        PID p2 = q2.getPredicateID();
        if(p1 == p2)
        {
          return 0;
        }
        LID pl1 = p1.returnLID();
        LID pl2 = p2.returnLID();
        if(pl1 == pl2)
        {
          return 0;
        }
        //get label from ID and compare
        Label pll1 = SystemDefs.JavabaseDB.getEntityHandle().getLabel(pl1);
        Label pll2 = SystemDefs.JavabaseDB.getEntityHandle().getLabel(pl2);
        return LabelUtils.CompareLabelWithLabel(pll1, pll2);
        break;
        
        case 3:                // Compare object
        EID o1 = q1.getObjecqid();
        EID o2 = q2.getObjecqid();
        if(o1 == o2)
        {
          return 0;
        }
        LID ol1 = o1.returnLID();
        LID ol2 = o2.returnLID();
        if (ol1 == ol2)
        {
          return 0;
        }
        //get label from ID and compare
        Label oll1 = SystemDefs.JavabaseDB.getEntityHandle().getLabel(ol1);
        Label oll2 = SystemDefs.JavabaseDB.getEntityHandle().getLabel(ol2);
        return LabelUtils.CompareLabelWithLabel(oll1, oll2);
        break;
        
        case 4:                // Compare confidence
        double c1 = q1.getConfidence();
        double c2 = q2.getConfidence();
        if (c1 > c2)
        {
          return 1;
        }
        else if (c1 < c2)
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
   * This function  compares  quadruple1 with another quadruple2
   *
   *@param    q1        one quadruple.
   *@param    q2        another quadruple.
   *@param    quadruple_fld_no the field numbers in the quadruples to be compared.
   *@return   true      if the two are equal,
   *          false     if the two are not equal.
   *
   * @throws InvalidSlotNumberException
   * @throws InvalidTupleSizeException
   * @throws HFException
   * @throws HFDiskMgrException
   * @throws HFBufMgrException
   * @throws Exception
   */            
  public static boolean Equal(
    Quadruple q1, 
    Quadruple q2)
    throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception
    {
      int i;
      for (i = 1; i <= 4; i++)
      {
        if (CompareQuadrupleWithQuadruple(q1, q2, i) != 0)
        {
          return false;
        }
      }
      return true;
    }
  
  /**
   *This function Compares two Quadruple inn all fields 
   * @param value the first quadruple
   * @param aquad the secocnd quadruple
   * @param fld_no the field number
   * @return  void
   * 
   */            
  
  public static void SetValue(
    Quadruple value, 
    Quadruple aquad, 
    int fld_no)
  {
    switch (fld_no)
    {
      case 1:                // set subject
      value.setSubjecqid(aquad.getSubjecqid());
      break;

      case 2:                // set predicate
      value.setPredicateid(aquad.getPredicateID());
      break;

      case 3:                // set object
      value.setObjecqid(aquad.getObjecqid());
      break;

      case 4:                // set confidence
      value.setConfidence(aquad.getConfidence());
      break;
    }
  }

}

