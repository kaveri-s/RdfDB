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
   *@param    quadrupleOrder order and field to compare quadruple by.
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
    QuadrupleOrder quadrupleOrder)
    throws QuadrupleUtilsException
    {
      int lRet = 0;
      LabelHeapFile labelHeapFile = SystemDefs.JavabaseDB.getEntityHandle();
      switch (quadrupleOrder._sortBy)
      {
        case QuadrupleOrder.SubjectPredicateObjectConfidence:
        lRet = comapareSubject(q1, q2, labelHeapFile);
        if (lRet == 0)
        {
          lRet = comaparePredicate(q1, q2, labelHeapFile);
          if (lRet == 0)
          {
            lRet = comapareObject(q1, q2, labelHeapFile);
            if (lRet == 0)
            {
              lRet = comapareConfidence(q1, q2);
            }
          }
        }
        break;

        case QuadrupleOrder.PredicateSubjectObjectConfidence:
        lRet = comaparePredicate(q1, q2, labelHeapFile);
        if (lRet == 0)
        {
          lRet = comapareSubject(q1, q2, labelHeapFile);
          if (lRet == 0)
          {
            lRet = comapareObject(q1, q2, labelHeapFile);
            if (lRet == 0)
            {
              lRet = comapareConfidence(q1, q2);
            }
          }
        }
        break;

        case QuadrupleOrder.SubjectConfidence:
        lRet = comapareSubject(q1, q2, labelHeapFile);
        if (lRet == 0)
        {
          lRet = comapareConfidence(q1, q2);
        }
        break;

        case QuadrupleOrder.PredicateConfidence:
        lRet = comaparePredicate(q1, q2, labelHeapFile);
        if (lRet == 0)
        {
          lRet = comapareConfidence(q1, q2);
        }
        break;

        case QuadrupleOrder.ObjectConfidence:
        lRet = comapareObject(q1, q2, labelHeapFile);
        if (lRet == 0)
        {
          lRet = comapareConfidence(q1, q2);
        }
        break;

        case QuadrupleOrder.Confidence:
        lRet = comapareConfidence(q1, q2);
        break;

        case QuadrupleOrder.Subject:
        lRet = comapareSubject(q1, q2, labelHeapFile);
        break;

        default:
        lRet = 0;
        break;
      }
      return lRet;
    }
    
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
    throws QuadrupleUtilsException
    {
      LabelHeapFile labelHeapFile = SystemDefs.JavabaseDB.getEntityHandle();
      switch (quadruple_fld_no)
      {
        case 1:                // Compare subject
        return comapareSubject(q1, q2, labelHeapFile);
        
        case 2:                // Compare predicate
        return comaparePredicate(q1, q2, labelHeapFile);
        
        case 3:                // Compare object
        return comapareObject(q1, q2, labelHeapFile);
        
        case 4:                // Compare confidence
        return comapareConfidence(q1, q2);
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
    throws QuadrupleUtilsException
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

  /**
   * This function Compares two Quadruple inn all fields 
   * @param value the first quadruple
   * @param aquad the secocnd quadruple
   * @return  void
   * 
   */            
  
  public static void SetValue(
    Quadruple value, 
    Quadruple aquad)
  {
    value.quadrupleCopy(aquad);
  }

  private static int comapareSubject(
    Quadruple q1,
    Quadruple q2, 
    LabelHeapFile labelHeapFile)
    throws QuadrupleUtilsException
  {
    try {
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
      Label sll1 = labelHeapFile.getLabel(sl1);
      Label sll2 = labelHeapFile.getLabel(sl2);
      return LabelUtils.CompareLabelWithLabel(sll1, sll2);
    } catch (Exception e) {
      throw new QuadrupleUtilsException(e, "QuadrupleUtils.java: getLabel error");
    }
  }

  private static int comaparePredicate(
    Quadruple q1,
    Quadruple q2, 
    LabelHeapFile labelHeapFile)
    throws QuadrupleUtilsException
  {
    try {
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
      Label pll1 = labelHeapFile.getLabel(pl1);
      Label pll2 = labelHeapFile.getLabel(pl2);
      return LabelUtils.CompareLabelWithLabel(pll1, pll2);
    } catch (Exception e) {
	    throw new QuadrupleUtilsException(e, "QuadrupleUtils.java: getLabel error");
    }
  }

  private static int comapareObject(
    Quadruple q1,
    Quadruple q2, 
    LabelHeapFile labelHeapFile)
    throws QuadrupleUtilsException
  {
    try {
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
      Label oll1 = labelHeapFile.getLabel(ol1);
      Label oll2 = labelHeapFile.getLabel(ol2);
      return LabelUtils.CompareLabelWithLabel(oll1, oll2);
    } catch (Exception e) {
	    throw new QuadrupleUtilsException(e, "QuadrupleUtils.java: getLabel error");
    }
  }

  private static int comapareConfidence(
    Quadruple q1,
    Quadruple q2)
  {
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
    return 0;
  }


}

