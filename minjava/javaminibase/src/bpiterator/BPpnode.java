package bpiterator; 

import global.*;
import bufmgr.*;
import diskmgr.*;
import heap.*;
import basicpattern.*;

/**
 * A structure describing a bp.
 * include a run number and the bp
 */
public class BPpnode {
  /** which run does this bp belong */
  public int     run_num;

  /** the bp reference */
  public BasicPattern   basicPattern;

  /**
   * class constructor, sets <code>run_num</code> to 0 and <code>bp</code>
   * to null.
   */
  public BPpnode() 
  {
    run_num = 0;  // this may need to be changed
    basicPattern = null; 
  }
  
  /**
   * class constructor, sets <code>run_num</code> and <code>bp</code>.
   * @param runNum the run number
   * @param t      the bp
   */
  public BPpnode(int runNum, BasicPattern b) 
  {
    run_num = runNum;
    basicPattern = b;
  }
  
}

