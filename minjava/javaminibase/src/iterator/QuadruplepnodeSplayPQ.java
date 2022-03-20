
package iterator;

import global.*;
import java.io.*;

/**
 * Implements a sorted binary tree (extends class QuadruplepnodePQ).
 * Implements the <code>enq</code> and the <code>deq</code> functions.
 */
public class QuadruplepnodeSplayPQ extends QuadruplepnodePQ
{

  /** the root of the tree */
  protected QuadruplepnodeSplayNode   root;
  /*
  QuadruplepnodeSplayNode*   leftmost();
  QuadruplepnodeSplayNode*   rightmost();
  QuadruplepnodeSplayNode*   pred(QuadruplepnodeSplayNode* t);
  QuadruplepnodeSplayNode*   succ(QuadruplepnodeSplayNode* t);
  void            _kill(QuadruplepnodeSplayNode* t);
  QuadruplepnodeSplayNode*   _copy(QuadruplepnodeSplayNode* t);
  */

  /**
   * class constructor, sets default values.
   */
  public QuadruplepnodeSplayPQ() 
  {
    root = null;
    count = 0;
    quadruple_order = new QuadrupleOrder(QuadrupleOrder.SubjectPredicateObjectConfidence);
  }

  /**
   * class constructor.
   * @param order   the order of sorting (Ascending or Descending)
   */  
  public QuadruplepnodeSplayPQ(QuadrupleOrder quadrupleOrder)
  {
    root = null;
    count = 0;
    quadruple_order = quadrupleOrder;
  }

  /**
   * Inserts an element into the binary tree.
   * @param item the element to be inserted
   * @exception IOException from lower layers
   * @exception QuadrupleUtilsException error in quadruple compare routines
   */
  public void enq(Quadruplepnode item) throws IOException, QuadrupleUtilsException 
  {
    count ++;
    QuadruplepnodeSplayNode newnode = new QuadruplepnodeSplayNode(item);
    QuadruplepnodeSplayNode q = root;

    if (q == null) {
      root = newnode;
      return;
    }
    
    int comp = QuadruplepnodeCMP(item, q.item);
    
    QuadruplepnodeSplayNode l = QuadruplepnodeSplayNode.dummy;
    QuadruplepnodeSplayNode r = QuadruplepnodeSplayNode.dummy;
     
    boolean done = false;

    while (!done)
    {
      if ((quadruple_order._sortingOrder == QuadrupleOrder.Ascending && comp >= 0) || (quadruple_order._sortingOrder == QuadrupleOrder.Descending && comp <= 0))
      {
        QuadruplepnodeSplayNode qr = q.rt;
        if (qr == null) {
          qr = newnode;
          comp = 0;
          done = true;
        }
        else comp = QuadruplepnodeCMP(item, qr.item);
        if ((quadruple_order._sortingOrder == QuadrupleOrder.Ascending && comp <= 0) || (quadruple_order._sortingOrder == QuadrupleOrder.Descending && comp >= 0))
        {
          l.rt = q;
          q.par = l;
          l = q;
          q = qr;
        }
        else
        {
          QuadruplepnodeSplayNode qrr = qr.rt;
          if (qrr == null)
          {
            qrr = newnode;
            comp = 0;
            done = true;
          }
          else comp = QuadruplepnodeCMP(item, qrr.item);
          if ((q.rt = qr.lt) != null) q.rt.par = q;
          qr.lt = q; q.par = qr;
          l.rt = qr; qr.par = l;
          l = qr;
          q = qrr;
        }
      } // end of if(comp >= 0)
      else {
        QuadruplepnodeSplayNode ql = q.lt;
        if (ql == null) {
          ql = newnode;
          comp = 0;
          done = true;
        }
        else comp = QuadruplepnodeCMP(item, ql.item);
        if ((quadruple_order._sortingOrder == QuadrupleOrder.Ascending && comp >= 0) || (quadruple_order._sortingOrder == QuadrupleOrder.Descending && comp <= 0))
        {
          r.lt = q; q.par = r;
          r = q;
          q = ql;
        }
        else 
        {
          QuadruplepnodeSplayNode qll = ql.lt;
          if (qll == null)
          {
            qll = newnode;
            comp = 0;
            done = true;
          }
          else comp = QuadruplepnodeCMP(item, qll.item);
          if ((q.lt = ql.rt) != null) q.lt.par = q;
          ql.rt = q; q.par = ql;
          r.lt = ql; ql.par = r;
          r = ql;
          q = qll;
        }
      } // end of else
    } // end of while(!done)
    
    if ((r.lt = q.rt) != null) r.lt.par = r;
    if ((l.rt = q.lt) != null) l.rt.par = l;
    if ((q.lt = QuadruplepnodeSplayNode.dummy.rt) != null) q.lt.par = q;
    if ((q.rt = QuadruplepnodeSplayNode.dummy.lt) != null) q.rt.par = q;
    q.par = null;
    root = q;
	    
    return; 
  }
  
  /**
   * Removes the minimum (Ascending) or maximum (Descending) element.
   * @return the element removed
   */
  public Quadruplepnode deq() 
  {
    if (root == null) return null;
    
    count --;
    QuadruplepnodeSplayNode q = root;
    QuadruplepnodeSplayNode l = root.lt;
    if (l == null) {
      if ((root = q.rt) != null) root.par = null;
      return q.item;
    }
    else {
      while (true) {
	QuadruplepnodeSplayNode ll = l.lt;
	if (ll == null) {
	  if ((q.lt = l.rt) != null) q.lt.par = q;
	  return l.item;
	}
	else {
	  QuadruplepnodeSplayNode lll = ll.lt;
	  if (lll == null) {
	    if((l.lt = ll.rt) != null) l.lt.par = l;
	    return ll.item;
	  }
	  else {
	    q.lt = ll; ll.par = q;
	    if ((l.lt = ll.rt) != null) l.lt.par = l;
	    ll.rt = l; l.par = ll;
	    q = ll;
	    l = lll;
	  }
	}
      } // end of while(true)
    } 
  }
  
  /*  
                  QuadruplepnodeSplayPQ(QuadruplepnodeSplayPQ& a);
  virtual       ~QuadruplepnodeSplayPQ();

  Pix           enq(Quadruplepnode  item);
  Quadruplepnode           deq(); 

  Quadruplepnode&          front();
  void          del_front();

  int           contains(Quadruplepnode  item);

  void          clear(); 

  Pix           first(); 
  Pix           last(); 
  void          next(Pix& i);
  void          prev(Pix& i);
  Quadruplepnode&          operator () (Pix i);
  void          del(Pix i);
  Pix           seek(Quadruplepnode  item);

  int           OK();                    // rep invariant
  */
}
