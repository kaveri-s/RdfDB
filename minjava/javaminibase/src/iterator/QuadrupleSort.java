package iterator;

import java.io.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import heap.*;
import quadrupleheap.*;
import index.*;
import chainexception.*;

/**
 * The QuadrupleSort class sorts a file. All necessary information are passed as 
 * arguments to the constructor. After the constructor call, the user can
 * repeatly call <code>get_next()</code> to get quadruples in sorted order.
 * After the sorting is done, the user should call <code>close()</code>
 * to clean up.
 */
public class QuadrupleSort extends Iterator implements GlobalConst
{
  private static final int ARBIT_RUNS = 10;

  private TScan                   _am;
  private QuadrupleOrder          _quadruple_order;
  private int                     _n_pages;
  private PageId[]                bufs_pids;
  private byte[][]                bufs;
  private boolean                 useBM = true; // flag for whether to use buffer manager
  private boolean                 first_time;
  private QuadrupleHeapFile[]     temp_files;
  private int                     n_tempfiles;
  private int[]                   n_quadruples;
  private int                     n_runs;
  private QuadrupleOBuf           o_buf;
  private int                     max_elems_in_heap;
  private QuadruplepnodeSplayPQ   Q;
  private Quadruple               op_buf;
  private int                     Nruns;
  private Quadruple               output_quadruple;
  private QuadrupleSpoofIbuf[]    i_buf;

  /**
   * Class constructor, take information about the quadruples, and set up 
   * the sorting
   * @param am an Heap file scan iterator for accessing the triples
   * @param quadruple_order the sorting order (ASCENDING, DESCENDING)
   * @param n_pages amount of memory (in pages) available for sorting
   * @exception IOException from lower layers
   * @exception QuadrupleSortException something went wrong in the lower layer. 
   */
  public QuadrupleSort(
          TScan am,
          QuadrupleOrder quadruple_order,
          int n_pages)
          throws IOException,
          QuadrupleSortException
  {
    _am = am;
    _quadruple_order = quadruple_order;
    _n_pages = n_pages;

    // this may need change, bufs ???  need io_bufs.java
    // bufs = get_buffer_pages(_n_pages, bufs_pids, bufs);
    bufs_pids = new PageId[_n_pages];
    bufs = new byte[_n_pages][];

    if (useBM) {
      try {
        get_buffer_pages(_n_pages, bufs_pids, bufs);
      }
      catch (Exception e) {
        throw new QuadrupleSortException(e, "QuadrupleSort.java: BUFmgr error");
      }
    }
    else {
      for (int k=0; k<_n_pages; k++) bufs[k] = new byte[MAX_SPACE];
    }

    first_time = true;

    // as a heuristic, we set the number of runs to an arbitrary value
    // of ARBIT_RUNS
    temp_files = new QuadrupleHeapFile[ARBIT_RUNS];
    n_tempfiles = ARBIT_RUNS;
    n_quadruples = new int[ARBIT_RUNS];
    n_runs = ARBIT_RUNS;

    try {
      temp_files[0] = new QuadrupleHeapFile(null);
    }
    catch (Exception e) {
      throw new QuadrupleSortException(e, "QuadrupleSort.java: QuadrupleHeapFile error");
    }

    o_buf = new QuadrupleOBuf();

    o_buf.init(bufs, _n_pages, temp_files[0], false);
    //    output_quadruple = null;

    max_elems_in_heap = 200;

    Q = new QuadruplepnodeSplayPQ(_quadruple_order);

    op_buf = new Quadruple();
  }

  /**
   * Returns the next quadruple in sorted order.
   * Note: You need to copy out the content of the quadruple, otherwise it
   *       will be overwritten by the next <code>get_next()</code> call.
   * @return the next quadruple, null if all quadruples exhausted
   * @exception IOException from lower layers
   * @exception SortException something went wrong in the lower layer. 
   * @exception JoinsException from <code>generate_runs()</code>.
   * @exception UnknowAttrType attribute type unknown
   * @exception LowMemException memory low exception
   * @exception Exception other exceptions
   */
  public Quadruple get_next()
          throws IOException,
          SortException,
          UnknowAttrType,
          LowMemException,
          JoinsException,
          Exception
  {
    if (first_time) {
      // first get_next call to the sort routine
      first_time = false;

      // generate runs
      Nruns = generate_runs(max_elems_in_heap);
      //      System.out.println("Generated " + Nruns + " runs");

      // setup state to perform merge of runs. 
      // Open input buffers for all the input file
      setup_for_merge(Nruns);
    }

    if (Q.empty()) {
      // no more quadruples availble
      return null;
    }

    output_quadruple = delete_min();
    if (output_quadruple != null){
      op_buf.quadrupleCopy(output_quadruple);
      return op_buf;
    }
    else
      return null;
  }

  /**
   * Cleaning up, including releasing buffer pages from the buffer pool
   * and removing temporary files from the database.
   * @exception IOException from lower layers
   * @exception SortException something went wrong in the lower layer. 
   */
  public void close() throws SortException, IOException
  {
    // clean up
    if (!closeFlag) {

      try {
        _am.closescan();
      }
      catch (Exception e) {
        throw new SortException(e, "QuadrupleSort.java: error in closing scan.");
      }

      if (useBM) {
        try {
          free_buffer_pages(_n_pages, bufs_pids);
        }
        catch (Exception e) {
          throw new SortException(e, "QuadrupleSort.java: BUFmgr error");
        }
        for (int i=0; i<_n_pages; i++) bufs_pids[i].pid = INVALID_PAGE;
      }

      /**
       * Cleanup heapfiles on exit.
       */
      for (int j = 0; j < i_buf.length; j++) {
        i_buf[j].close();
      }

      for (int i = 0; i<temp_files.length; i++) {
        if (temp_files[i] != null) {
          try {
            temp_files[i].deleteFile();
          }
          catch (Exception e) {
            throw new SortException(e, "QuadrupleSort.java: QuadrupleHeapfile error");
          }
          temp_files[i] = null;
        }
      }
      closeFlag = true;
    }
  }

  /**
   * Generate sorted runs.
   * Using heap sort.
   * @param  max_elems    maximum number of elements in heap
   * @return number of runs generated
   * @exception IOException from lower layers
   * @exception QuadrupleSortException something went wrong in the lower layer. 
   * @exception JoinsException from <code>Iterator.get_next()</code>
   */
  private int generate_runs(int max_elems)
          throws IOException,
          QuadrupleSortException,
          QuadrupleUtilsException,
          JoinsException,
          Exception
  {
    Quadruple quadruple;
    Quadruplepnode cur_node;
    QuadruplepnodeSplayPQ Q1 = new QuadruplepnodeSplayPQ(_quadruple_order);
    QuadruplepnodeSplayPQ Q2 = new QuadruplepnodeSplayPQ(_quadruple_order);
    QuadruplepnodeSplayPQ pcurr_Q = Q1;
    QuadruplepnodeSplayPQ pother_Q = Q2;
    Quadruple lastElem = getDummyQuadruple();

    int run_num = 0;  // keeps track of the number of runs

    // number of elements in Q
    //    int nelems_Q1 = 0;
    //    int nelems_Q2 = 0;
    int p_elems_curr_Q = 0;
    int p_elems_other_Q = 0;

    int comp_res;

    // maintain a fixed maximum number of elements in the heap
    while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
      try {
        QID qid = new QID();
        quadruple = _am.getNext(qid);  // according to TScan.java
      } catch (Exception e) {
        e.printStackTrace();
        throw new QuadrupleSortException(e, "QuadrupleSort.java: getNext() failed");
      }

      if (quadruple == null) {
        break;
      }
      cur_node = new Quadruplepnode();
      cur_node.quadruple = new Quadruple(quadruple); // quadruple copy needed

      pcurr_Q.enq(cur_node);
      p_elems_curr_Q ++;
    }

    // now the queue is full, starting writing to file while keep trying
    // to add new quadruple to the queue. The ones that does not fit are put
    // on the other queue temperarily
    while (true) {
      cur_node = pcurr_Q.deq();
      if (cur_node == null) break;
      p_elems_curr_Q --;

      comp_res = QuadrupleUtils.CompareQuadrupleWithQuadruple(cur_node.quadruple, lastElem, _quadruple_order);  // need QuadrupleUtils.java

      if ((comp_res < 0 && _quadruple_order._sortingOrder == QuadrupleOrder.Ascending) || (comp_res > 0 && _quadruple_order._sortingOrder == QuadrupleOrder.Descending))
      {
        // doesn't fit in current run, put into the other queue
        try {
          pother_Q.enq(cur_node);
        }
        catch (Exception e) {
          throw new QuadrupleSortException(e, "QuadrupleSort.java: UnknowAttrType caught from Q.enq()");
        }
        p_elems_other_Q ++;
      }
      else {
        // set lastElem to have the value of the current quadruple,
        // need QuadrupleUtils.java
        QuadrupleUtils.SetValue(lastElem, cur_node.quadruple);
        // write quadruple to output file, need io_bufs.java, type cast???
        //	System.out.println("Putting quadruple into run " + (run_num + 1)); 
        //	cur_node.quadruple.print(_in);

        o_buf.Put(cur_node.quadruple);
      }

      // check whether the other queue is full
      if (p_elems_other_Q == max_elems) {
        // close current run and start next run
        n_quadruples[run_num] = (int) o_buf.flush();  // need Quadrupleio_bufs.java
        run_num ++;

        // check to see whether need to expand the array
        if (run_num == n_tempfiles) {
          QuadrupleHeapFile[] temp1 = new QuadrupleHeapFile[2*n_tempfiles];
          for (int i=0; i<n_tempfiles; i++) {
            temp1[i] = temp_files[i];
          }
          temp_files = temp1;
          n_tempfiles *= 2;

          int[] temp2 = new int[2*n_runs];
          for(int j=0; j<n_runs; j++) {
            temp2[j] = n_quadruples[j];
          }
          n_quadruples = temp2;
          n_runs *=2;
        }

        try {
          temp_files[run_num] = new QuadrupleHeapFile(null);
        }
        catch (Exception e) {
          throw new QuadrupleSortException(e, "QuadrupleSort.java: create QuadrupleHeapFile failed");
        }

        // need io_bufs.java
        o_buf.init(bufs, _n_pages, temp_files[run_num], false);

        // set the last Elem to be the minimum value for the sort field
        lastElem = getDummyQuadruple();

        // switch the current heap and the other heap
        QuadruplepnodeSplayPQ tempQ = pcurr_Q;
        pcurr_Q = pother_Q;
        pother_Q = tempQ;
        int tempelems = p_elems_curr_Q;
        p_elems_curr_Q = p_elems_other_Q;
        p_elems_other_Q = tempelems;
      }

      // now check whether the current queue is empty
      else if (p_elems_curr_Q == 0) {
        while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
          try
          {
            QID qid = new QID();
            quadruple = _am.getNext(qid);  // according to TScan.java
          } catch (Exception e) {
            throw new QuadrupleSortException(e, "getNext() failed");
          }

          if (quadruple == null) {
            break;
          }
          cur_node = new Quadruplepnode();
          cur_node.quadruple = new Quadruple(quadruple); // quadruple copy needed

          try {
            pcurr_Q.enq(cur_node);
          }
          catch (Exception e) {
            throw new QuadrupleSortException(e, "QuadrupleSort.java: UnknowAttrType caught from Q.enq()");
          }
          p_elems_curr_Q ++;
        }
      }

      // Check if we are done
      if (p_elems_curr_Q == 0) {
        // current queue empty despite our attemps to fill in
        // indicating no more tuples from input
        if (p_elems_other_Q == 0) {
          // other queue is also empty, no more tuples to write out, done
          break; // of the while(true) loop
        }
        else {
          // generate one more run for all tuples in the other queue
          // close current run and start next run
          n_quadruples[run_num] = (int) o_buf.flush();  // need io_bufs.java
          run_num ++;

          // check to see whether need to expand the array
          if (run_num == n_tempfiles) {
            QuadrupleHeapFile[] temp1 = new QuadrupleHeapFile[2*n_tempfiles];
            for (int i=0; i<n_tempfiles; i++) {
              temp1[i] = temp_files[i];
            }
            temp_files = temp1;
            n_tempfiles *= 2;

            int[] temp2 = new int[2*n_runs];
            for(int j=0; j<n_runs; j++) {
              temp2[j] = n_quadruples[j];
            }
            n_quadruples = temp2;
            n_runs *=2;
          }

          try {
            temp_files[run_num] = new QuadrupleHeapFile(null);
          }
          catch (Exception e) {
            throw new QuadrupleSortException(e, "QuadrupleSort.java: create quadrupleHeapFile failed");
          }

          // need io_bufs.java
          o_buf.init(bufs, _n_pages, temp_files[run_num], false);

          lastElem = getDummyQuadruple();

          // switch the current heap and the other heap
          QuadruplepnodeSplayPQ tempQ = pcurr_Q;
          pcurr_Q = pother_Q;
          pother_Q = tempQ;
          int tempelems = p_elems_curr_Q;
          p_elems_curr_Q = p_elems_other_Q;
          p_elems_other_Q = tempelems;
        }
      } // end of if (p_elems_curr_Q == 0)
    } // end of while (true)

    // close the last run
    n_quadruples[run_num] = (int) o_buf.flush();
    run_num ++;

    return run_num;
  }

  /**
   * Set up for merging the runs.
   * Open an input buffer for each run, and insert the first element (min)
   * from each run into a heap. <code>delete_min() </code> will then get 
   * the minimum of all runs.
   * @param n_R_runs number of runs
   * @exception IOException from lower layers
   * @exception LowMemException there is not enough memory to 
   *                 sort in two passes (a subclass of SortException).
   * @exception QuadrupleSortException something went wrong in the lower layer. 
   * @exception Exception other exceptions
   */
  private void setup_for_merge(int n_R_runs)
          throws IOException,
          LowMemException,
          SortException,
          Exception
  {
    // don't know what will happen if n_R_runs > _n_pages
    if (n_R_runs > _n_pages)
      throw new LowMemException("QuadrupleSort.java: Not enough memory to sort in two passes.");

    int i;
    Quadruplepnode cur_node;  // need pq_defs.java

    i_buf = new QuadrupleSpoofIbuf[n_R_runs];   // need Quadrupleio_bufs.java
    for (int j=0; j<n_R_runs; j++) i_buf[j] = new QuadrupleSpoofIbuf();

    // construct the lists, ignore TEST for now
    // this is a patch, I am not sure whether it works well -- bingjie 4/20/98

    for (i=0; i<n_R_runs; i++) {
      byte[][] apage = new byte[1][];
      apage[0] = bufs[i];

      // need iobufs.java
      i_buf[i].init(temp_files[i], apage, 1, n_quadruples[i]);

      cur_node = new Quadruplepnode();
      cur_node.run_num = i;

      // may need change depending on whether Get() returns the original
      // or make a copy of the tuple, need io_bufs.java ???
      Quadruple temp_quadruple = new Quadruple();

      temp_quadruple =i_buf[i].Get(temp_quadruple);  // need io_bufs.java

      if (temp_quadruple != null) {
	/*
	System.out.print("Get quadruple from run " + i);
	temp_quadruple.print(_in);
	*/
        cur_node.quadruple = temp_quadruple; // no copy needed
        try {
          Q.enq(cur_node);
        }
        catch (QuadrupleUtilsException e) {
          throw new QuadrupleSortException(e, "QuadrupleSort.java: QuadrupleUtilsException caught from Q.enq()");
        }

      }
    }
    return;
  }

  /**
   * Remove the minimum value among all the runs.
   * @return the minimum tuple removed
   * @exception IOException from lower layers
   * @exception QuadrupleSortException something went wrong in the lower layer. 
   */
  private Quadruple delete_min()
          throws IOException,
          QuadrupleSortException,
          Exception
  {
    Quadruplepnode cur_node;                // needs pq_defs.java  
    Quadruple new_quadruple, old_quadruple;

    cur_node = Q.deq();
    old_quadruple = cur_node.quadruple;
    /*
    System.out.print("Get ");
    old_quadruple.print(_in);
    */
    // we just removed one quadruple from one run, now we need to put another
    // quadruple of the same run into the queue
    if (i_buf[cur_node.run_num].empty() != true) {
      // run not exhausted 
      new_quadruple = new Quadruple(); // need Quadruple.java

      new_quadruple = i_buf[cur_node.run_num].Get(new_quadruple);
      if (new_quadruple != null) {
        /*
        System.out.print(" fill in from run " + cur_node.run_num);
        new_quadruple.print(_in);
        */
        cur_node.quadruple = new_quadruple;  // no copy needed -- I think Bingjie 4/22/98
        try {
          Q.enq(cur_node);
        } catch (QuadrupleUtilsException e) {
          throw new QuadrupleSortException(e, "QuadrupleSort.java: QuadrupleUtilsException caught from Q.enq()");
        }
      }
      else {
        throw new QuadrupleSortException("********** Wait a minute, I thought input is not empty ***************");
      }
    }
    // changed to return Tuple instead of return char array ????
    return old_quadruple;
  }

  private Quadruple getDummyQuadruple()
  {
    PageId pageno = new PageId(-1);
    LID lid = new LID(pageno, -1);
    Quadruple quadruple = new Quadruple();
    try
    {
      quadruple.setSubjecqid(lid.returnEID());
      quadruple.setPredicateid(lid.returnPID());
      quadruple.setObjecqid(lid.returnEID());
      quadruple.setConfidence(-1);
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    return quadruple;
  }

}


