package bpiterator;

import java.io.*;
import global.*;
import heap.*;
import basicpattern.*;
import iterator.*;

/**
 * The Sort class sorts a file. All necessary information are passed as 
 * arguments to the constructor. After the constructor call, the user can
 * repeatly call <code>get_next()</code> to get tuples in sorted order.
 * After the sorting is done, the user should call <code>close()</code>
 * to clean up.
 */
public class BPSort extends BPIterator implements GlobalConst
{
	private static final int ARBIT_RUNS = 10;

	private BPFileScan    _am;
	private int           _sort_fld;
	private BPOrder       order;
	private int           _n_pages;
	private byte[][]      bufs;
	private boolean       first_time;
	private int           Nruns;
	private int           max_elems_in_heap;
	private int           bp_size;

	private BPpnodeSplayPQ    Q;
	private Heapfile[]        temp_files;
	private int               n_tempfiles;
	private BasicPattern      output_bp;
	private int[]             n_bps;
	private int               n_runs;
	private BasicPattern      op_buf;
	private OBuf o_buf;
	private SpoofIbuf[]     i_buf;
	private PageId[]          bufs_pids;
	private boolean useBM = true; // flag for whether to use buffer manager

	/**
	 * Set up for merging the runs.
	 * Open an input buffer for each run, and insert the first element (min)
	 * from each run into a heap. <code>delete_min() </code> will then get
	 * the minimum of all runs.
	 * @param bp_size size (in bytes) of each basicpattern
	 * @param n_R_runs number of runs
	 * @exception IOException from lower layers
	 * @exception LowMemException there is not enough memory to
	 *                 sort in two passes (a subclass of SortException).
	 * @exception SortException something went wrong in the lower layer.
	 * @exception Exception other exceptions
	 */
	private void setup_for_merge(int bp_size, int n_R_runs)
			throws Exception
	{
		// don't know what will happen if n_R_runs > _n_pages
		if (n_R_runs > _n_pages)
			throw new LowMemException("Sort.java: Not enough memory to sort in two passes.");

		int i;
		BPpnode cur_node;  // need pq_defs.java

		i_buf = new SpoofIbuf[n_R_runs];   // need io_bufs.java
		for (int j=0; j<n_R_runs; j++) i_buf[j] = new SpoofIbuf();

		// construct the lists, ignore TEST for now
		// this is a patch, I am not sure whether it works well -- bingjie 4/20/98
		for (i=0; i<n_R_runs; i++) {
			byte[][] apage = new byte[1][];
			apage[0] = bufs[i];

			// need iobufs.java
			i_buf[i].init(temp_files[i], apage, 1, bp_size, n_bps[i]);
			cur_node = new BPpnode();
			cur_node.run_num = i;

			// may need change depending on whether Get() returns the original
			// or make a copy of the tuple, need io_bufs.java ???
			Tuple temp_tuple = new Tuple(bp_size);

			Tuple temp_tup =i_buf[i].Get(temp_tuple);  // need io_bufs.java
			if (temp_tup != null) {
				cur_node.basicPattern = new BasicPattern(temp_tup.returnTupleByteArray(), 0); // no copy needed
				try {
					Q.enq(cur_node);
				}
				catch (UnknowAttrType e) {
					throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
				}
				catch (TupleUtilsException e) {
					throw new SortException(e, "Sort.java: TupleUtilsException caught from Q.enq()");
				}

			}
		}
		return;
	}

	public void sort_init(AttrType[] in,short len_in)
			throws IOException, SortException
	{

		bp_size = (len_in-1)*8 + 8;

		bufs_pids = new PageId[_n_pages];
		bufs = new byte[_n_pages][];

		if (useBM)
		{
			try
			{
				get_buffer_pages(_n_pages, bufs_pids, bufs);
			}
			catch (Exception e) {
				throw new SortException(e, "Sort.java: BUFmgr error");
			}
		}
		else
		{
			for (int k=0; k<_n_pages; k++) bufs[k] = new byte[MAX_SPACE];
		}

		// as a heuristic, we set the number of runs to an arbitrary value
		// of ARBIT_RUNS
		temp_files = new Heapfile[ARBIT_RUNS];
		n_tempfiles = ARBIT_RUNS;
		n_bps = new int[ARBIT_RUNS];
		n_runs = ARBIT_RUNS;

		try
		{
			temp_files[0] = new Heapfile(null);
		}
		catch (Exception e) {
			throw new SortException(e, "Sort.java: Heapfile error");
		}

		o_buf = new OBuf();

		o_buf.init(bufs, _n_pages, bp_size, temp_files[0], false);	// todo: CHIRAYU
		//    output_tuple = null;

		Q = new BPpnodeSplayPQ(_sort_fld, in[_sort_fld - 1], order);		// todo: CHIRAYU
		op_buf = new BasicPattern(len_in-1);   // need Tuple.java
	}

	/**
	 * Generate sorted runs.
	 * Using heap sort.
	 * @param  max_elems    maximum number of elements in heap
	 * @param  sortFldType  attribute type of the sort field
	 * @return number of runs generated
	 * @exception IOException from lower layers
	 * @exception SortException something went wrong in the lower layer.
	 * @exception JoinsException from <code>Iterator.get_next()</code>
	 */
	private int generate_runs(int max_elems, AttrType sortFldType/*, int sortFldLen*/)
			throws IOException,
			SortException,
			UnknowAttrType,
			TupleUtilsException,
			JoinsException,
			Exception
	{
		int init_flag = 1;
		BasicPattern basicPattern;
		BPpnode cur_node;
		BPpnodeSplayPQ Q1 = new BPpnodeSplayPQ(_sort_fld, sortFldType, order);
		BPpnodeSplayPQ Q2 = new BPpnodeSplayPQ(_sort_fld, sortFldType, order);
		BPpnodeSplayPQ pcurr_Q = Q1;
		BPpnodeSplayPQ pother_Q = Q2;

		int run_num = 0;  // keeps track of the number of runs

		int p_elems_curr_Q = 0;
		int p_elems_other_Q = 0;

		int comp_res;

		// maintain a fixed maximum number of elements in the heap
		while ((p_elems_curr_Q + p_elems_other_Q) < max_elems)
		{
			try
			{
				basicPattern = _am.get_next();  // according to Iterator.java
				if(init_flag==1)
				{
					// fill all the vars here!! --- CHIRAYU
					int size = basicPattern.getNodeIDCount() + 1;
					AttrType[] in = new AttrType[basicPattern.getNodeIDCount() + 1];
					int j = 0;
					for(j = 0 ; j < (basicPattern.getNodeIDCount())  ; j++)
					{
						in[j] = new AttrType(AttrType.attrInteger);
					}
					in[j] = new AttrType(AttrType.attrFloat);

					if (_sort_fld==-1)
					{
						_sort_fld = basicPattern.getNodeIDCount() + 1;
						BPpnodeSplayPQ Q1_confidence = new BPpnodeSplayPQ(_sort_fld, sortFldType, order);
						BPpnodeSplayPQ Q2_confidence = new BPpnodeSplayPQ(_sort_fld, sortFldType, order);
						pcurr_Q = Q1_confidence;
						pother_Q = Q2_confidence;
					}

					sort_init(in, (short) (basicPattern.getNodeIDCount() +1));
					init_flag = 0;
				}
			}
			catch (Exception e)
			{
				e.printStackTrace();
				throw new SortException(e, "BPSort.java: get_next() failed");
			}
			if (basicPattern == null)
			{
				break;
			}
			cur_node = new BPpnode();
			cur_node.basicPattern = new BasicPattern(basicPattern.getNodeIDCount());
			cur_node.basicPattern.basicPatternCopy(basicPattern); // tuple copy needed --  Bingjie 4/29/98

			pcurr_Q.enq(cur_node);
			p_elems_curr_Q ++;
		}

		BasicPattern lastElem = new BasicPattern((bp_size - 8)/8);  // need tuple.java
		// set the lastElem to be the minimum value for the sort field
		if(order.bpOrder == BPOrder.Ascending)
		{
			try
			{
				MIN_VAL(lastElem, sortFldType);
			}
			catch (UnknowAttrType e)
			{
				throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
			}
			catch (Exception e)
			{
				throw new SortException(e, "MIN_VAL failed");
			}
		}
		else
		{
			try
			{
				MAX_VAL(lastElem, sortFldType);
			}
			catch (UnknowAttrType e)
			{
				throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
			}
			catch (Exception e)
			{
				throw new SortException(e, "MIN_VAL failed");
			}
		}


		// now the queue is full, starting writing to file while keep trying
		// to add new tuples to the queue. The ones that does not fit are put
		// on the other queue temperarily
		while (true) {
			cur_node = pcurr_Q.deq();
			if (cur_node == null) break;
			p_elems_curr_Q --;
			comp_res = BPUtils.CompareBPWithValue(cur_node.basicPattern, _sort_fld, lastElem);  // need tuple_utils.java

			if ((comp_res < 0 && order.bpOrder == BPOrder.Ascending) || (comp_res > 0 && order.bpOrder == BPOrder.Descending)) {
				// doesn't fit in current run, put into the other queue
				try {
					pother_Q.enq(cur_node);
				}
				catch (UnknowAttrType e) {
					throw new SortException(e, "BPSort.java: UnknowAttrType caught from Q.enq()");
				}
				p_elems_other_Q ++;
			}
			else {
				// set lastElem to have the value of the current tuple,
				// need tuple_utils.java
				BPUtils.SetValue(cur_node.basicPattern,lastElem, _sort_fld);
				// write tuple to output file, need io_bufs.java, type cast???
				o_buf.Put(cur_node.basicPattern);
			}

			// check whether the other queue is full
			if (p_elems_other_Q == max_elems) {
				// close current run and start next run
				n_bps[run_num] = (int) o_buf.flush();  // need io_bufs.java
				run_num ++;

				// check to see whether need to expand the array
				if (run_num == n_tempfiles) {
					Heapfile[] temp1 = new Heapfile[2*n_tempfiles];
					for (int i=0; i<n_tempfiles; i++) {
						temp1[i] = temp_files[i];
					}
					temp_files = temp1;
					n_tempfiles *= 2;

					int[] temp2 = new int[2*n_runs];
					for(int j=0; j<n_runs; j++) {
						temp2[j] = n_bps[j];
					}
					n_bps = temp2;
					n_runs *=2;
				}

				try {
					temp_files[run_num] = new Heapfile(null);
				}
				catch (Exception e) {
					throw new SortException(e, "BPSort.java: create Heapfile failed");
				}

				// need io_bufs.java
				o_buf.init(bufs, _n_pages, bp_size, temp_files[run_num], false);

				// set the last Elem to be the minimum value for the sort field
				if(order.bpOrder == BPOrder.Ascending) {
					try {
						MIN_VAL(lastElem, sortFldType);
					} catch (UnknowAttrType e) {
						throw new SortException(e, "BPSort.java: UnknowAttrType caught from MIN_VAL()");
					} catch (Exception e) {
						throw new SortException(e, "MIN_VAL failed");
					}
				}
				else {
					try {
						MAX_VAL(lastElem, sortFldType);
					} catch (UnknowAttrType e) {
						throw new SortException(e, "BPSort.java: UnknowAttrType caught from MAX_VAL()");
					} catch (Exception e) {
						throw new SortException(e, "MIN_VAL failed");
					}
				}

				// switch the current heap and the other heap
				BPpnodeSplayPQ tempQ = pcurr_Q;
				pcurr_Q = pother_Q;
				pother_Q = tempQ;
				int tempelems = p_elems_curr_Q;
				p_elems_curr_Q = p_elems_other_Q;
				p_elems_other_Q = tempelems;
			}

			// now check whether the current queue is empty
			else if (p_elems_curr_Q == 0) {
				while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
					try {
						basicPattern = _am.get_next();  // according to Iterator.java
					} catch (Exception e) {
						throw new SortException(e, "get_next() failed");
					}

					if (basicPattern == null) {
						break;
					}
					cur_node = new BPpnode();
					cur_node.basicPattern = new BasicPattern(basicPattern.getNodeIDCount());
					cur_node.basicPattern.basicPatternCopy(basicPattern); // tuple copy needed --  Bingjie 4/29/98
					try {
						pcurr_Q.enq(cur_node);
					}
					catch (UnknowAttrType e) {
						throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
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
					n_bps[run_num] = (int) o_buf.flush();  // need io_bufs.java
					run_num ++;

					// check to see whether need to expand the array
					if (run_num == n_tempfiles) {
						Heapfile[] temp1 = new Heapfile[2*n_tempfiles];
						for (int i=0; i<n_tempfiles; i++) {
							temp1[i] = temp_files[i];
						}
						temp_files = temp1;
						n_tempfiles *= 2;

						int[] temp2 = new int[2*n_runs];
						for(int j=0; j<n_runs; j++) {
							temp2[j] = n_bps[j];
						}
						n_bps = temp2;
						n_runs *=2;
					}

					try {
						temp_files[run_num] = new Heapfile(null);
					}
					catch (Exception e) {
						throw new SortException(e, "BPSort.java: create Heapfile failed");
					}

					// need io_bufs.java
					o_buf.init(bufs, _n_pages, bp_size, temp_files[run_num], false);

					// set the last Elem to be the minimum value for the sort field
					if(order.bpOrder == BPOrder.Ascending) {
						try {
							MIN_VAL(lastElem, sortFldType);
						} catch (UnknowAttrType e) {
							throw new SortException(e, "BPSort.java: UnknowAttrType caught from MIN_VAL()");
						} catch (Exception e) {
							throw new SortException(e, "MIN_VAL failed");
						}
					}
					else {
						try {
							MAX_VAL(lastElem, sortFldType);
						} catch (UnknowAttrType e) {
							throw new SortException(e, "BPSort.java: UnknowAttrType caught from MAX_VAL()");
						} catch (Exception e) {
							throw new SortException(e, "MIN_VAL failed");
						}
					}

					// switch the current heap and the other heap
					BPpnodeSplayPQ tempQ = pcurr_Q;
					pcurr_Q = pother_Q;
					pother_Q = tempQ;
					int tempelems = p_elems_curr_Q;
					p_elems_curr_Q = p_elems_other_Q;
					p_elems_other_Q = tempelems;
				}
			} // end of if (p_elems_curr_Q == 0)
		} // end of while (true)

		// close the last run
		n_bps[run_num] = (int) o_buf.flush();
		run_num ++;

		return run_num;
	}

	/**
	 * Remove the minimum value among all the runs.
	 * @return the minimum tuple removed
	 * @exception IOException from lower layers
	 * @exception SortException something went wrong in the lower layer.
	 */
	private BasicPattern delete_min()
			throws IOException,
			SortException,
			Exception
	{
		BPpnode cur_node;                // needs pq_defs.java
		BasicPattern new_bp, old_bp;

		cur_node = Q.deq();
		old_bp = cur_node.basicPattern;
		// we just removed one tuple from one run, now we need to put another
		// tuple of the same run into the queue
		if (i_buf[cur_node.run_num].empty() != true) {
			// run not exhausted
			new_bp = new BasicPattern((bp_size - 8)/8); // need tuple.java??

			Tuple temp_tup =i_buf[cur_node.run_num].Get(new_bp);  // need io_bufs.java
			new_bp = new BasicPattern(temp_tup.returnTupleByteArray(), 0);
			if (new_bp != null) {

				cur_node.basicPattern = new_bp;  // no copy needed -- I think Bingjie 4/22/98
				try {
					Q.enq(cur_node);
				} catch (UnknowAttrType e) {
					throw new SortException(e, "BPSort.java: UnknowAttrType caught from Q.enq()");
				} catch (TupleUtilsException e) {
					throw new SortException(e, "BPSort.java: TupleUtilsException caught from Q.enq()");
				}
			}
			else {
				throw new SortException("********** Wait a minute, I thought input is not empty ***************");
			}

		}

		// changed to return Tuple instead of return char array ????
		return old_bp;
	}

	/**
	 * Set lastElem to be the minimum value of the appropriate type
	 * @param lastElem the basicpattern
	 * @param sortFldType the sort field type
	 * @exception IOException from lower layers
	 * @exception UnknowAttrType attrSymbol or attrNull encountered
	 */
	private void MIN_VAL(BasicPattern lastElem, AttrType sortFldType)
			throws IOException,
			FieldNumberOutOfBoundException,
			UnknowAttrType {

		switch (sortFldType.attrType) {
			case AttrType.attrFloat:
				//      lastElem.setHdr(fld_no, junk, null);
				lastElem.setConfidence(Float.MIN_VALUE);
				break;
			case AttrType.attrInteger:
				//      lastElem.setHdr(fld_no, junk, null);
				EID eid = new EID();
				eid.pageNo = new PageId(-1);
				eid.slotNo = 0;
				lastElem.setNodeID(_sort_fld -1, eid);
				break;
			default:
				// don't know how to handle attrSymbol, attrNull
				//System.err.println("error in sort.java");
				throw new UnknowAttrType("BPSort.java: don't know how to handle attrSymbol, attrNull");
		}

		return;
	}

	/**
	 * Set lastElem to be the maximum value of the appropriate type
	 * @param lastElem the basicpattern
	 * @param sortFldType the sort field type
	 * @exception IOException from lower layers
	 * @exception UnknowAttrType attrSymbol or attrNull encountered
	 */
	private void MAX_VAL(BasicPattern lastElem, AttrType sortFldType)
			throws IOException,
			FieldNumberOutOfBoundException,
			UnknowAttrType {


		switch (sortFldType.attrType) {
			case AttrType.attrFloat:
				//      lastElem.setHdr(fld_no, junk, null);
				lastElem.setConfidence(Float.MAX_VALUE);
				break;
			case AttrType.attrInteger:
				PageId page = new PageId();
				page.pid = -2;
				LID lid = new LID(page, -2);
				EID eid = new EID(lid);
				lastElem.setNodeID(_sort_fld -1, eid);
				break;
			default:
				// don't know how to handle attrSymbol, attrNull
				//System.err.println("error in sort.java");
				throw new UnknowAttrType("BPSort.java: don't know how to handle attrSymbol, attrNull");
		}

		return;
	}

	/**
	 * Class constructor, take information about the basicpatterns, and set up
	 * the sorting
	 * @param am an iterator for accessing the basicpatterns
	 * @param sort_fld the field number of the field to sort on
	 * @param sort_order the sorting order (ASCENDING, DESCENDING)
	 * @param n_pages amount of memory (in pages) available for sorting
	 * @exception IOException from lower layers
	 * @exception SortException something went wrong in the lower layer.
	 */
	public BPSort(BPFileScan   am,
				  int        sort_fld,
				  BPOrder sort_order,
				  int        n_pages
	) throws IOException, SortException
	{
		_am = am;
		_sort_fld = sort_fld;
		order = sort_order;
		_n_pages = n_pages;

		first_time = true;
		max_elems_in_heap = 200;
	}

	/**
	 * Returns the next basicpattern in sorted order.
	 * Note: You need to copy out the content of the basicpattern, otherwise it
	 *       will be overwritten by the next <code>get_next()</code> call.
	 * @return the next basicpattern, null if all basicpatterns exhausted
	 * @exception IOException from lower layers
	 * @exception SortException something went wrong in the lower layer.
	 * @exception JoinsException from <code>generate_runs()</code>.
	 * @exception UnknowAttrType attribute type unknown
	 * @exception LowMemException memory low exception
	 * @exception Exception other exceptions
	 */
	public BasicPattern get_next()
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

			AttrType sortFldTyp;
			if(_sort_fld<0)
			{
				sortFldTyp = new AttrType(AttrType.attrFloat);
			}
			else
			{
				sortFldTyp = new AttrType(AttrType.attrInteger);
			}

			// generate runs
			Nruns = generate_runs(max_elems_in_heap, sortFldTyp);
			//      System.out.println("Generated " + Nruns + " runs");

			// setup state to perform merge of runs.
			// Open input buffers for all the input file
			setup_for_merge(bp_size, Nruns);
		}

		if (Q.empty()) {
			// no more tuples availble
			return null;
		}

		output_bp = delete_min();
		if (output_bp != null){
			op_buf.basicPatternCopy(output_bp);
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
				_am.close();
			}
			catch (Exception e) {
				try {
					throw new SortException(e, "BPSort.java: error in closing iterator.");
				} catch (SortException e1) {
					e1.printStackTrace();
				}
			}

			if (useBM) {
				try {
					free_buffer_pages(_n_pages, bufs_pids);
				}
				catch (Exception e) {
					try {

						throw new SortException(e, "BPSort.java: BUFmgr error");
					} catch (SortException e1) {
						e1.printStackTrace();
					}
				}
				for (int i=0; i<_n_pages; i++) bufs_pids[i].pid = INVALID_PAGE;
			}

			for (int i = 0; i<temp_files.length; i++) {
				if (temp_files[i] != null) {
					try {
						temp_files[i].deleteFile();
					}
					catch (Exception e) {
						try {
							throw new SortException(e, "BPSort.java: Heapfile error");
						} catch (SortException e1) {
							e1.printStackTrace();
						}
					}
					temp_files[i] = null;
				}
			}
			closeFlag = true;
		}
	}

}


