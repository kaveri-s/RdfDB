package quadrupleheap;

import java.io.*;
import diskmgr.*;
import global.*;
import heap.*;

/**  This heapfile implementation is directory-based. We maintain a
 *  directory of info about the data pages (which are of type THFPage
 *  when loaded into memory).  The directory itself is also composed
 *  of HFPages, with each record being of type DataPageInfo
 *  as defined below.
 *
 *  The first directory page is a header page for the entire database
 *  (it is the one to which our filename is mapped by the DB).
 *  All directory pages are in a doubly-linked list of pages, each
 *  directory entry points to a single data page, which contains
 *  the actual records.
 *
 *  The heapfile data pages are implemented as slotted pages, with
 *  the slots at the front and the records in the back, both growing
 *  into the free space in the middle of the page.
 *
 *  We can store roughly pagesize/sizeof(DataPageInfo) records per
 *  directory page; for any given HeapFile insertion, it is likely
 *  that at least one of those referenced data pages will have
 *  enough free space to satisfy the request.
 */


/** DataPageInfo class : the type of records stored on a directory page.
 *
 * April 9, 1998
 */

public class QuadrupleHeapFile implements GlobalConst {

	public PageId      _firstDirPageId;   // page number of header page
	protected int         _ftype;
	protected     boolean     _file_deleted;
	protected     String 	 _fileName;
	protected static int tempfilecount = 0;

	private THFPage _newDatapage(DataPageInfo dpinfop)
			throws HFException,
			HFBufMgrException,
			HFDiskMgrException,
			IOException
	{
		Page apage = new Page();
		PageId pageId;
		pageId = newPage(apage, 1);

		if(pageId == null)
			throw new HFException(null, "can't new pae");

		// initialize internal values of the new page:

		THFPage thfpage = new THFPage();
		thfpage.init(pageId, apage);

		dpinfop.pageId.pid = pageId.pid;
		dpinfop.recct = 0;
		dpinfop.availspace = thfpage.available_space();

		return thfpage;

	}

	private boolean  _findDataPage( QID qid,
									PageId dirPageId, HFPage dirpage,
									PageId dataPageId, THFPage datapage,
									RID rpDataPageRid)
			throws InvalidSlotNumberException,
			InvalidTupleSizeException,
			HFException,
			HFBufMgrException,
			HFDiskMgrException,
			Exception
	{
		PageId currentDirPageId = new PageId(_firstDirPageId.pid);

		HFPage currentDirPage = new HFPage();
		THFPage currentDataPage = new THFPage();
		RID currentDataPageRid;
		PageId nextDirPageId;


		pinPage(currentDirPageId, currentDirPage, false/*read disk*/);

		Tuple atuple = new Tuple();

		while (currentDirPageId.pid != INVALID_PAGE)
		{

			for( currentDataPageRid = currentDirPage.firstRecord();
				 currentDataPageRid != null;
				 currentDataPageRid = currentDirPage.nextRecord(currentDataPageRid))
			{
				try{
					atuple = currentDirPage.getRecord(currentDataPageRid);
				}
				catch (InvalidSlotNumberException e)// check error! return false(done)
				{
					return false;
				}

				DataPageInfo dpinfo = new DataPageInfo(atuple);
				try{
					pinPage(dpinfo.pageId, currentDataPage, false/*Rddisk*/);


					//check error;need unpin currentDirPage
				}catch (Exception e)
				{
					unpinPage(currentDirPageId, false/*undirty*/);
					dirpage = null;
					datapage = null;
					throw e;
				}

				if(dpinfo.pageId.pid==qid.pageNo.pid)
				{
					dirpage.setpage(currentDirPage.getpage());
					dirPageId.pid = currentDirPageId.pid;

					datapage.setpage(currentDataPage.getpage());
					dataPageId.pid = dpinfo.pageId.pid;

					rpDataPageRid.pageNo.pid = currentDataPageRid.pageNo.pid;
					rpDataPageRid.slotNo = currentDataPageRid.slotNo;
					return true;
				}
				else
				{
					// user record not found on this datapage; unpin it
					// and try the next one
					unpinPage(dpinfo.pageId, false /*undirty*/);

				}

			}

			nextDirPageId = currentDirPage.getNextPage();
			try{
				unpinPage(currentDirPageId, false /*undirty*/);
			}
			catch(Exception e) {
				throw new HFException (e, "heapfile,_find,unpinpage failed");
			}

			currentDirPageId.pid = nextDirPageId.pid;
			if(currentDirPageId.pid != INVALID_PAGE)
			{
				pinPage(currentDirPageId, currentDirPage, false/*Rdisk*/);
				if(currentDirPage == null)
					throw new HFException(null, "pinPage return null page");
			}


		}
		dirPageId.pid = dataPageId.pid = INVALID_PAGE;

		return false;


	}

	/** Initialize.  A null name produces a temporary heapfile which will be
	 * deleted by the destructor.  If the name already denotes a file, the
	 * file is opened; otherwise, a new empty file is created.
	 *
	 * @exception HFException heapfile exception
	 * @exception HFBufMgrException exception thrown from bufmgr layer
	 * @exception HFDiskMgrException exception thrown from diskmgr layer
	 * @exception IOException I/O errors
	 */
	public QuadrupleHeapFile(String name)
			throws HFException,
			HFBufMgrException,
			HFDiskMgrException,
			IOException {
		// Give us a prayer of destructing cleanly if construction fails.
		_file_deleted = true;
		_fileName = null;

		if(name == null)
		{
			// If the name is NULL, allocate a temporary name
			// and no logging is required.
			_fileName = "tempQuadrupleHeapFile";
			String useId = new String("user.name");
			String userAccName;
			userAccName = System.getProperty(useId);
			_fileName = _fileName + userAccName;

			String filenum = Integer.toString(tempfilecount);
			_fileName = _fileName + filenum;
			_ftype = TEMP;
			tempfilecount ++;

		}
		else
		{
			_fileName = name;
			_ftype = ORDINARY;
		}

		// The constructor gets run in two different cases.
		// In the first case, the file is new and the header page
		// must be initialized.  This case is detected via a failure
		// in the db->get_file_entry() call.  In the second case, the
		// file already exists and all that must be done is to fetch
		// the header page into the buffer pool

		// try to open the file

		Page apage = new Page();
		_firstDirPageId = null;
		if (_ftype == ORDINARY)
			_firstDirPageId = get_file_entry(_fileName);

		if(_firstDirPageId==null)
		{
			// file doesn't exist. First create it.
			_firstDirPageId = newPage(apage, 1);
			// check error
			if(_firstDirPageId == null)
				throw new HFException(null, "can't new page");

			add_file_entry(_fileName, _firstDirPageId);
			// check error(new exception: Could not add file entry

			HFPage firstDirPage = new HFPage();
			firstDirPage.init(_firstDirPageId, apage);
			PageId pageId = new PageId(INVALID_PAGE);

			firstDirPage.setNextPage(pageId);
			firstDirPage.setPrevPage(pageId);
			unpinPage(_firstDirPageId, true /*dirty*/ );


		}
		_file_deleted = false;
		// ASSERTIONS:
		// - ALL private data members of class Heapfile are valid:
		//
		//  - _firstDirPageId valid
		//  - _fileName valid
		//  - no datapage pinned yet

	} // end of constructor

	/** Return number of records in file.
	 *
	 * @exception InvalidSlotNumberException invalid slot number
	 * @exception InvalidTupleSizeException invalid tuple size
	 * @exception HFBufMgrException exception thrown from bufmgr layer
	 * @exception HFDiskMgrException exception thrown from diskmgr layer
	 * @exception IOException I/O errors
	 */
	public int getQuadrupleCnt()
			throws InvalidSlotNumberException,
			InvalidTupleSizeException,
			HFDiskMgrException,
			HFBufMgrException,
			IOException {
		int answer = 0;
		PageId currentDirPageId = new PageId(_firstDirPageId.pid);

		PageId nextDirPageId = new PageId(0);

		HFPage currentDirPage = new HFPage();
		Page pageinbuffer = new Page();

		while(currentDirPageId.pid != INVALID_PAGE)
		{
			pinPage(currentDirPageId, currentDirPage, false);

			RID rid = new RID();
			Tuple atuple;
			for (rid = currentDirPage.firstRecord();
				 rid != null;	// rid==NULL means no more record
				 rid = currentDirPage.nextRecord(rid))
			{
				atuple = currentDirPage.getRecord(rid);
				DataPageInfo dpinfo = new DataPageInfo(atuple);

				answer += dpinfo.recct;
			}

			// ASSERTIONS: no more record
			// - we have read all datapage records on
			//   the current directory page.

			nextDirPageId = currentDirPage.getNextPage();
			unpinPage(currentDirPageId, false /*undirty*/);
			currentDirPageId.pid = nextDirPageId.pid;
		}

		return answer;
	} // end of getRecCnt

	/** Insert record into file, return its Rid.
	 *
	 * @param quadruplePtr pointer of the record
	 * @exception InvalidSlotNumberException invalid slot number
	 * @exception InvalidTupleSizeException invalid tuple size
	 * @exception SpaceNotAvailableException no space left
	 * @exception HFException heapfile exception
	 * @exception HFBufMgrException exception thrown from bufmgr layer
	 * @exception HFDiskMgrException exception thrown from diskmgr layer
	 * @exception IOException I/O errors
	 *
	 * @return the rid of the record
	 */
	public QID insertQuadruple(byte[] quadruplePtr)
			throws InvalidSlotNumberException,
			InvalidTupleSizeException,
			SpaceNotAvailableException,
			HFException,
			HFBufMgrException,
			HFDiskMgrException,
			IOException
	{
		int dpinfoLen = 0;
		int recLen = quadruplePtr.length;
		boolean found;
		RID currentDataPageRid = new RID();
		Page pageinbuffer = new Page();
		HFPage currentDirPage = new HFPage();
		THFPage currentDataPage = new THFPage();

		HFPage nextDirPage = new HFPage();
		PageId currentDirPageId = new PageId(_firstDirPageId.pid);
		PageId nextDirPageId;

		pinPage(currentDirPageId, currentDirPage, false/*Rdisk*/);

		found = false;
		Tuple atuple;
		DataPageInfo dpinfo = new DataPageInfo();
		while (found == false)
		{
			for (currentDataPageRid = currentDirPage.firstRecord();
				 currentDataPageRid != null;
				 currentDataPageRid =
						 currentDirPage.nextRecord(currentDataPageRid))
			{
				atuple = currentDirPage.getRecord(currentDataPageRid);
				dpinfo = new DataPageInfo(atuple);

				if(recLen <= dpinfo.availspace)
				{
					found = true;
					break;
				}
			}
			if(found == false)
			{
				if(currentDirPage.available_space() >= dpinfo.size)
				{
					currentDataPage = _newDatapage(dpinfo);
					atuple = dpinfo.convertToTuple();

					byte [] tmpData = atuple.getTupleByteArray();
					currentDataPageRid = currentDirPage.insertRecord(tmpData);

					RID tmprid = currentDirPage.firstRecord();

					// need catch error here!
					if(currentDataPageRid == null)
						throw new HFException(null, "no space to insert rec.");

					found = true;

				} //end of IF02
				else
				{
					nextDirPageId = currentDirPage.getNextPage();
					if (nextDirPageId.pid != INVALID_PAGE)
					{
						unpinPage(currentDirPageId, false);
						currentDirPageId.pid = nextDirPageId.pid;
						pinPage(currentDirPageId,
								currentDirPage, false);
					}
					else
					{
						nextDirPageId = newPage(pageinbuffer, 1);
						if(nextDirPageId == null)
							throw new HFException(null, "can't new page");

						nextDirPage.init(nextDirPageId, pageinbuffer);
						PageId temppid = new PageId(INVALID_PAGE);
						nextDirPage.setNextPage(temppid);
						nextDirPage.setPrevPage(currentDirPageId);

						currentDirPage.setNextPage(nextDirPageId);
						unpinPage(currentDirPageId, true/*dirty*/);

						currentDirPageId.pid = nextDirPageId.pid;
						currentDirPage = new HFPage(nextDirPage);
					}
				}

			}
			else
			{
				pinPage(dpinfo.pageId, currentDataPage, false);
			}
		}

		if ((dpinfo.pageId).pid == INVALID_PAGE) // check error!
			throw new HFException(null, "invalid PageId");

		if (!(currentDataPage.available_space() >= recLen))
			throw new SpaceNotAvailableException(null, "no available space");

		if (currentDataPage == null)
			throw new HFException(null, "can't find Data page");

		QID qid;
		qid = currentDataPage.insertRecord(quadruplePtr);

		dpinfo.recct++;
		dpinfo.availspace = currentDataPage.available_space();

		unpinPage(dpinfo.pageId, true /* = DIRTY */);

		// DataPage is now released
		atuple = currentDirPage.returnRecord(currentDataPageRid);
		DataPageInfo dpinfo_ondirpage = new DataPageInfo(atuple);

		dpinfo_ondirpage.availspace = dpinfo.availspace;
		dpinfo_ondirpage.recct = dpinfo.recct;
		dpinfo_ondirpage.pageId.pid = dpinfo.pageId.pid;
		dpinfo_ondirpage.flushToTuple();

		unpinPage(currentDirPageId, true /* = DIRTY */);

		return qid;

	}

	/** Delete record from file with given rid.
	 *
	 * @exception InvalidSlotNumberException invalid slot number
	 * @exception InvalidTupleSizeException invalid tuple size
	 * @exception HFException heapfile exception
	 * @exception HFBufMgrException exception thrown from bufmgr layer
	 * @exception HFDiskMgrException exception thrown from diskmgr layer
	 * @exception Exception other exception
	 *
	 * @return true record deleted  false:record not found
	 */
	public boolean deleteQuadruple(QID qid)
			throws Exception {
		boolean status;
		HFPage currentDirPage = new HFPage();
		PageId currentDirPageId = new PageId();
		THFPage currentDataPage = new THFPage();
		PageId currentDataPageId = new PageId();
		RID currentDataPageRid = new RID();

		status = _findDataPage(qid,
				currentDirPageId, currentDirPage,
				currentDataPageId, currentDataPage,
				currentDataPageRid);
		if(status != true) return status;	// record not found

		// ASSERTIONS:
		// - currentDirPage, currentDirPageId valid and pinned
		// - currentDataPage, currentDataPageid valid and pinned

		// get datapageinfo from the current directory page:
		Tuple atuple;
		atuple = currentDirPage.returnRecord(currentDataPageRid);
		DataPageInfo pdpinfo = new DataPageInfo(atuple);

		// delete the record on the datapage
		currentDataPage.deleteRecord(qid);

		pdpinfo.recct--;
		pdpinfo.flushToTuple();	//Write to the buffer pool
		if (pdpinfo.recct >= 1) {
			// more records remain on datapage so it still hangs around.
			// we just need to modify its directory entry
			pdpinfo.availspace = currentDataPage.available_space();
			pdpinfo.flushToTuple();
			unpinPage(currentDataPageId, true /* = DIRTY*/);
			unpinPage(currentDirPageId, true /* = DIRTY */);
		}
		else {
			// the record is already deleted:
			// we're removing the last record on datapage so free datapage
			// also, free the directory page if
			//   a) it's not the first directory page, and
			//   b) we've removed the last DataPageInfo record on it.

			// delete empty datapage: (does it get unpinned automatically? -NO, Ranjani)
			unpinPage(currentDataPageId, false /*undirty*/);
			freePage(currentDataPageId);

			// delete corresponding DataPageInfo-entry on the directory page:
			// currentDataPageRid points to datapage (from for loop above)
			currentDirPage.deleteRecord(currentDataPageRid);

			// now check whether the directory page is empty:
			currentDataPageRid = currentDirPage.firstRecord();

			// st == OK: we still found a datapageinfo record on this directory page
			PageId pageId;
			pageId = currentDirPage.getPrevPage();
			if((currentDataPageRid == null)&&(pageId.pid != INVALID_PAGE))
			{
				// the directory-page is not the first directory page and it is empty:
				// delete it

				// point previous page around deleted page:

				THFPage prevDirPage = new THFPage();
				pinPage(pageId, prevDirPage, false);
				pageId = currentDirPage.getNextPage();
				prevDirPage.setNextPage(pageId);
				pageId = currentDirPage.getPrevPage();
				unpinPage(pageId, true /* = DIRTY */);

				// set prevPage-pointer of next Page
				pageId = currentDirPage.getNextPage();
				if(pageId.pid != INVALID_PAGE)
				{
					THFPage nextDirPage = new THFPage();
					pageId = currentDirPage.getNextPage();
					pinPage(pageId, nextDirPage, false);
					//nextDirPage.openHFpage(apage);
					pageId = currentDirPage.getPrevPage();
					nextDirPage.setPrevPage(pageId);
					pageId = currentDirPage.getNextPage();
					unpinPage(pageId, true /* = DIRTY */);
				}
				// delete empty directory page: (automatically unpinned?)
				unpinPage(currentDirPageId, false/*undirty*/);
				freePage(currentDirPageId);
			}
			else
			{
				// either (the directory page has at least one more datapagerecord
				// entry) or (it is the first directory page):
				// in both cases we do not delete it, but we have to unpin it:
				unpinPage(currentDirPageId, true /* == DIRTY */);
			}
		}
		return true;
	}

	/** Updates the specified record in the heapfile.
	 * @param qid: the record which needs update
	 * @param newquadruple: the new content of the record
	 *
	 * @exception InvalidSlotNumberException invalid slot number
	 * @exception InvalidUpdateException invalid update on record
	 * @exception InvalidTupleSizeException invalid tuple size
	 * @exception HFException heapfile exception
	 * @exception HFBufMgrException exception thrown from bufmgr layer
	 * @exception HFDiskMgrException exception thrown from diskmgr layer
	 * @exception Exception other exception
	 * @return ture:update success   false: can't find the record
	 */
	public boolean updateQuadruple(QID qid, Quadruple newquadruple)
			throws InvalidSlotNumberException,
			InvalidUpdateException,
			InvalidTupleSizeException,
			HFException,
			HFDiskMgrException,
			HFBufMgrException,
			Exception
	{
		boolean status;
		HFPage dirPage = new HFPage();
		PageId currentDirPageId = new PageId();
		THFPage dataPage = new THFPage();
		PageId currentDataPageId = new PageId();
		RID currentDataPageRid = new RID();

		status = _findDataPage(qid,
				currentDirPageId, dirPage,
				currentDataPageId, dataPage,
				currentDataPageRid);

		if(status != true) return status;	// record not found
		Quadruple aquad = dataPage.returnRecord(qid);

		// new copy of this record fits in old space;
		aquad.quadrupleCopy(newquadruple);
		unpinPage(currentDataPageId, true /* = DIRTY */);

		unpinPage(currentDirPageId, false /*undirty*/);


		return true;
	}



	/** Read record from file, returning pointer and length.
	 * @param qid Record ID
	 *
	 * @exception InvalidSlotNumberException invalid slot number
	 * @exception InvalidTupleSizeException invalid tuple size
	 * @exception SpaceNotAvailableException no space left
	 * @exception HFException heapfile exception
	 * @exception HFBufMgrException exception thrown from bufmgr layer
	 * @exception HFDiskMgrException exception thrown from diskmgr layer
	 * @exception Exception other exception
	 *
	 * @return a Quadruple. if Quadruple==null, no more tuple
	 */
	public  Quadruple getQuadruple(QID qid)
			throws InvalidSlotNumberException,
			InvalidTupleSizeException,
			HFException,
			HFDiskMgrException,
			HFBufMgrException,
			Exception
	{
		boolean status;
		HFPage dirPage = new HFPage();
		PageId currentDirPageId = new PageId();
		THFPage dataPage = new THFPage();
		PageId currentDataPageId = new PageId();
		RID currentDataPageRid = new RID();

		status = _findDataPage(qid,
				currentDirPageId, dirPage,
				currentDataPageId, dataPage,
				currentDataPageRid);

		if(status != true) return null; // record not found

		Quadruple aquad = dataPage.getRecord(qid);

		/*
		 * getRecord has copied the contents of qid into recPtr and fixed up
		 * recLen also.  We simply have to unpin dirpage and datapage which
		 * were originally pinned by _findDataPage.
		 */
		unpinPage(currentDataPageId,false /*undirty*/);
		unpinPage(currentDirPageId,false /*undirty*/);

		return  aquad;  //(true?)OK, but the caller need check if atuple==NULL
	}



	/** Initiate a sequential scan.
	 * @exception InvalidTupleSizeException Invalid tuple size
	 * @exception IOException I/O errors
	 *
	 */
	public TScan openScan()
			throws InvalidTupleSizeException,
			IOException
	{
		TScan newscan = new TScan(this);
		return newscan;
	}



	/** Delete the file from the database.
	 *
	 * @exception InvalidSlotNumberException invalid slot number
	 * @exception InvalidTupleSizeException invalid tuple size
	 * @exception FileAlreadyDeletedException file is deleted already
	 * @exception HFBufMgrException exception thrown from bufmgr layer
	 * @exception HFDiskMgrException exception thrown from diskmgr layer
	 * @exception IOException I/O errors
	 */
	public void deleteFile()
			throws InvalidSlotNumberException,
			FileAlreadyDeletedException,
			InvalidTupleSizeException,
			HFBufMgrException,
			HFDiskMgrException,
			IOException
	{
		if(_file_deleted )
			throw new FileAlreadyDeletedException(null, "file already deleted");
		// Mark the deleted flag (even if it doesn't get all the way done).
		_file_deleted = true;

		// Deallocate all data pages
		PageId currentDirPageId = new PageId();
		currentDirPageId.pid = _firstDirPageId.pid;
		PageId nextDirPageId = new PageId();
		nextDirPageId.pid = 0;
		Page pageinbuffer = new Page();
		THFPage currentDirPage =  new THFPage();
		Quadruple aquad;

		pinPage(currentDirPageId, currentDirPage, false);

		QID qid = new QID();
		while(currentDirPageId.pid != INVALID_PAGE)
		{
			for(qid = currentDirPage.firstRecord(); //TO-Modify
				qid != null;
				qid = currentDirPage.nextRecord(qid)) //TO-Modify
			{
				aquad = currentDirPage.getRecord(qid); //TO-Modify
				DataPageInfo dpinfo = new DataPageInfo( aquad); //Change class
				freePage(dpinfo.pageId);
			}
			// ASSERTIONS:
			// - we have freePage()'d all data pages referenced by
			// the current directory page.
			nextDirPageId = currentDirPage.getNextPage();
			freePage(currentDirPageId);
			currentDirPageId.pid = nextDirPageId.pid;

			if (nextDirPageId.pid != INVALID_PAGE)
			{
				pinPage(currentDirPageId, currentDirPage, false);
				//currentDirPage.openHFpage(pageinbuffer);
			}
		}
		delete_file_entry( _fileName );
	}

	/**
	 * short cut to access the pinPage function in bufmgr package.
	 * @see bufmgr.pinPage
	 */
	private void pinPage(PageId pageno, Page page, boolean emptyPage)
			throws HFBufMgrException {

		try {
			SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
		}
		catch (Exception e) {
			throw new HFBufMgrException(e,"QuadrupleHeapFile.java: pinPage() failed");
		}

	} // end of pinPage

	/**
	 * short cut to access the unpinPage function in bufmgr package.
	 * @see bufmgr.unpinPage
	 */
	private void unpinPage(PageId pageno, boolean dirty)
			throws HFBufMgrException {

		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
		}
		catch (Exception e) {
			throw new HFBufMgrException(e,"QuadrupleHeapFile.java: unpinPage() failed");
		}

	} // end of unpinPage

	private void freePage(PageId pageno)
			throws HFBufMgrException {

		try {
			SystemDefs.JavabaseBM.freePage(pageno);
		}
		catch (Exception e) {
			throw new HFBufMgrException(e,"QuadrupleHeapFile.java: freePage() failed");
		}

	} // end of freePage

	private PageId newPage(Page page, int num)
			throws HFBufMgrException {

		PageId tmpId = new PageId();

		try {
			tmpId = SystemDefs.JavabaseBM.newPage(page,num);
		}
		catch (Exception e) {
			throw new HFBufMgrException(e,"QuadrupleHeapFile.java: newPage() failed");
		}

		return tmpId;

	} // end of newPage

	private PageId get_file_entry(String filename)
			throws HFDiskMgrException {

		PageId tmpId = new PageId();

		try {
			tmpId = SystemDefs.JavabaseDB.get_file_entry(filename);
		}
		catch (Exception e) {
			throw new HFDiskMgrException(e,"QuadrupleHeapFile.java: get_file_entry() failed");
		}

		return tmpId;

	} // end of get_file_entry

	private void add_file_entry(String filename, PageId pageno)
			throws HFDiskMgrException {

		try {
			SystemDefs.JavabaseDB.add_file_entry(filename,pageno);
		}
		catch (Exception e) {
			throw new HFDiskMgrException(e,"QuadrupleHeapFile.java: add_file_entry() failed");
		}

	} // end of add_file_entry

	private void delete_file_entry(String filename)
			throws HFDiskMgrException {

		try {
			SystemDefs.JavabaseDB.delete_file_entry(filename);
		}
		catch (Exception e) {
			throw new HFDiskMgrException(e,"QuadrupleHeapFile.java: delete_file_entry() failed");
		}

	} // end of delete_file_entry



}// End of HeapFile 
