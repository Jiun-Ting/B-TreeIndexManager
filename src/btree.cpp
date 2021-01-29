/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/page_not_pinned_exception.h"


 //#define DEBUG

namespace badgerdb
{

	// -----------------------------------------------------------------------------
	// BTreeIndex::BTreeIndex -- Constructor
	// -----------------------------------------------------------------------------

	BTreeIndex::BTreeIndex(const std::string& relationName,
		std::string& outIndexName,
		BufMgr* bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
	{
		// Generate the name of index file
		std::ostringstream idxStr;
		idxStr << relationName << '.' << attrByteOffset;
		std::string indexName = idxStr.str();
		outIndexName = indexName;

		bufMgr = bufMgrIn;
		scanExecuting = false;
		IndexMetaInfo* metainfo;
		Page* header_page;
		Page* root_page;

		try {
			// file exist: open
			file = new BlobFile(outIndexName, false);

			// get the header page number and thus meta info
			headerPageNum = file->getFirstPageNo();
			bufMgr->readPage(file, headerPageNum, header_page);
			metainfo = (IndexMetaInfo*)header_page;
			rootPageNum = metainfo->rootPageNo;

			bufMgr->unPinPage(file, headerPageNum, false);
		}
		catch (FileNotFoundException e)
		{
			// non-exist: create new file
			file = new BlobFile(outIndexName, true);

			// allocate new header page and root page
			bufMgr->allocPage(file, headerPageNum, header_page);
			bufMgr->allocPage(file, rootPageNum, root_page);

			// fill in metainfo
			metainfo = (IndexMetaInfo*)header_page;
			snprintf(metainfo->relationName, sizeof(metainfo->relationName), "%s", relationName.c_str());
			metainfo->attrByteOffset = attrByteOffset;
			metainfo->attrType = attrType;
			metainfo->rootPageNo = rootPageNum;

			// To label if the root was split
			if_rootleaf = 1;

			// initiaize root
			LeafNodeInt* root = (LeafNodeInt*)root_page;
			root->rightSibPageNo = 0;
			for (int i = 0; i < INTARRAYLEAFSIZE; i++) {
				root->ridArray[i].page_number = 0;
				root->keyArray[i] = 0;
			}

			bufMgr->unPinPage(file, headerPageNum, true);
			bufMgr->unPinPage(file, rootPageNum, true);

			// Scan relation and insert entries for all tuples into index
			try {
				FileScan fileScan(relationName, bufMgr);
				RecordId rid = {};
				while (true) {
					fileScan.scanNext(rid);
					std::string record = fileScan.getRecord();
					insertEntry(record.c_str() + attrByteOffset, rid);
				}
			}
			catch (EndOfFileException e) {
				// save Btee index file to disk
				bufMgr->flushFile(file);
			}
		}
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::~BTreeIndex -- destructor
	// -----------------------------------------------------------------------------

	BTreeIndex::~BTreeIndex()
	{
		// End the scan
		scanExecuting = false;

		// Unpin pinned pages
		try {
			bufMgr->unPinPage(file, currentPageNum, false);
		}
		catch (PageNotPinnedException & e) {}

		// Flush index file if exist
		if (file) {
			bufMgr->flushFile(file);
		}
		// Delete index file 
		delete this->file;
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::insertEntry
	// -----------------------------------------------------------------------------

	const void BTreeIndex::insertEntry(const void* key, const RecordId rid)
	{
		if (key == nullptr) {
			return;
		}

		// Declare and initialize entries and get root information
		RIDKeyPair<int> entry;
		entry.set(rid, *((int*)key));
		Page* root_page;
		bufMgr->readPage(file, rootPageNum, root_page);
		PageKeyPair<int>* newchildentry = nullptr;

		// implement the alogorithm from cowbook
		insert(root_page, rootPageNum, if_rootleaf, entry, newchildentry);
	}

	void BTreeIndex::insert(Page* currentPage, PageId currentPageNum, bool isLeaf, const RIDKeyPair<int> entry, PageKeyPair<int>*& newchildentry)
	{
		if (!isLeaf) {
			NonLeafNodeInt* current_nonleaf = (NonLeafNodeInt*)currentPage;
			Page* nextPage;
			PageId nextNodeNum;

			// choose subtree: get the page number of next node to visit
			ChooseSubtree(current_nonleaf, nextNodeNum, entry.key);

			// recursively insert entry
			bufMgr->readPage(file, nextNodeNum, nextPage);
			insert(nextPage, nextNodeNum, current_nonleaf->level == 1, entry, newchildentry);

			if (newchildentry == nullptr) {
				// didn't split child
				bufMgr->unPinPage(file, currentPageNum, false);
				return;
			}
			else {
				// if the current node has space
				if (current_nonleaf->pageNoArray[INTARRAYNONLEAFSIZE] == 0 &&
					current_nonleaf->keyArray[INTARRAYNONLEAFSIZE - 1] == 0) {
					insert_nonLeaf(current_nonleaf, newchildentry);
					newchildentry = nullptr;
					bufMgr->unPinPage(file, currentPageNum, true);
					return;
				}
				else {
					split_nonLeaf(current_nonleaf, currentPageNum, newchildentry);

					// if the current node is the root
					if (currentPageNum == rootPageNum) {
						new_root(currentPageNum, newchildentry);
					}
					return;
				}
			}
		}
		else {
			// current node is a leaf
			LeafNodeInt* current_leaf = (LeafNodeInt*)currentPage;
			// if the leaf has space
			if (current_leaf->ridArray[INTARRAYLEAFSIZE - 1].page_number == 0
				&& current_leaf->keyArray[INTARRAYLEAFSIZE - 1] == 0) {
				// put entry here
				insert_Leaf(current_leaf, entry);

				// set newchildentry to null
				newchildentry = nullptr;
				bufMgr->unPinPage(file, currentPageNum, true);
				return;
			}
			else {
				// split leaf
				split_Leaf(current_leaf, currentPageNum, newchildentry, entry);

				// if the leaf is root then create new root after split
				if (currentPageNum == rootPageNum) {
					new_root(currentPageNum, newchildentry);
				}
				return;
			}
		}
	}

	void BTreeIndex::ChooseSubtree(NonLeafNodeInt* Node, PageId& nextpageNum, int key)
	{
		int idx = 0;
		while (idx < INTARRAYNONLEAFSIZE &&
			Node->keyArray[idx] <= key &&
			Node->pageNoArray[idx + 1] != 0
			) {
			idx++;
		}
		nextpageNum = Node->pageNoArray[idx];
	}

	void BTreeIndex::insert_Leaf(LeafNodeInt* leaf, RIDKeyPair<int> entry) {
		if (leaf->ridArray[0].page_number == 0 &&
			leaf->keyArray[0] == 0) {
			leaf->keyArray[0] = entry.key;
			leaf->ridArray[0] = entry.rid;
		}
		else {
			int End = 0;
			while (leaf->ridArray[End + 1].page_number != 0) {
				End++;
			}

			int idx;
			for (idx = End; ((leaf->keyArray[idx] > entry.key) && idx >= 0); idx--) {
				leaf->keyArray[idx + 1] = leaf->keyArray[idx];
				leaf->ridArray[idx + 1] = leaf->ridArray[idx];
			}

			leaf->keyArray[idx + 1] = entry.key;
			leaf->ridArray[idx + 1] = entry.rid;
		}
	}

	void BTreeIndex::insert_nonLeaf(NonLeafNodeInt* nonleaf, PageKeyPair<int>* entry) {
		// locate the first occupied position
		int End = 0;
		while (nonleaf->pageNoArray[End + 1] != 0) {
			End++;
		}

		// shift the elements from the acquired position
		int idx;
		for (idx = End; ((nonleaf->keyArray[idx - 1] > entry->key) && idx >= 0); idx--) {
			nonleaf->keyArray[idx] = nonleaf->keyArray[idx - 1];
			nonleaf->pageNoArray[idx + 1] = nonleaf->pageNoArray[idx];
		}

		nonleaf->keyArray[idx] = entry->key;
		nonleaf->pageNoArray[idx + 1] = entry->pageNo;
	}

	void BTreeIndex::split_nonLeaf(NonLeafNodeInt* nonleaf, PageId oldPageNum, PageKeyPair<int>*& newchildentry) {
		// Create nonleaf node
		Page* Page;
		PageId PageNum;
		bufMgr->allocPage(file, PageNum, Page);
		NonLeafNodeInt* new_nonleaf = (NonLeafNodeInt*)Page;
		
		for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
			new_nonleaf->keyArray[i] = 0;
			new_nonleaf->pageNoArray[i] = 0;
		}
		new_nonleaf->pageNoArray[INTARRAYNONLEAFSIZE] = 0;
		new_nonleaf->level = nonleaf->level;

		int mid = INTARRAYNONLEAFSIZE / 2;
		if ((INTARRAYNONLEAFSIZE % 2 == 0) && (newchildentry->key < nonleaf->keyArray[mid]))
		{
			mid--;
		}
		// pushup the mid: copy and remove
		PageKeyPair<int> copyup;
		copyup.set(PageNum, nonleaf->keyArray[mid]);
		nonleaf->keyArray[mid] = 0;
		nonleaf->pageNoArray[mid] = 0;

		int move_start = mid + 1;
		new_nonleaf->pageNoArray[0] = nonleaf->pageNoArray[move_start];
		// initialize and copy elements from old node
		for (int i = move_start; i < INTARRAYNONLEAFSIZE; i++) {
			new_nonleaf->keyArray[i - move_start] = nonleaf->keyArray[i];
			new_nonleaf->pageNoArray[i - move_start+1] = nonleaf->pageNoArray[i + 1];
			nonleaf->pageNoArray[i] = 0;
			nonleaf->keyArray[i] = 0;
		}
		nonleaf->pageNoArray[INTARRAYNONLEAFSIZE] = 0;

		
		// insert the new child entry
		if (newchildentry->key < new_nonleaf->keyArray[0]) {
			insert_nonLeaf(nonleaf, newchildentry);
		}
		else {
			insert_nonLeaf(new_nonleaf, newchildentry);
		}

		newchildentry = &copyup;
		bufMgr->unPinPage(file, oldPageNum, true);
		bufMgr->unPinPage(file, PageNum, true);
	}
	void BTreeIndex::new_root(PageId old_rootPageNum, PageKeyPair<int>* newchildentry)
	{
		// create and initialize a new root
		PageId new_rootPageNum;
		Page* new_root;
		bufMgr->allocPage(file, new_rootPageNum, new_root);
		NonLeafNodeInt* new_rootPage = (NonLeafNodeInt*)new_root;

		for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
			new_rootPage->pageNoArray[i] = 0;
			new_rootPage->keyArray[i] = 0;
		}
		new_rootPage->pageNoArray[INTARRAYNONLEAFSIZE] = 0;

		new_rootPage->level = if_rootleaf;
		new_rootPage->pageNoArray[0] = old_rootPageNum;
		new_rootPage->pageNoArray[1] = newchildentry->pageNo;
		new_rootPage->keyArray[0] = newchildentry->key;

		// update meta info
		Page* metainfo;
		bufMgr->readPage(file, headerPageNum, metainfo);
		IndexMetaInfo* metaPage = (IndexMetaInfo*)metainfo;
		metaPage->rootPageNo = new_rootPageNum;
		rootPageNum = new_rootPageNum;
		if_rootleaf = 0;

		bufMgr->unPinPage(file, headerPageNum, true);
		bufMgr->unPinPage(file, new_rootPageNum, true);
	}

	void BTreeIndex::split_Leaf(LeafNodeInt* leaf, PageId leafPageNum, PageKeyPair<int>*& newchildentry, const RIDKeyPair<int> entry) {
		// Create a new leaf, initialize, and copy from old leaf
		Page* Page;
		PageId PageNum;

		bufMgr->allocPage(file, PageNum, Page);
		LeafNodeInt* newLeaf = (LeafNodeInt*)Page;
		for (int i = 0; i < INTARRAYLEAFSIZE; i++) {
			newLeaf->keyArray[i] = 0;
			newLeaf->ridArray[i].page_number = 0;
		}


		int mid = INTARRAYLEAFSIZE / 2;
		if (INTARRAYLEAFSIZE % 2 == 1 && entry.key > leaf->keyArray[mid]){
			mid = mid + 1;
		}
		for (int i = mid; i < INTARRAYLEAFSIZE; i++) {
			newLeaf->keyArray[i - mid] = leaf->keyArray[i];
			newLeaf->ridArray[i - mid] = leaf->ridArray[i];
			leaf->keyArray[i] = 0;
			leaf->ridArray[i].page_number = 0;
		}

		if (entry.key > leaf->keyArray[mid - 1]) {
			insert_Leaf(newLeaf, entry);
		}
		else {
			insert_Leaf(leaf, entry);
		}

		PageKeyPair<int> smallest_newLeaf;
		smallest_newLeaf.set(PageNum, newLeaf->keyArray[0]);
		newchildentry = &smallest_newLeaf;

		// set sibling pointers
		newLeaf->rightSibPageNo = leaf->rightSibPageNo;
		leaf->rightSibPageNo = PageNum;

		bufMgr->unPinPage(file, leafPageNum, true);
		bufMgr->unPinPage(file, PageNum, true);
	}



	// -----------------------------------------------------------------------------
	// BTreeIndex::startScan
	// -----------------------------------------------------------------------------

	const void BTreeIndex::startScan(const void* lowValParm,
		const Operator lowOpParm,
		const void* highValParm,
		const Operator highOpParm)
	{
		// Check the validity of operator
		if ((lowOpParm != GT && lowOpParm != GTE) || (highOpParm != LT && highOpParm != LTE)) {
			throw BadOpcodesException();
		}

		// Check the validity of range
		lowValInt = *(int*)lowValParm;
		highValInt = *(int*)highValParm;
		if (lowValInt > highValInt)
			throw BadScanrangeException();

		// End another scan
		if (scanExecuting) {
			endScan();
		}

		// Set up all the variables for scan
		scanExecuting = true;
		lowOp = lowOpParm;
		highOp = highOpParm;

		// Search recursive from root to find out the leaf page that contains the first RecordID
		tree_search(rootPageNum);
	}

	void BTreeIndex::tree_search(PageId PageNum) {

		// Read content of page and cast it to node
		currentPageNum = PageNum;
		bufMgr->readPage(file, currentPageNum, currentPageData);
		if (if_rootleaf) {
			LeafNodeInt* LeafNode = (LeafNodeInt*)currentPageData;
			bufMgr->unPinPage(file, currentPageNum, false);

			nextEntry = 0;
			while (((lowOp == GT && LeafNode->keyArray[nextEntry] <= lowValInt)
				|| (lowOp == GTE && LeafNode->keyArray[nextEntry] < lowValInt))
				&& (nextEntry < INTARRAYLEAFSIZE)
				&& (LeafNode->ridArray[nextEntry].page_number != 0)) {
				nextEntry++;
			}

			// Check if the scan criteria fit any index 
			if ((lowOp == GT && LeafNode->keyArray[nextEntry] <= lowValInt)
				|| (lowOp == GTE && LeafNode->keyArray[nextEntry] < lowValInt)
				|| (highOp == LT && LeafNode->keyArray[nextEntry] >= highValInt)
				|| (highOp == LTE && LeafNode->keyArray[nextEntry] > highValInt)) {
				throw  NoSuchKeyFoundException();
			}

		}
		else {
			NonLeafNodeInt* Non_LeafNode = (NonLeafNodeInt*)currentPageData;

			// Compare the target value with Non-leafnode, find the position to go deep
			int NodeEntry = 0;
			while (((lowOp == GT && Non_LeafNode->keyArray[NodeEntry] <= lowValInt)
				|| (lowOp == GTE && Non_LeafNode->keyArray[NodeEntry] < lowValInt))
				&& (NodeEntry < INTARRAYNONLEAFSIZE)
				&& Non_LeafNode->pageNoArray[NodeEntry + 1] != 0) {
				NodeEntry++;
			}

			if (Non_LeafNode->level == 1) {
				try {
					bufMgr->unPinPage(file, currentPageNum, false);
				}
				catch (PageNotPinnedException & e) {}

				// Get the leaf node and the initial entry
				currentPageNum = Non_LeafNode->pageNoArray[NodeEntry];
				bufMgr->readPage(file, currentPageNum, currentPageData);
				LeafNodeInt* LeafNode = (LeafNodeInt*)currentPageData;

				nextEntry = 0;
				while (((lowOp == GT && LeafNode->keyArray[nextEntry] <= lowValInt)
					|| (lowOp == GTE && LeafNode->keyArray[nextEntry] < lowValInt))
					&& (nextEntry < INTARRAYLEAFSIZE)
					&& (LeafNode->ridArray[nextEntry].page_number != 0)) {
					nextEntry++;
				}

				// Check if the scan criteria fit any index 
				if ((lowOp == GT && LeafNode->keyArray[nextEntry] <= lowValInt)
					|| (lowOp == GTE && LeafNode->keyArray[nextEntry] < lowValInt)
					|| (highOp == LT && LeafNode->keyArray[nextEntry] >= highValInt)
					|| (highOp == LTE && LeafNode->keyArray[nextEntry] > highValInt)) {
					throw  NoSuchKeyFoundException();
				}
			}
			else {
				// Go deeper
				try {
					bufMgr->unPinPage(file, currentPageNum, false);
				}
				catch (PageNotPinnedException & e) {}
				tree_search(Non_LeafNode->pageNoArray[NodeEntry]);
			}
		}
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::scanNext
	// -----------------------------------------------------------------------------

	const void BTreeIndex::scanNext(RecordId& outRid)
	{
		// Check if the scan has been initialized
		if (!scanExecuting)
			throw ScanNotInitializedException();

		// Get the current node
		LeafNodeInt* LeafNode = (LeafNodeInt*)currentPageData;

		if ((nextEntry == INTARRAYLEAFSIZE)
			|| (LeafNode->ridArray[nextEntry].page_number == 0)) {

			// jump to the right sibling
			try {
				bufMgr->unPinPage(file, currentPageNum, false);
			}
			catch (PageNotPinnedException & e) {}

			PageId right_sib = LeafNode->rightSibPageNo;

			// Check that the right sibling is a valid leaf page
			if (right_sib == 0)
				throw IndexScanCompletedException();

			currentPageNum = right_sib;
			bufMgr->readPage(file, currentPageNum, currentPageData);
			LeafNode = (LeafNodeInt*)currentPageData;
			nextEntry = 0;
		}
		else if ((highOp == LT && LeafNode->keyArray[nextEntry] >= highValInt)
			|| (highOp == LTE && LeafNode->keyArray[nextEntry] > highValInt)) {
			// exit if no more records satisfied the scan criteria
			throw IndexScanCompletedException();
		}

		outRid = LeafNode->ridArray[nextEntry];
		nextEntry++;
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::endScan
	// -----------------------------------------------------------------------------
	//
	const void BTreeIndex::endScan()
	{
		// if called before a successful ​startScan​ call
		if (!scanExecuting)
			throw ScanNotInitializedException();

		// terminate the current scan
		scanExecuting = false;

		try {
			bufMgr->unPinPage(file, currentPageNum, false);
		}
		catch (PageNotPinnedException & e) {}
	}

}
