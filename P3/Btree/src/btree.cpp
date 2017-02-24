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

#include <algorithm>
#include <vector>
//#define DEBUG

#ifdef DEBUG
#include <iostream>
using namespace std;
#endif
namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
  std::ostringstream	idxStr;
  idxStr << relationName << '.' << attrByteOffset;
  std::string	indexName = idxStr.str();  //  indexName is the name of the index file
  outIndexName = indexName;
  this->bufMgr = bufMgrIn;
  this->attrByteOffset = attrByteOffset;
  this->attributeType = attrType;

  if (true == badgerdb::BlobFile::exists(indexName)) {
    this->file = new BlobFile(indexName, false);
    this->headerPageNum = this->file->getFirstPageNo();
    Page *headerPage;
    this->bufMgr->readPage(this->file, this->headerPageNum, headerPage);
    IndexMetaInfo* metaData = reinterpret_cast<IndexMetaInfo*>(headerPage);
    this->rootPageNum = metaData->rootPageNo;
    this->bufMgr->unPinPage(this->file, this->headerPageNum, false);
  } else {
    this->file = new BlobFile(indexName, true);

    Page *headerPage, *rootPage;

    this->bufMgr->allocPage(this->file, this->headerPageNum, headerPage);
    this->bufMgr->allocPage(this->file, this->rootPageNum, rootPage);
    IndexMetaInfo metaData;
    metaData.attrByteOffset = this->attrByteOffset;
    metaData.attrType = this->attributeType;
    strncpy(metaData.relationName, relationName.c_str(), sizeof(metaData.relationName)-1);
    metaData.relationName[19] = '\0';
    metaData.rootPageNo = this->rootPageNum;
    memcpy(headerPage, &metaData, sizeof(IndexMetaInfo));
    this->bufMgr->unPinPage(this->file, this->headerPageNum, true);

    NonLeafNodeInt rootData;
    memset(&rootData, 0, sizeof(NonLeafNodeInt));
    rootData.level = 1;
    memcpy(rootPage, &rootData, sizeof(NonLeafNodeInt));
    this->bufMgr->unPinPage(this->file, this->rootPageNum, true);

    {
      FileScan fscan(relationName, bufMgr);
      try
      {
        RecordId scanRid;
        while(1)
        {
          fscan.scanNext(scanRid);
          //Assuming RECORD.i is our key, lets extract the key, which we know is INTEGER and whose byte offset is also know inside the record.
          std::string recordStr = fscan.getRecord();
          const char *record = recordStr.c_str();
          int key = *((int *)(record + offsetof (RECORD, i)));
          this->insertEntry(&key, scanRid);
        }
      }
      catch(EndOfFileException e) { }
    }
    // filescan goes out of scope here, so relation file gets closed.
  }
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
  this->bufMgr->flushFile(this->file);
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
  Page* rootPage;
  int keyValue = *reinterpret_cast<int*>(const_cast<void*>(key));
  this->bufMgr->readPage(this->file, this->rootPageNum, rootPage);
  NonLeafNodeInt* rootData = reinterpret_cast<NonLeafNodeInt*>(rootPage);
  if (rootData->pageNoArray[0] == Page::INVALID_NUMBER) {
    Page* lessKey, *greaterKey;
    this->bufMgr->allocPage(this->file, rootData->pageNoArray[0], lessKey);
    this->bufMgr->allocPage(this->file, rootData->pageNoArray[1], greaterKey);
    
    LeafNodeInt dataPageLeft, dataPageRight;
    memset(&dataPageLeft, 0, sizeof(LeafNodeInt));
    memset(&dataPageRight, 0, sizeof(LeafNodeInt));
    
    dataPageLeft.rightSibPageNo = rootData->pageNoArray[1];
    
    memcpy(lessKey, &dataPageLeft, sizeof(LeafNodeInt));
    this->bufMgr->unPinPage(this->file, rootData->pageNoArray[0], true);

    dataPageRight.keyArray[0] = keyValue;
    dataPageRight.ridArray[0] = rid;
    memcpy(greaterKey, &dataPageRight, sizeof(LeafNodeInt));
    this->bufMgr->unPinPage(this->file, rootData->pageNoArray[1], true);

    rootData->level = 2;
    this->bufMgr->unPinPage(this->file, this->rootPageNum, true);
  } else {
    PageId dataPageNum;
    PageId dataPageNumPrev;
    Page* tempPage;
    int insertAt = -1, endOfRecordsOffset;
    getPageNoAndOffsetOfKeyInsert(key, rootPage, dataPageNum, insertAt, endOfRecordsOffset, dataPageNumPrev);
    this->bufMgr->readPage(this->file, dataPageNum, tempPage);
    LeafNodeInt* dataPage = reinterpret_cast<LeafNodeInt*>(tempPage);
    
    // TODO karantalreja : code to move elements to next page
    for (int j = endOfRecordsOffset; j > insertAt; j--) {
      dataPage->ridArray[j] = dataPage->ridArray[j-1];
      dataPage->keyArray[j] = dataPage->keyArray[j-1];
    }
    dataPage->ridArray[insertAt] = rid;
    dataPage->keyArray[insertAt] = keyValue;
    this->bufMgr->unPinPage(this->file, dataPageNum, true);
#ifdef DEBUG
    cout << "DBG: Key " << keyValue << " inserted on page " << dataPageNum << " at offset " << insertAt << ":" << endOfRecordsOffset << endl; 
#endif
  }

}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
  this->scanExecuting = true;
  this->lowOp = lowOpParm;
  this->highOp = highOpParm;
  switch (this->attributeType) {
    case INTEGER:
    {
      this->lowValInt = *reinterpret_cast<int*>(const_cast<void*>(lowValParm));
      this->highValInt = *reinterpret_cast<int*>(const_cast<void*>(highValParm));
      //if (lowValInt > highValInt) throw BadScanrangeException;
      break;  
    }
    case DOUBLE:
    {
      break;  
    }
    case STRING:
    {
      break;  
    }
    default:
      break;
  }
  Page* rootPage;
  this->bufMgr->readPage(this->file, this->rootPageNum, rootPage);
  int insertAt, endOfRecordsOffset;
  PageId dataPageNum, dataPageNumPrev;
  this->getPageNoAndOffsetOfKeyInsert(lowValParm, rootPage, dataPageNum, insertAt, endOfRecordsOffset, dataPageNumPrev, false);
  if (dataPageNumPrev == dataPageNum) {
    this->currentPageNum = dataPageNum;
    this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
    this->nextEntry = insertAt;
    LeafNodeInt* dataPage = reinterpret_cast<LeafNodeInt*>(this->currentPageData);
    if (this->lowOp == GT) {
      if (dataPage->keyArray[this->nextEntry] == lowValInt) {
        if (this->nextEntry + 1 == INTARRAYLEAFSIZE) {
          this->nextEntry = 0;
          this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
          this->currentPageNum = dataPage->rightSibPageNo;
          this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
        } else this->nextEntry++;
      }
    }
    if (dataPage->keyArray[this->nextEntry] > highValInt) throw NoSuchKeyFoundException();
    else if (this->highOp == LT && dataPage->keyArray[this->nextEntry] == highValInt) throw NoSuchKeyFoundException();
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{
  LeafNodeInt* dataPage = reinterpret_cast<LeafNodeInt*>(this->currentPageData);
  if (this->highOp == LT && dataPage->keyArray[this->nextEntry] >= this->highValInt) throw IndexScanCompletedException();
  if (this->highOp == LTE && dataPage->keyArray[this->nextEntry] > this->highValInt) throw IndexScanCompletedException();
  outRid = dataPage->ridArray[this->nextEntry];
  if (this->nextEntry + 1 == INTARRAYLEAFSIZE) {
    this->nextEntry = 0;
    this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
    this->currentPageNum = dataPage->rightSibPageNo;
    if (this->currentPageNum == Page::INVALID_NUMBER) throw IndexScanCompletedException();
    this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
  } else this->nextEntry++;
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{
  this->scanExecuting = false;

}

void BTreeIndex::getPageNoAndOffsetOfKeyInsert(const void* key, Page* rootPage, PageId& pageNo, int& insertAt, int& endOfRecordsOffset, PageId& lastPageNo, bool insert)
{
  int i = 0, depth = 1;
  int keyValue = *reinterpret_cast<int*>(const_cast<void*>(key));
  NonLeafNodeInt* rootData = reinterpret_cast<NonLeafNodeInt*>(rootPage);
  NonLeafNodeInt* currPage = rootData;
  std::vector<std::pair<int,PageId>> pathOfTraversal;
  PageId lastPage = this->rootPageNum;
  Page* tempPage;
  while (depth < rootData->level) {
    if (keyValue < currPage->keyArray[0]) {
      i = 0;
    } else {
      for (i = 1; i < INTARRAYNONLEAFSIZE; i++) {
        if (currPage->pageNoArray[i] == Page::INVALID_NUMBER) {
          pathOfTraversal.push_back(std::pair<int,PageId>(i, lastPage));
          break;
        }
        if (currPage->keyArray[i] < keyValue) {
          if (currPage->pageNoArray[i+1] != Page::INVALID_NUMBER) //TODO karantalreja _split || i+1 == INTARRAYNONLEAFSIZE)
            continue; // means if this was the last page in the node, we need to add to this page only otherwise continue
        }
        pathOfTraversal.push_back(std::pair<int,PageId>(i+1, lastPage));
        break;
      }
#ifdef DEBUG
    assert(i != INTARRAYNONLEAFSIZE);
#endif
    }
    // TODO karantalreja : if i == INTARRAYNONLEAFSIZE then need to split page
    this->bufMgr->unPinPage(this->file, lastPage, false);
    this->bufMgr->readPage(this->file, currPage->pageNoArray[i], tempPage);
    lastPage = currPage->pageNoArray[i];
    currPage = reinterpret_cast<NonLeafNodeInt*>(tempPage);
    depth++;
  }
  pageNo = lastPage;
  i = 0;
  insertAt = -1;
  LeafNodeInt* dataPage = reinterpret_cast<LeafNodeInt*>(currPage);
  for (i = 0; i < INTARRAYLEAFSIZE; i++) {
    if (dataPage->ridArray[i].page_number == Page::INVALID_NUMBER) {
      insertAt = i;
      break;
    }
    if (keyValue > dataPage->keyArray[i]) continue;
    if (insertAt == -1) {
      insertAt = i;
      if (insert == false) break;
    }
  }
  if (i == INTARRAYLEAFSIZE) {
    // split data page
    PageId parentPageId = pathOfTraversal.back().second;
    Page* parentPage;
    this->bufMgr->readPage(this->file, parentPageId, parentPage);
    NonLeafNodeInt* parentData = reinterpret_cast<NonLeafNodeInt*>(parentPage);
    int offset = pathOfTraversal.back().first; // TODO karantalreja : Handle non-append mode
    Page *greaterKey;
    this->bufMgr->allocPage(this->file, parentData->pageNoArray[offset], greaterKey);
    dataPage->rightSibPageNo = parentData->pageNoArray[offset];
    parentData->keyArray[offset-1] = keyValue;
    this->bufMgr->unPinPage(this->file, lastPage, true);
    this->bufMgr->unPinPage(this->file, parentPageId, true);
    this->bufMgr->unPinPage(this->file, parentData->pageNoArray[offset], false);
    lastPageNo = pageNo;
    pageNo = parentData->pageNoArray[offset];
    insertAt = 0;
    endOfRecordsOffset = 0;
  } else {
    this->bufMgr->unPinPage(this->file, lastPage, false);
    endOfRecordsOffset = i;
    lastPageNo = pageNo;
  }
  #ifdef DEBUG
  assert(insertAt >= 0);
  assert(pageNo >= 0);
  #endif
  return;
}
}
