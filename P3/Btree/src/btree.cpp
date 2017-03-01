/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"


#include <algorithm>
#include <vector>
//#define DEBUG


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

    switch (this->attributeType) {
    case INTEGER:
      createRoot<int>(rootPage);
      break;
    case DOUBLE:
      createRoot<double>(rootPage);
      break;
    case STRING:
      createRoot<char*>(rootPage);
      break;
    default:
      break;
    }

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
          void* key = (void*)(record + this->attrByteOffset);
          this->insertEntry(key, scanRid);
#ifdef DEBUG
          this->bufMgr->flushFile(this->file);
#endif
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
  delete this->file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
  switch(this->attributeType) {
  case INTEGER:
    this->insertKeyTemplate<int>(key, rid);
    break;
  case DOUBLE:
    this->insertKeyTemplate<double>(key, rid);
    break;
  case STRING:
    this->insertKeyTemplate<char*>(key,rid);
    break;
  default:
    break;
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
  switch (lowOpParm) {
  case GT:
  case GTE:
    this->lowOp = lowOpParm;
    break;
  default:
    throw BadOpcodesException();
  }
  switch (highOpParm) {
  case LT:
  case LTE:
    this->highOp = highOpParm;
    break;
  default:
    throw BadOpcodesException();
  }

  switch (this->attributeType) {
    case INTEGER:
    {
      this->startScanTemplate<int>(lowValParm, highValParm);
      break;  
    }
    case DOUBLE:
    {
      this->startScanTemplate<double>(lowValParm, highValParm);
      break;  
    }
    case STRING:
    {
      this->startScanTemplate<char*>(lowValParm, highValParm);
      break;  
    }
    default:
      break;
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{
  if(this->scanExecuting == false) throw ScanNotInitializedException();
  switch (this->attributeType) {
    case INTEGER:
    {
      this->scanNextTemplate<int>(outRid);
      break;
    }
    case DOUBLE:
    {
      this->scanNextTemplate<double>(outRid);
      break;
    }
    case STRING:
    {
      this->scanNextTemplate<char*>(outRid);
      break;
    }
    default:
      break;
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{
  if (this->scanExecuting == false) throw ScanNotInitializedException();
  this->scanExecuting = false;
}

}
