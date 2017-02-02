/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
}

void BufMgr::advanceClock()
{
  clockHand = (clockHand + 1) % numBufs;
}

void BufMgr::allocBuf(FrameId & frame)
{
  unsigned int cnt = 0;
  while(1) {
    advanceClock();
    if (cnt == numBufs) throw BufferExceededException();
    if (bufDescTable[clockHand].pinCnt == 0 && bufDescTable[clockHand].refbit == false) {
      frame = clockHand;
      break;
    } else if (bufDescTable[clockHand].pinCnt == 1) cnt++;
    bufDescTable[clockHand].refbit = false;
  }
  if (bufDescTable[frame].valid == true) {
    hashTable->remove(bufDescTable[frame].file, bufDescTable[frame].pageNo);
    if (bufDescTable[frame].dirty == true) bufDescTable[frame].file->writePage(bufPool[frame]);
    bufDescTable[frame].Clear();
  }
  return;
}

void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
  FrameId frameNo;
  bool rc = hashTable->lookup(file, pageNo, frameNo);
  if (rc == false) {
    allocBuf(frameNo);
    Page pageRc = file->readPage(pageNo);
    bufPool[frameNo] = pageRc;
    page = &bufPool[frameNo];
    hashTable->insert(file, pageNo, frameNo);
    bufDescTable[frameNo].Set(file, pageNo);
  } else {
    page = &bufPool[frameNo];
    bufDescTable[frameNo].refbit = 1;
    bufDescTable[frameNo].pinCnt++;
  }
  return;
}

void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
  FrameId frameNo;
  bool rc = hashTable->lookup(file, pageNo, frameNo);
  if (rc == false) return;
  if (bufDescTable[frameNo].pinCnt == 0) throw PageNotPinnedException(file->filename(), pageNo, frameNo);
  bufDescTable[frameNo].pinCnt--;
  if (dirty == true) bufDescTable[frameNo].dirty = true;
}

void BufMgr::flushFile(const File* file) 
{
  for (unsigned int i = 0; i < numBufs; i++) {
    if (bufDescTable[i].file == file) {
      if (bufDescTable[i].pageNo == Page::INVALID_NUMBER)
        throw BadBufferException(i, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
      if (bufDescTable[i].pinCnt > 0) throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, i);
      if (bufDescTable[i].dirty == true) {
        bufDescTable[i].file->writePage(bufPool[i]);
        bufDescTable[i].dirty = false;
      }
      hashTable->remove(bufDescTable[i].file, bufDescTable[i].pageNo);
      bufDescTable[i].Clear();
    }
  }
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
  Page pageRc = file->allocatePage();
  FrameId frameNo;
  allocBuf(frameNo);
  bufPool[frameNo] = pageRc;
  page = &bufPool[frameNo];
  pageNo = page->page_number();
  hashTable->insert(file, pageNo, frameNo);
  bufDescTable[frameNo].Set(file, pageNo);
  return;
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
    
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	    tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}