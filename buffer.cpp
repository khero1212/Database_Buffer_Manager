/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

/**
 * Ben Reas, reas
 * Archana Dhyani, adhyani
 * Karthik Thatipalli, thatipalli
 *
 * This class represents a buffer pool manager which controls the buffer pool. This class can 
 * allocate pages in the buffer pool, read pages, flush files, and dispose of pages. The 
 * buffer manager uses a clock algorithm to select frames in the buffer pool to be allocated
 * for new pages.
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

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

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
    // Iterates through all the pages in the buffer pool and writes any
    // valid and dirty pages to their associated file
    for(FrameId i = 0; i < numBufs; ++i) {
        if(bufDescTable[i].valid && bufDescTable[i].dirty) 
            bufDescTable[i].file->writePage(bufPool[i]);
    }

    delete hashTable;
    delete[] bufDescTable;
    delete[] bufPool;
}

void BufMgr::advanceClock()
{
    clockHand = (clockHand + 1) % numBufs;
}

void BufMgr::allocBuf(FrameId & frame) 
{
    // Keeps track of the number of pinned pages encountered
    std::uint32_t pinCount = 0;

    while (pinCount != numBufs) {
        advanceClock();
        // checks if the page is valid
        if (!bufDescTable[clockHand].valid) {
            frame = clockHand;
            return;
        }
        // checks if the page has been referenced recently
        if (bufDescTable[clockHand].refbit) {
            bufDescTable[clockHand].refbit = false;
            continue;
        }
        // Checks if the page is pinned
        if (bufDescTable[clockHand].pinCnt > 0) {
            ++pinCount;
            continue;
        }
        // checks if the page is dirty
        if (bufDescTable[clockHand].dirty) 
            bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
        // If this code is reached then a page has been found 
        // which can be evicted from the buffer pool
        frame = clockHand;
        // Clears the evicted page from the hash table and clears the page in the 
        // buffer description table
        hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
        bufDescTable[clockHand].Clear(); 
        return;
    }
    // If this code is reached then all pages in the buffer pool are pinned
    throw BufferExceededException();
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
    FrameId frame;
    try {
        hashTable->lookup(file, pageNo, frame);
        // If this code is reached then the page is in the buffer pool
        bufDescTable[frame].refbit = true;
        bufDescTable[frame].pinCnt += 1;
        page = &(bufPool[frame]);
    } catch (const HashNotFoundException & e) {
        // The page being read is inserted into the buffer pool
        allocBuf(frame);
        bufPool[frame] = file->readPage(pageNo); 
        hashTable->insert(file, pageNo, frame);
        bufDescTable[frame].Set(file, pageNo);
        page = &(bufPool[frame]);
    }
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
    try {
        FrameId frame;
        hashTable->lookup(file, pageNo, frame);

        if (dirty)
            bufDescTable[frame].dirty = true;

        if (bufDescTable[frame].pinCnt <= 0)
            throw PageNotPinnedException(file->filename(), pageNo, frame);

        bufDescTable[frame].pinCnt -= 1;
    } catch (const HashNotFoundException & e) {
        // If an exception is thrown then the page is not in the buffer
        // pool and nothing is done
    }
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
    // Allocates an empty page in the specified file
    Page new_page = file->allocatePage();
    // The number of the new page to be returned
    pageNo = new_page.page_number();
    FrameId frame;
    // Obtains a buffer pool frame
    allocBuf(frame);
    hashTable->insert(file, pageNo, frame);
    bufDescTable[frame].Set(file, pageNo);
    bufPool[frame] = new_page;
    // returns a pointer to the buffer frame allocated
    page = &(bufPool[frame]);
}

void BufMgr::flushFile(const File* file) 
{
    // Iterates through each page in the buffer pool
    for (FrameId i = 0; i < numBufs; ++i) {
        // Checks if the file belonging to the current page is the same
        // file passed into the method
        if (bufDescTable[i].file == file) {
            if (bufDescTable[i].pinCnt > 0)
                throw PagePinnedException(bufDescTable[i].file->filename(), bufDescTable[i].pageNo, i);

            if (bufDescTable[i].pageNo == Page::INVALID_NUMBER)
                throw BadBufferException(i, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);

            if (bufDescTable[i].dirty) {
                bufDescTable[i].file->writePage(bufPool[i]);
                bufDescTable[i].dirty = false;
            }

            hashTable->remove(bufDescTable[i].file, bufDescTable[i].pageNo);
            bufDescTable[i].Clear();
        }
    }
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
    try {
        FrameId frame;
        // After this method is called either frame stores the frame
        // number holding the desired page or a HashNotFoundException
        // is thrown if the hashtable does not contain the page
        hashTable->lookup(file, PageNo, frame);
        hashTable->remove(file, PageNo);
        bufDescTable[frame].Clear();
    } catch (const HashNotFoundException & e) {
    }
    file->deletePage(PageNo);
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
