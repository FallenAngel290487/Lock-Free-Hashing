#ifndef HOPSCOTCHHASH_H
#define HOPSCOTCHHASH_H
#include <atomic>
#include <climits>
#include <stdlib.h>
#include <time.h>
#include <iostream>
#include <mutex>
#include <bitset>
#include <sys/time.h>
#include "SpookyHash.h"
#include "fnv.h"
#include "emmintrin.h"

// 10 20 40 60

//Load factor values 

//10% -> 83886080
//20% -> 41943040
//40% -> 20971520
//60% -> 13981013
#define TotalBuckets   20971520 
#define mmix(h,k) { k *= m; k ^= k >> r; k *= m; h *= m; h ^= k; }

class HopscotchHash
{
    public:
        std::atomic<unsigned long> TimeCounter;
        HopscotchHash();
        virtual ~HopscotchHash();
        unsigned int MurmurHash2A (const void * key, int len, unsigned int tabsize)
        {
            const unsigned int m = 0x5bd1e995;
            const int r = 24;
            unsigned int l = len;

            const unsigned char * data = (const unsigned char *)key;

            unsigned int h = seed;

            while(len >= 4)
            {
                unsigned int k = *(unsigned int*)data;

                mmix(h,k);

                data += 4;
                len -= 4;
            }

            unsigned int t = 0;

            switch(len)
            {
            case 3: t ^= data[2] << 16; break;
            case 2: t ^= data[1] << 8; break;
            case 1: t ^= data[0]; break;
            };

            mmix(h,t);
            mmix(h,l);

            h ^= h >> 13;
            h *= m;
            h ^= h >> 15;

            return h % tabsize;
        }


        unsigned int CreateBucketEntry(unsigned int key, unsigned long *NewEntry, short int threadID)
        {
            unsigned int InitialSlot =  MurmurHash2A((const void*)&key, 4, TotalBuckets);
            unsigned long Entry = 0;
            short int header=0;
            short int STATE = 3;
            STATE<<=14;
            header|=STATE;
            header|=threadID;
            Entry|=key;
            Entry|=(0L|header)<<48;
            *NewEntry = Entry;
            return InitialSlot;

        }
        unsigned int getKey(unsigned long Entry)
        {
                 long Mask = 0xFFFFFFFFFFFFL;
                 return (unsigned int)(Entry & Mask);
        }

        unsigned short int getHeader(unsigned long Entry)
        {

                unsigned short int header = Entry >> 48;
                return header;
        }

        unsigned int getBucket(unsigned int index)
        {
            unsigned int StoredKey = getKey(Buckets[index].load());
            unsigned int ind = MurmurHash2A((const void*)&StoredKey, 4, TotalBuckets);
            return ind;
        }

        unsigned short int getThread(unsigned long Entry)
        {
            unsigned short int Header = getHeader(Entry);
            unsigned short int mask=0X3FFF;
            return Header&mask;
        }

        void clearThread(unsigned long *Entry)
        {
                long MASK = 0XC000FFFFFFFFFFFFL;
                *Entry &= MASK;
                MASK = 0X0080000000000000L;
                *Entry |= MASK;
        }

        unsigned int checkIntegrity(int num)
        {
            int counter=0;
            for(int i=0;i<TotalBuckets ; i++)
            if(Buckets[i].load()!=0)
            counter++;
            std::cout <<  " elements " << counter <<"\n";
            return counter;
        }
        void print_table()
        {
                for(int i=0;i<TotalBuckets ; i++)
                if(Buckets[i].load()!=0)
                {
                        std::cout<< this->getKey(Buckets[i].load()) << "\n";
                }
        }

        bool add(unsigned int key, int);
        bool Remove(unsigned int key);
        unsigned int Relocate(unsigned int RelocateEntry, long Entry);
        bool contains(unsigned int key);

        bool Eliminate(unsigned int key, int threadId, int, int );
        protected:
        private:
          std::atomic<unsigned long> *Buckets;
          int ADD_RANGE = 256;//512;
          unsigned int H = 32;//64;
          int KEYINFO = 28;
          unsigned int seed;
          public:
          std::atomic<unsigned long> substract;
          struct timeval start, stop;

};

#endif // HOPSCOTCHHASH_H
