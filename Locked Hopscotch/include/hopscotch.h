#ifndef HOPSCOTCH_H
#define HOPSCOTCH_H
#include<iostream>
#include<pthread.h>
#define mmix(h,k) { k *= m; k ^= k >> r; k *= m; h *= m; h ^= k; }
# define rot(x, k) (((x) << (k)) | ((x) >> (32 - (k))))
#define MSB (((uint64_t)1) << 63)
using namespace std;
//10% -> 83886080
//20% -> 41943040
//40% -> 20971520
//60% -> 13981013

class Hopscotch {
  private:
	static const int HOP_RANGE = 32;//32;//1024;//32;
	static const int ADD_RANGE = 256;//1024*4;//256;
 	static const int MAX_SEGMENTS = 41943040;//118857;//1024*1024*16-1;//13981013;//1024*1024*2-1;//1048576;//13981013;//102*1024*8;//// // Including neighbourhodd for last hash location
	static const int MAX_TRIES = 2;
  int* BUSY;

  struct Bucket {

		unsigned int volatile _hop_info;
		int* volatile _key;
		int* volatile _data;
		unsigned int volatile _lock;
		unsigned int volatile _timestamp;
		pthread_mutex_t lock_mutex;
		pthread_cond_t lock_cv;

    Bucket(){
		  _hop_info = 0;
			_lock = 0;
			_key = NULL;
      _data=NULL;
			_timestamp = 0;
			pthread_mutex_init(&lock_mutex,NULL);
			pthread_cond_init(&lock_cv, NULL);
  	}

    void lock(){

      			pthread_mutex_lock(&lock_mutex);
      while(1){
		    if (_lock==0){
        				_lock =1;
				  pthread_mutex_unlock(&lock_mutex);
				  break;
			  }
                    pthread_cond_wait(&lock_cv, &lock_mutex);

 		  }
		}

		void unlock(){
			pthread_mutex_lock(&lock_mutex);
			_lock = 0;
		  pthread_cond_signal(&lock_cv);
	    pthread_mutex_unlock(&lock_mutex);
		}

	};

	Bucket* segments_arys;

  public:
  static uint64_t qt_hashword(uint64_t key)
    {   /*{{{*/
    uint32_t a, b, c;

    const union {
        uint64_t key;
        uint8_t  b[sizeof(uint64_t)];
    } k = {
        key
    };

    a  = b = c = 0x32533d0c + sizeof(uint64_t); // an arbitrary value, randomly selected
    c += 47;

    b += k.b[7] << 24;
    b += k.b[6] << 16;
    b += k.b[5] << 8;
    b += k.b[4];
    a += k.b[3] << 24;
    a += k.b[2] << 16;
    a += k.b[1] << 8;
    a += k.b[0];

    c ^= b;
    c -= rot(b, 14);
    a ^= c;
    a -= rot(c, 11);
    b ^= a;
    b -= rot(a, 25);
    c ^= b;
    c -= rot(b, 16);
    a ^= c;
    a -= rot(c, 4);
    b ^= a;
    b -= rot(a, 14);
    c ^= b;
    c -= rot(b, 24);
    return ((uint64_t)c + (((uint64_t)b) << 32)) & (~MSB);
    }
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

	Hopscotch();
   ~Hopscotch();
	bool contains(int* key);
	bool add(int *key, int *data);
	int* remove(int* key);
	unsigned int seed;
  void find_closer_bucket(Bucket**,int*,int &);
	void trial();

};

#endif // HOPSCOTCH_H
