 #include<stdio.h>
#include<stdlib.h>
#include<atomic>
#include<thread>
#include <climits>
#include <vector>
#include <time.h>
#include <sys/time.h>
#include<iostream>
#include <fstream>
long b = 281474976710656;
#define FIRST 1
#define SECOND 2
#define NIL -1
using namespace std;
int totaldata;
#define TOTDATA 1024*200
#define mmix(h,k) { k *= m; k ^= k >> r; k *= m; h *= m; h ^= k; }

unsigned int dataset[TOTDATA];
unsigned int randArray[TOTDATA];
int counter=0;
pthread_mutex_t count_mutex;
int contains, inserts, deletes;
unsigned int seed=rand() % LONG_MAX +1;
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

class entry
{
	public:
		unsigned int  key;
		unsigned int  value;
		entry(unsigned int  k, unsigned int  v)
		{
			key = k;
			value = v;
		}
};

unsigned int  get_cnt(void* ptr){
	unsigned long a;
	unsigned int  cnt;
	a= ((unsigned long)ptr & (0xffff000000000000));
	cnt= a>>48;
	return cnt;
}


void inc_counter(entry** ptr){
	*ptr = (entry *)((unsigned long)*ptr + b);
}

void store_count(entry** ptr,unsigned int  cnt){
	unsigned long new_cnt = cnt;
	new_cnt  = new_cnt<<48;
	*ptr = (entry *)((unsigned long)*ptr & 0x0000FFFFFFFFFFFF);
	*ptr = (entry *)((unsigned long)*ptr + new_cnt);
}

entry* extract_address(entry *e){
	e = (entry *)((unsigned long)e & (0x0000fffffffffffc));
	return e;
}


bool is_marked(void *ptr){
	if((unsigned long)ptr & 0x01)
		return true;
	else
		return false;
}

unsigned int  hashFunc1(unsigned int  key, unsigned int  size){

    unsigned int InitialSlot =  MurmurHash2A((const void*)&key, 4, size);
	//return key&(size-1);
	return InitialSlot;
}

unsigned int  hashFunc2(unsigned int  key, unsigned int  size){

    unsigned int InitialSlot =  MurmurHash2A((const void*)&key, 4, size);
	//return key&(size-1);
	return InitialSlot;
	//return key&(size-1);
}

bool checkCounter(unsigned int  ctr1,unsigned int  ctr2, unsigned int  ctrs1, unsigned int  ctrs2){
	if((ctrs1 - ctr1)>=2 || (ctrs2 - ctr2)>=2)
		return true;
	return false;
}

class cuckooHashTable
{
	public:
		atomic<entry *> *table1;
		atomic<entry *> *table2;
		unsigned int  t1Size;
		unsigned int  t2Size;

		cuckooHashTable(unsigned int  size1, unsigned int  size2)
		{
			t1Size = size1;
			t2Size = size2;
			table1 = new atomic<entry *>[size1];
			table2 = new atomic<entry *>[size2];
			//(std::atomic<unsigned long> *)malloc(sizeof(std::atomic<unsigned long>) * TotalBuckets + ADD_RANGE);
			//table1 = (atomic<entry *> *) malloc(sizeof(atomic<entry> ) * size1);
			//table2 = (atomic<entry *> *) malloc(sizeof(atomic<entry> ) * size2);
			init();
		}
		void init();
		unsigned int  Search(unsigned int  key);
		unsigned int  Find(unsigned int  key, entry **e1, entry **e2);
		void Insert(unsigned int  key, unsigned int  value);
		void Remove(unsigned int  key);
		bool Relocate(unsigned int  tableNum , unsigned int  pos);
		void help_relocate(unsigned int  table , unsigned int  idx, bool initiator);
		void print_table();
		void del_dup(unsigned int  idx1,entry *e1,unsigned int  idx2,entry *e2);
};

void cuckooHashTable::init(){
	entry *temp = NULL;
	unsigned int  i;
	for(i=0;i<t1Size;i++){
		atomic_store( &table1[i], temp);
	}

	for(i=0;i<t2Size;i++)
                atomic_store( &table2[i],temp);
}

void cuckooHashTable::help_relocate(unsigned int  which , unsigned int  idx , bool initiator){
	entry *src,*dst,*tmp;
	unsigned int  dst_idx,nCnt,cnt1,cnt2,size[2];
	atomic<entry *> *tbl[2];
	tbl[0] = table1;
	tbl[1] = table2;
	size[0] = t1Size;
	size[1] = t2Size;
	while(true){
		src =atomic_load_explicit(&tbl[which][idx],memory_order_seq_cst);
		//Marks the entry to logically swap it
		while(initiator && !(is_marked((void *)src)) ){
			tmp = extract_address(src);
			if(tmp == NULL)
				return;
			tmp = (entry *)((unsigned long)src|1);
			atomic_compare_exchange_strong(&(tbl[which][idx]), &src, tmp);
			src =atomic_load_explicit(&tbl[which][idx],memory_order_seq_cst);
		}//while

		cnt1 = get_cnt((void *)src);
		if(!(is_marked((void *)src))){
			return;
		}

		dst_idx = hashFunc1(extract_address(src)->key,size[1-which]);
		dst =atomic_load_explicit(&tbl[1-which][dst_idx],memory_order_seq_cst);
		// if dst is null , starts the swap with two CAS
		if(extract_address(dst) == NULL){
			cnt2 =get_cnt((void*)dst);
			nCnt = cnt1>cnt2?cnt1+1:cnt2+1;
			tmp =atomic_load_explicit(&tbl[which][idx],memory_order_seq_cst);

			if(tmp != src){
				continue;
			}

			entry *tmp_src = src;
			tmp_src = (entry *)((unsigned long)src & ~1);
			store_count(&tmp_src,nCnt);
			tmp = NULL;
			store_count(&tmp,cnt1+1);
			if(atomic_compare_exchange_strong(&(tbl[1-which][dst_idx]), &dst, tmp_src))
			atomic_compare_exchange_strong(&(tbl[which][idx]), &src, tmp);
			return;
		}//if dst==NULL
		//helper part of the code which helps to finish the second part of the relocate.
		//Might be called by helper thread or in some case by initiator thread also
		if(src == dst){
			tmp = NULL;
			store_count(&tmp,cnt1+1);
			atomic_compare_exchange_strong(&(tbl[which][idx]), &src, tmp);
			return;
		}//if src == dst

	tmp = NULL;
	tmp = (entry *)((unsigned long)src&(~1));
	store_count(&tmp,cnt1+1);
	atomic_compare_exchange_strong(&(tbl[which][idx]), &src, tmp);
	return;

}
}

void cuckooHashTable::del_dup(unsigned int  idx1,entry *e1,unsigned int  idx2,entry *e2){
	entry *tmp1,*tmp2;
	unsigned int  key1,key2,cnt;
		tmp1 = atomic_load(&table1[idx1]);
		tmp2 = atomic_load(&table2[idx2]);
		if((e1 != tmp1)&&(e2 != tmp2))
			return;
		key1 = extract_address(e1)->key;
		key2 = extract_address(e2)->key;
		if(key1 != key2)
			return;
	tmp2 = NULL;
	cnt = get_cnt(e2);
	store_count(&tmp2,cnt+1);
	atomic_compare_exchange_strong(&(table2[idx2]), &e2, tmp2);
}

void cuckooHashTable::print_table()
{
    int counter=0;
	printf("******************hash_table 1*****************\n");
	unsigned int  i;
	entry *e,*tmp = NULL;
	for(i=0;i<t1Size;i++){
		if(table1[i] != NULL){
			e = atomic_load_explicit(&table1[i],memory_order_relaxed);
			tmp = extract_address(e);
			if(tmp != NULL)
			{
			//printf("%d\t%016lx\t%d\t%d\n",i,(long)e ,tmp->key,tmp->value);
			counter++;
			}
			//else
			//printf("%d\t%016lx\n",i,(long)e);
				}
		//else
			//printf("%d\tNULL\n",i);
	}
	printf("****************hash_table 2*******************\n");
	for(i=0;i<t2Size;i++){
		if(table2[i] != NULL){
			e = atomic_load_explicit(&table2[i],memory_order_relaxed);
			tmp = extract_address(e);
			if(tmp != NULL)
			{
			counter++;
			//printf("%d\t%016lx\t%d\t%d\n",i,(long)e ,tmp->key,tmp->value);
			}
			//else
			//printf("%d\t%016lx\n",i,(long)e);
				}
		//else
			//printf("%d\tNULL\n",i);
	}
	printf("\n \n total data %d\n ", counter);


}

unsigned int  cuckooHashTable::Search(unsigned int  key)
{
	unsigned int  h1,h2;
	unsigned int  c1,c2,c1s,c2s;
	while(1){
		h1 = hashFunc1(key,t1Size);
		h2 = hashFunc2(key,t2Size);

		entry *e = atomic_load_explicit(&table1[h1],memory_order_relaxed);
		c1 = get_cnt(e);
		e= extract_address(e);
		//Looking in table 1
		if(e!= NULL && e->key == key)
			return e->value;
		e = atomic_load_explicit(&table2[h2],memory_order_relaxed);
		c2 = get_cnt(e);
		e= extract_address(e);
		//Looking in table 2
		if(e!= NULL && e->key == key)
			return e->value;

		//second round
		e = atomic_load_explicit(&table1[h1],memory_order_relaxed);
		c1s = get_cnt(e);
		e= extract_address(e);

		if(e!= NULL && e->key == key)
			return e->value;
		e = atomic_load_explicit(&table2[h2],memory_order_relaxed);
		c2s = get_cnt(e);
		e= extract_address(e);
		if(e!= NULL && e->key == key)
			return e->value;

		if(checkCounter(c1,c2,c1s,c2s))
			continue;
		return NIL;
	}
}

unsigned int  cuckooHashTable::Find(unsigned int  key, entry **e1, entry **e2)
{
	unsigned int  h1,h2,result=0;
	unsigned int  c1,c2,c1s,c2s;
	while(1){
		h1 = hashFunc1(key,t1Size);
		h2 = hashFunc2(key,t2Size);
		/*********************Round 1************************************/
		entry *e = atomic_load_explicit(&table1[h1],memory_order_relaxed);
		*e1=e;
		c1 = get_cnt(e);
		e= extract_address(e);
		// helping other concurrent thread if its not completely done with its job
		if(e != NULL){
			if(is_marked((void*)e))	{
			help_relocate(0,h1,false);
			continue;
			}
			else if(e->key == key)
				result = FIRST;
		}

		e = atomic_load_explicit(&table2[h2],memory_order_relaxed);
		*e2=e;
		e= extract_address(e);
		c2 = get_cnt(e);
		e= extract_address(e);
		// helping other concurrent thread if its not completely done with its job
		if(e != NULL){
			if(is_marked((void*)e))	{
				help_relocate(1,h2,false);
				continue;
			}
			if(e->key == key ){
				if( result == FIRST){
					printf("Find(): Delete_dup()\n");
					del_dup(h1,*e1,h2,*e2);
				}
				else
			   		result = SECOND;
			}
		}

		if(result == FIRST || result == SECOND)
			return result;
		/*********************Round 2************************************/
		e = atomic_load_explicit(&table1[h1],memory_order_relaxed);
		*e1=e;
		c1s = get_cnt(e);
		e= extract_address(e);
		if(e != NULL){
			if(is_marked((void*)e))	{
				help_relocate(0,h1,false);
				printf("Find(): help_relocate()");
				continue;
			}
			else if(e->key == key)
				result = FIRST;
		}

		e = atomic_load_explicit(&table2[h2],memory_order_relaxed);
		*e2=e;
		e= extract_address(e);
		c2s = get_cnt(e);
		e= extract_address(e);
		if(e != NULL){
			if(is_marked((void*)e))	{
					help_relocate(1,h2,false);
					continue;
			}
			if(e->key == key ){
				if( result == FIRST){
					printf("Find(): Delete_dup()\n");
					del_dup(h1,*e1,h2,*e2);
				}
				else
			    	result = SECOND;
			}
		}

		if(result == FIRST || result == SECOND)
			return result;
		if(checkCounter(c1,c2,c1s,c2s)){
			continue;
		}
		return NIL;
	}//end while(1)
}


void cuckooHashTable::Insert(unsigned int  key , unsigned int  value)
{
	entry *newEntry = new entry(key,value);
	entry *ent1 = NULL, *ent2 = NULL;
	unsigned int  cnt=0,h1,h2;
	h1 = hashFunc1(key,t1Size);
	h2 = hashFunc2(key,t2Size);

	while(true)
	{
		unsigned int  result = Find(key, &ent1, &ent2);
		//updating existing content
		if(result == 1)
		{
			cnt = get_cnt(ent1);
			store_count(&newEntry,cnt+1);
			bool casResult = atomic_compare_exchange_strong(&(table1[h1]), &ent1, newEntry);
			delete[] extract_address(ent1);
			if(casResult == true)
				return;
			else
				continue;
		}

		if(result == 2)
		{
			cnt = get_cnt(ent2);
			store_count(&newEntry,cnt+1);
			bool casResult = atomic_compare_exchange_strong(&(table2[h2]), &ent2, newEntry);
			delete[] extract_address(ent2);
			if(casResult == true)
				return;
			else
				continue;
		}
		//avoiding double duplicate instance of key
		if(extract_address(ent1) == NULL && extract_address(ent2) == NULL)
				{
					cnt = get_cnt(ent2);
					store_count(&newEntry,cnt+1);
					bool casResult = atomic_compare_exchange_strong(&(table2[h2]), &ent2, newEntry);
					if(casResult == true)
						return;
					else{
						continue;
					}
				}
		if(extract_address(ent1) == NULL)
		{
			cnt = get_cnt(ent1);
			store_count(&newEntry,cnt+1);
			bool casResult = atomic_compare_exchange_strong(&(table1[h1]), &ent1, newEntry);
			delete[] extract_address(ent1);
			if(casResult == true)
				return;
			else
				continue;
		}

		if(extract_address(ent2) == NULL)
		{
			cnt = get_cnt(ent2);
			store_count(&newEntry,cnt+1);
			bool casResult = atomic_compare_exchange_strong(&(table2[h2]), &ent2, newEntry);
			delete[] extract_address(ent2);
			if(casResult == true)
				return;
			else
				continue;
		}

		bool relocateResult = cuckooHashTable::Relocate(1,hashFunc1(key,t1Size));
		if(relocateResult == true){
			continue;
		}
		else{
			//rehash
			return;
		}
	}
}



void cuckooHashTable::Remove(unsigned int  key)
{
    entry *ent1 = NULL, *ent2 = NULL;
  	unsigned int  cnt=0,h1,h2;
  	h1 = hashFunc1(key,t1Size);
  	h2 = hashFunc2(key,t2Size);
          while(true){
                  unsigned int  result = Find(key, &ent1, &ent2);
                  if(result == FIRST){
                	  	  entry *tmp = NULL;
				   	  	  cnt = get_cnt(ent1);
                	  	  store_count(&tmp,cnt+1);
                	  	  bool casResult = atomic_compare_exchange_strong(&(table1[h1]), &ent1, tmp);
                	  	  if(casResult == true){
                	  		  return;
                	  	  }
                	  	  else
                	  		  continue;
                  	  }
					else if(result == SECOND){
							if(table1[h1] != ent1){
								continue;
							}
							entry *tmp = NULL;
							cnt = get_cnt(ent2);
							store_count(&tmp,cnt+1);
							bool casResult = atomic_compare_exchange_strong(&(table2[h2]), &ent2, tmp);
							if(casResult == true){
								return;
							}
							else
								continue;
					}

					else
						return;
          }
}


bool cuckooHashTable::Relocate(unsigned int  tableNum, unsigned int  pos)
{
	unsigned int  threshold = t1Size+t2Size;
	unsigned int  route[threshold];
	unsigned int  start_level = 0, tbl_num = tableNum, idx = pos, pre_idx=0,key;
	atomic<entry *> *tbl[2];
		tbl[0] = table1;
		tbl[1] = table2;
	path_discovery:
	bool found = false;
	unsigned int  depth = start_level;

	do{
		entry *e1 = NULL;
		entry *pre = NULL;
		e1 = atomic_load(&tbl[tbl_num][idx]);
		while( is_marked((void *)e1) ){
			help_relocate(tbl_num,idx,false);
			e1 = atomic_load(&tbl[tbl_num][idx]);
		}
		if (depth >0){
			entry *pre_addr,*e1_addr;//assign to masked value
			pre_addr = extract_address(pre);
			e1_addr = extract_address(e1);
			if(e1_addr != NULL && pre_addr != NULL){
				if(pre == e1 || pre_addr->key == e1_addr->key){
					if(tbl_num == 0)
						del_dup(idx,e1,pre_idx,pre);
					else
						del_dup(pre_idx,pre,idx,e1);
				}
			}
		}

		if(extract_address(e1) != NULL){
			route[depth] = idx;
			key = extract_address(e1)->key;
			pre = e1;
			pre_idx = idx;
			tbl_num = 1-tbl_num;
			idx =  (tbl_num == 0 ? hashFunc2(key,t1Size) : hashFunc1(key,t2Size));
		}

		else{
			found = true;
		}
	}while(!found && ++depth<threshold);

	if(found){
		entry *e,*dst;
		unsigned int  dst_idx,key;
		tbl_num = 1-tbl_num;
		for(unsigned int  i=depth-1; i>=0; --i, tbl_num = 1-tbl_num){
			idx = route[i];
			e = atomic_load(&tbl[tbl_num][idx]);
			if(is_marked((void *)e)){
					help_relocate(tbl_num,idx,false);
					e = atomic_load(&tbl[tbl_num][idx]);
			}
			if(extract_address(e) == NULL){
					continue;
			}
			key = extract_address(e)->key;
			if(tbl_num == 0)
				dst_idx = hashFunc2(key,t2Size);
			else
				dst_idx = hashFunc1(key,t1Size);
			dst = atomic_load(&tbl[1-tbl_num][dst_idx]);
			if(extract_address(dst) != NULL){
				start_level=i+1;
				idx = dst_idx;
				tbl_num = 1-tbl_num;
				goto path_discovery;
			}
			help_relocate(tbl_num,idx,true);
		}
	}
	return found;
}
cuckooHashTable *H;

void* insertKeys(void *x)
{
    int *data =  (int*)(x);
    int tid = *data;

        unsigned int key = 0;
        for(int i=0;i<DATAPERTHREAD;i++)
        {

             key=tid*DATAPERTHREAD + i  +1;
             H->Insert(key, i++);
             pthread_mutex_lock(&count_mutex);
             printf(" added key %d \r", counter);
             dataset[counter++]=key;
             pthread_mutex_unlock(&count_mutex);

             //dataset[tid*DATAPERTHREAD + i]=key;

        }
}
void* checkKeys(void *x)
{
    int *data =  (int*)(x);
    int thid = *data;
    for(int i=0;i<DATAPERTHREAD;i++)
    {

            if(!H->Search(dataset[rand()%TOTDATA])){}
                //printf(" FAILURE %ld \n ", dataset[thid*DATAPERTHREAD+i]);


    }

}

void * worker(void* x)
{
    int *data =  (int*)(x);
    int thid = *data;
    int key=0;
    for(int i=0;i<contains;i++)
    {
        int index = rand() % TOTDATA;
        unsigned int key = dataset[index];
        H->Search(key);

    }

    for(int x=0;x<100;x++)
    {
        key = rand() % INT_MAX +1;
        H->Insert(key, x++);
    }

    for(int x=0;x<deletes;x++)
    {
        int index = rand()%TOTDATA;
        unsigned int key = dataset[index];
        H->Remove(key);
        //printf(" Error in removing key %ld \n ", key);
    }

}
int main (int argc, char* argv[])
{

    THREADS=atoi(argv[1]);
    DATAPERTHREAD=TOTDATA/THREADS;
    intptr_t i = 1;
    struct timeval start,end;


    for(int i=0;i<TOTDATA ;i++)
    {
        randArray[i]=rand()% TOTDATA + 1;
    }

    H = new cuckooHashTable(1024*1024*8-1,1024*1024*8-1);
    pthread_t threads[THREADS], ContainsThread[THREADS], insertThreads[THREADS];

    for(int i=0;i<THREADS;i++)
    {
        pthread_create(&insertThreads[i], NULL, insertKeys,  &i);
    }
    gettimeofday(&start, NULL);
    for(int i=0;i<THREADS;i++)
    {
        pthread_join(insertThreads[i],NULL);
    }

    gettimeofday(&end, NULL);
    long seconds = end.tv_sec - start.tv_sec;
    long nsec = end.tv_usec - start.tv_usec;
    printf("Time for full insertion %f \n ", (float)TOTDATA/(((seconds*1000000)+(nsec))));


    for(int i=0;i<THREADS;i++)
    {
        pthread_create(&ContainsThread[i], NULL, checkKeys,  &i);
    }
    gettimeofday(&start, NULL);
    for(int i=0;i<THREADS;i++)
    {
        pthread_join(ContainsThread[i],NULL);
    }

    gettimeofday(&end, NULL);
    seconds = end.tv_sec - start.tv_sec;
    nsec = end.tv_usec - start.tv_usec;
    printf("Time for full retreival %f \n ", (float)TOTDATA/(((seconds*1000000)+(nsec))));

    cout<<"\n [90-5-5] (1) [80-10-10] (2) [60-30-30] (3) [30-30-30] (4) \n";
   int choice;
   cin>>choice;
   switch(choice)
   {
        case 1:
        {
            contains = (int)(TOTDATA*0.9)/THREADS;
            inserts =  (int)(TOTDATA*0.05)/THREADS;
            deletes =  (int)(TOTDATA*0.05)/THREADS;
            break;
        }
        case 2:
        {
            contains = (int)(TOTDATA*0.8)/THREADS;
            inserts =  (int)(TOTDATA*0.1)/THREADS;
            deletes =  (int)(TOTDATA*0.1)/THREADS;
            break;
        }
        case 3:
        {
            contains = (int)(TOTDATA*0.6)/THREADS;
            inserts =  (int)(TOTDATA*0.2)/THREADS;
            deletes =  (int)(TOTDATA*0.2)/THREADS;
            break;
        }
        case 4:
        {
            contains = (int)(TOTDATA*0.4)/THREADS;
            inserts =  (int)(TOTDATA*0.3)/THREADS;
            deletes =  (int)(TOTDATA*0.3)/THREADS;
            break;
        }



   }


    for (int r=0;r<THREADS;r++){
    pthread_create(&threads[r], NULL, worker,  &r);
    }
    gettimeofday(&start,NULL);
    for(int r=0;r<THREADS;r++){
    pthread_join(threads[r],NULL);
    }
    gettimeofday(&end,NULL);
    seconds = end.tv_sec - start.tv_sec;
    nsec = end.tv_usec - start.tv_usec;
        if(choice==1)
            cout<<"Time for 90 % contains 5% add and 5 % delete "<<(float)TOTDATA/(((seconds*1000000)+(nsec)))<<endl;
        else if(choice==2)
            cout<<"Time for 80 % contains 10% add and 10 % delete "<<(float)TOTDATA/(((seconds*1000000)+(nsec)))<<endl;
        else if(choice==3)
            cout<<"Time for 60 % contains 20% add and 20% delete "<<(float)TOTDATA/(((seconds*1000000)+(nsec)))<<endl;
        else
            cout<<"Time for 40 % contains 30% add and 30% delete "<<(float)TOTDATA/(((seconds*1000000)+(nsec)))<<endl;
        return 0;
}
