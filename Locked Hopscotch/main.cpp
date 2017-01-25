 #include <iostream>       // std::cout
#include <atomic>         // std::atomic, std::memory_order_relaxed
#include <thread>
#include <algorithm>
#include <climits>
#include <vector>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <stdio.h>
#include <fstream>
#include <map>
#include "hopscotch.h"
#define TOTDATA 1024*1024*8
struct timeval start, stop;
using namespace std;
std::atomic<int> datacounter (0);
unsigned int *dataset;
unsigned int *randArray;
int contains, inserts, deletes;

void insertKey(Hopscotch *h, int tid)
{
unsigned int key = 0;
        for(int i=0;i<DATAPERTHREAD;i++)
        {

             key=tid*DATAPERTHREAD + i  +1;
             if(h->add(new int(key), new int(tid)))
             dataset[datacounter.fetch_add(1)]=key;

        }
}

void checkKeys(Hopscotch *h, int index)
{

    for(int i=0;i<DATAPERTHREAD;i++)
    {

            if(!h->contains(new int (dataset[index*DATAPERTHREAD+i]))){}
           //std::cout <<" CHECK -> FAILURE " << dataset[index*DATAPERTHREAD+i]<<"\n ";


    }

}

void worker(Hopscotch *h, int tid)
{
    int key=0;
    for(int i=0;i<contains;i++)
    {
        //int index = rand() % datacounter.load();
        //unsigned int key = dataset[index];
        h->contains(new int(dataset[randArray[tid * contains + i]]));


    }

    for(int x=0;x<inserts;x++)
    {
        key = randArray[tid * inserts + x];

        h->add(new int(key),new int(tid));
    }



    for(int x=0;x<deletes;x++)
    {
        key = randArray[tid * deletes + x];
        h->remove(new int(key));
        /*if(!h->remove(new int(key)))
        {
            cout<<" ERROR: Removal Failure for key " << key << " by thread "<< tid << "\n" ;
            exit(0);
        }*/
    }

}

int main (int argc, char* argv[])
{

  THREADS=atoi(argv[1]);
  DATAPERTHREAD=TOTDATA/THREADS;

  dataset = new unsigned int [TOTDATA];

   randArray = new unsigned int[TOTDATA];

  for(int i=0;i<TOTDATA ;i++)
  {
        dataset[i]=0;
        randArray[i]=rand()% TOTDATA + 1;
  }
  Hopscotch h;

  std::vector<std::thread> Threads;
  std::vector<std::thread> ContainThreads;


  for(int i=0;i<THREADS;i++)
  {
        Threads.push_back(std::thread(insertKey, &h, i));
  }
  gettimeofday(&start, NULL);
  for(int i=0;i<THREADS;i++)
  {
        Threads[i].join();
  }

  gettimeofday(&stop, NULL);
  cout<<"build time "<< (float)TOTDATA/((stop.tv_sec * 1000000 + stop.tv_usec)
	            				 - (start.tv_sec * 1000000 + start.tv_usec)) <<" MQPS \n";

  for(int i=0;i<THREADS;i++)
  {
        ContainThreads.push_back(std::thread(checkKeys, &h, i));
  }
  gettimeofday(&start, NULL);
  for(int i=0;i<THREADS;i++)
  {
        ContainThreads[i].join();
  }

  gettimeofday(&stop, NULL);
  long seconds = stop.tv_sec - start.tv_sec;
  long nsec = stop.tv_usec - start.tv_usec;
  cout<<"Time for full retreival "<<(float)TOTDATA/(((seconds*1000000)+(nsec)))<<endl;

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

            std::vector<std::thread> threads;
            for(int i=0;i<THREADS;i++)
            {
                threads.push_back(std::thread(worker, &h, i));
            }

            gettimeofday(&start, NULL);
            for(int i=0;i<THREADS;i++)
            threads[i].join();
            gettimeofday(&stop, NULL);
            seconds = stop.tv_sec - start.tv_sec;
            nsec = stop.tv_usec - start.tv_usec;
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
