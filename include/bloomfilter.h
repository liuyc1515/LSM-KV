#ifndef BLOOMFILTER_H
#define BLOOMFILTER_H

#include <functional>
#include <string>
#include <ctime>
#include <cstring>
#include "MurmurHash3.h"

template <class T>
class BloomFilter
{
private:
    int m_;         // size of bloomfilter
    const static int MAX_SIZE_ = 1 << 16;
    bool table_[MAX_SIZE_];
public:
    BloomFilter(int m);
    ~BloomFilter();
    void Insert(const T &);
    bool Exist(const T &) const;

    template <class KEY, class VALUE>
    friend class SSTable;
};

#endif // BLOOMFILTER_H
