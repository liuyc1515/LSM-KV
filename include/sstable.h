#ifndef SSTABLE_H
#define SSTABLE_H

#include <vector>
#include <fstream>
#if defined(_MSC_VER)
#include <io.h>
#include <direct.h>
#else
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#endif
#include "bloomfilter.h"
#include "skiplist.h"

template <class KEY, class VALUE>
class SSTable
{
private:
    struct Head
    {
        uint64_t timestamp_;
        uint64_t length_;
        KEY max_ele_key_;
        KEY min_ele_key_;
        Head()
        {
            timestamp_ = 0;
            length_ = 0;
            max_ele_key_ = 0;
            min_ele_key_ = UINT64_MAX;
        }
    };
    Head header_;
    BloomFilter<KEY> filter_;
    std::vector<std::pair<KEY, uint32_t>> index_;
    std::vector<VALUE> data_; 
    int makedir(std::string dir_name);
public:
    static int timestamp_;
    SSTable(const std::vector<std::pair<KEY, VALUE>> &data, int bloom_filter_size);
    ~SSTable();
    bool SSTableOut(std::string output_path);          // if success, return true
};

template <class KEY, class VALUE>
int SSTable<KEY, VALUE>::timestamp_ = 0;

#endif // SSTABLE_H
