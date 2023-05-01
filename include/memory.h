#ifndef MEMORY_H
#define MEMORY_H

#include <vector>
#include <map>
#include <iostream>
#include <algorithm>
#include <tuple>
#include <list>
#include <sstream>
#if defined(_MSC_VER)
#include <io.h>
#include <direct.h>
#else
#include <stdio.h>
#include <unistd.h>
#endif
#include <cerrno>
#include <iterator>
#include "skiplist.h"
#include "bloomfilter.h"
#include "sstable.h"
#include "utils.h"

template <class KEY, class VALUE>
class Memory
{
private: 
    struct SmallSSTable
    {
        struct Head
        {
            uint64_t timestamp_;
            uint64_t length_;
            KEY max_ele_key_;
            KEY min_ele_key_;
            Head();
            Head(uint64_t timestamp, uint64_t length, KEY max_ele_key, KEY min_ele_key);
            bool operator == (const SmallSSTable::Head &head) const;
        };
        Head header_;
        BloomFilter<KEY> filter_;
        std::vector<std::pair<KEY, uint32_t>> index_;
        SmallSSTable(const std::vector<std::pair<KEY, VALUE>> &data, int bloom_filter_size);
    };
    typedef std::tuple<int, uint64_t, uint64_t, KEY, KEY> file_index_t;
    typedef std::pair<file_index_t, uint32_t> item_index_t;

    SkipList<KEY, VALUE>* list_;
    std::list<std::pair<int, SmallSSTable>> buffer_;
    int current_size_;
    int element_num_;

    std::string output_path_;

    const int MAX_SIZE_;
    const int BLOOM_FILTER_SIZE_;

    bool NeedCompaction(int level) const;
    std::vector<std::string> Split(const std::string &str, char delim) const;
    void GetCompactionFiles(int level, std::vector<SmallSSTable*> &files_to_compaction);
    uint64_t GetCompactionFilesRange(int level, uint64_t min, uint64_t max, std::vector<SmallSSTable*> &files_to_compaction);
    void WriteToDisk(int level, std::vector<std::pair<KEY, VALUE>> &data);
    std::tuple<uint64_t, uint64_t, KEY, KEY> ReadHead(std::string filename) const;
    void ReadFile(const file_index_t &file_index, std::vector<VALUE> &values, bool is_delete) const;
    void Compaction(std::vector<SmallSSTable*> &files_to_compaction, int next_level);
    void MergeSort(typename std::vector<std::pair<KEY, item_index_t>>::const_iterator merge1_it,
                   const typename std::vector<std::pair<KEY, item_index_t>>::const_iterator merge1_end,
                   typename std::vector<std::pair<KEY, item_index_t>>::const_iterator merge2_it,
                   const typename std::vector<std::pair<KEY, item_index_t>>::const_iterator merge2_end,
                   std::vector<std::pair<KEY, item_index_t>> &target) const;
    void PackSmallSSTable(SmallSSTable* table, int level, std::vector<std::pair<KEY, item_index_t>> &table_package) const;
    void DeleteSmallSSTable(int level, SmallSSTable* table);
    bool FindKey(KEY key, const SmallSSTable* table, uint32_t &offset) const;
    VALUE FindValue(int level, const SmallSSTable* table, uint32_t offset) const;
    void PackSmallSSTableRange(std::vector<std::pair<int, const SmallSSTable*>> &tables, uint64_t min,
                               uint64_t max, std::vector<std::vector<std::pair<KEY, item_index_t>>> &tables_package) const;
    void ScanBuffer(uint64_t min, uint64_t max,
                    std::vector<std::pair<int, const SmallSSTable*>> &files_to_scan) const;
    void Merge(std::vector<std::vector<std::pair<KEY, item_index_t>>> &tables_package,
               std::vector<std::pair<KEY, item_index_t>> &merge_result) const;
    void ReorganizeScanResult(std::vector<std::pair<KEY, item_index_t>> &files_data,
                              std::list<std::pair<KEY, VALUE>> &list) const;
    bool Exist(const KEY &key) const;
public:
    Memory(std::string output_path, int max_size = 2 * 1024 * 1024, int bloom_filter_size = 10240);
    ~Memory();
    void Put(KEY key, VALUE value);
    VALUE Get(const KEY &key) const;
    bool Del(const KEY &key);
    void Reset();
    void Scan(const KEY &key1, const KEY &key2, std::list<std::pair<KEY, VALUE>> &list) const;
};

#endif // MEMORY_H
