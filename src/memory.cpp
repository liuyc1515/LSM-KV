#include "memory.h"

template <class KEY, class VALUE>
Memory<KEY, VALUE>::SmallSSTable::Head::Head()
{
    timestamp_ = 0;
    length_ = 0;
    max_ele_key_ = 0;
    min_ele_key_ = UINT64_MAX;
}

template <class KEY, class VALUE>
Memory<KEY, VALUE>::SmallSSTable::Head::Head(uint64_t timestamp, uint64_t length, KEY max_ele_key, KEY min_ele_key):
    timestamp_(timestamp), length_(length), max_ele_key_(max_ele_key), min_ele_key_(min_ele_key){};

template <class KEY, class VALUE>
bool Memory<KEY, VALUE>::SmallSSTable::Head::operator == (const SmallSSTable::Head &head) const
{
    if (timestamp_ == head.timestamp_ &&
            length_ == head.length_ &&
            max_ele_key_ == head.max_ele_key_ &&
            min_ele_key_ == head.min_ele_key_)
    {
        return true;
    }
    return false;
}

template <class KEY, class VALUE>
Memory<KEY, VALUE>::SmallSSTable::SmallSSTable(const std::vector<std::pair<KEY, VALUE>> &data,
                                               int bloom_filter_size):
    header_(), filter_(bloom_filter_size)
{
    index_.clear();
    uint32_t pos = 0;
    for (typename std::vector<std::pair<KEY, VALUE>>::const_iterator it = data.begin(); it != data.end(); ++it)
    {
        ++(header_.length_);
        VALUE value = it->second;
        pos += sizeof(char) * value.length();
        header_.max_ele_key_ = (it->first > header_.max_ele_key_)? it->first : header_.max_ele_key_;
        header_.min_ele_key_ = (it->first < header_.min_ele_key_)? it->first : header_.min_ele_key_;
        filter_.Insert(it->first);
        index_.push_back(std::pair<KEY, uint32_t>(it->first, pos - sizeof(char) * value.length()));
    }
}

template <class KEY, class VALUE>
Memory<KEY, VALUE>::Memory(std::string output_path, int max_size, int bloom_filter_size):
    MAX_SIZE_(max_size), BLOOM_FILTER_SIZE_(bloom_filter_size)     // ln(2) = 0.69314718055994530941723212145818
{
    list_ = new SkipList<KEY, VALUE>();
    buffer_.clear();
    current_size_ = 0;
    element_num_ = 0;
    output_path_ = output_path;
    if (output_path_[output_path_.length() - 1] != '/')
    {
        output_path_.append("/");
    }
}

template <class KEY, class VALUE>
Memory<KEY, VALUE>::~Memory()
{
    delete list_;
    buffer_.clear();
}

template <class KEY, class VALUE>
bool Memory<KEY, VALUE>::NeedCompaction(int level) const
{
    int file_num = 0;
    for (typename std::list<std::pair<int, SmallSSTable>>::const_iterator buffer_it = buffer_.begin();
         buffer_it != buffer_.end();
         ++buffer_it)
    {
        if (buffer_it->first == level)
        {
            ++file_num;
        }
    }
    int max_file_num = 1 << (level + 1);
    return (file_num > max_file_num)? true : false;
}

template <class KEY, class VALUE>
void Memory<KEY, VALUE>::GetCompactionFiles(int level, std::vector<SmallSSTable*> &files_to_compaction)
{
    typedef std::pair<std::pair<uint64_t, KEY>, SmallSSTable*> file_info_t;
    std::vector<file_info_t> timestamp_to_file;
    for (typename std::list<std::pair<int, SmallSSTable>>::iterator buffer_it = buffer_.begin(); buffer_it != buffer_.end(); ++buffer_it)
    {
        if (buffer_it->first == level)
        {
            uint64_t timestamp = buffer_it->second.header_.timestamp_;
            KEY max_ele_key = buffer_it->second.header_.max_ele_key_;
            timestamp_to_file.push_back(file_info_t({{timestamp, max_ele_key}, &(buffer_it->second)}));
        }
    }
    int max_file_num = 1 << (level + 1);
    int file_num = timestamp_to_file.size();
    typename std::vector<file_info_t>::const_iterator file_it = timestamp_to_file.begin();
    if (level == 0)
    {
        while (file_it != timestamp_to_file.end())
        {
            files_to_compaction.push_back(file_it->second);
            ++file_it;
        }
    }
    else
    {
        auto cmp_file_less = [] (const file_info_t &file1, const file_info_t &file2)
        {
            if (file1.first.first < file2.first.first)
            {
                return true;
            }
            else if (file1.first.first == file2.first.first)
            {
                if (file1.first.second < file2.first.second)
                {
                    return true;
                }
            }
            return false;
        };
        std::sort(timestamp_to_file.begin(), timestamp_to_file.end(), cmp_file_less);
        for (int i = 0; i < file_num - max_file_num; ++i)
        {
            if (file_it >= timestamp_to_file.end())
            {
                break;
            }
            files_to_compaction.push_back(file_it->second);
            ++file_it;
        }
    }
}

template <class KEY, class VALUE>
uint64_t Memory<KEY, VALUE>::GetCompactionFilesRange(int level, uint64_t min, uint64_t max,
                                                     std::vector<SmallSSTable*> &files_to_compaction)
{
    uint64_t merge_length = 0;
    for (typename std::list<std::pair<int, SmallSSTable>>::iterator buffer_it = buffer_.begin(); buffer_it != buffer_.end(); ++buffer_it)
    {
        if (buffer_it->first == level && buffer_it->second.header_.max_ele_key_ >= min && buffer_it->second.header_.min_ele_key_ < max)
        {
            files_to_compaction.push_back(&(buffer_it->second));
            merge_length += buffer_it->second.header_.length_;
        }
    }
    return merge_length;
}

template <class KEY, class VALUE>
void Memory<KEY, VALUE>::WriteToDisk(int level, std::vector<std::pair<KEY, VALUE>> &data)
{
    SSTable<KEY, VALUE> sstable(data, BLOOM_FILTER_SIZE_);
    SmallSSTable small_sstable(data, BLOOM_FILTER_SIZE_);
    small_sstable.header_.timestamp_ = SSTable<KEY, VALUE>::timestamp_;
    buffer_.push_back({level, small_sstable});
    sstable.SSTableOut(output_path_ + "level" + std::to_string(level) + "/");
}

template <class KEY, class VALUE>
std::tuple<uint64_t, uint64_t, KEY, KEY> Memory<KEY, VALUE>::ReadHead(std::string filename) const
{
    std::ifstream sstable_in(filename, std::ios::in | std::ios::binary);
    if (!sstable_in)
    {
        std::cerr << "Failed to open file " << filename << "\n";
        return std::tuple<uint64_t, uint64_t, KEY, KEY>{0, 0, 0, 0};
    }
    uint64_t timestamp;
    uint64_t length;
    KEY max_ele_key;
    KEY min_ele_key;
    sstable_in.read((char*)&timestamp, sizeof(uint64_t));
    sstable_in.read((char*)&length, sizeof(uint64_t));
    sstable_in.read((char*)&max_ele_key, sizeof(KEY));
    sstable_in.read((char*)&min_ele_key, sizeof(KEY));
    sstable_in.close();
    return std::tuple<uint64_t, uint64_t, KEY, KEY>{timestamp, length, max_ele_key, min_ele_key};
}

template <class KEY, class VALUE>
void Memory<KEY, VALUE>::ReadFile(const file_index_t &file_index, std::vector<VALUE> &values, bool is_delete) const
{
    int level = std::get<0>(file_index);
    uint64_t timestamp = std::get<1>(file_index);
    uint64_t length = std::get<2>(file_index);
    KEY max_ele_key = std::get<3>(file_index);
    KEY min_ele_key = std::get<4>(file_index);
    std::string filename = std::to_string(timestamp) + "-" + std::to_string(length) + "-" +
            std::to_string(max_ele_key) + "-" + std::to_string(min_ele_key) + ".sst";
    std::ifstream sstable_in(output_path_ + "level" + std::to_string(level) + "/" + filename,
                             std::ios::in | std::ios::binary);
    if (!sstable_in)
    {
        std::cerr << "Failed to open file " << output_path_ + "level" + std::to_string(level) + "/" + filename << "\n";
        std::cerr << "Errno: " << errno << "\n";
        return;
    }
    uint64_t file_offset = sizeof(uint64_t) * 2 + sizeof(KEY) * 2 + BLOOM_FILTER_SIZE_;

    sstable_in.seekg(file_offset, std::ios::beg);
    std::list<std::pair<KEY, uint32_t>> index;

    for (uint64_t i = 0; i < length; ++i)
    {
        KEY key;
        uint32_t offset;
        sstable_in.read((char*)&key, sizeof(KEY));
        sstable_in.read((char*)&offset, sizeof(uint32_t));
        index.push_back({key, offset});
    }

    values.reserve(length);
    char ch[1024 * 64 + 1];
    memset(ch, '\0', (1024 * 64 + 1) * sizeof(char));
    for (typename std::list<std::pair<KEY, uint32_t>>::iterator key_it = index.begin();
         std::next(key_it, 1) != index.end();
         ++key_it)
    {
        VALUE value;
        sstable_in.read(ch, std::next(key_it, 1)->second - key_it->second);
        ch[std::next(key_it, 1)->second - key_it->second] = '\0';
        value = ch;
        values.push_back(value);
    }

    // read the last element
    VALUE value;
    int i = 0;
    while (!sstable_in.eof())
    {
        sstable_in.read(ch + i, sizeof(char));
        ++i;
    }
    ch[i] = '\0';
    value = ch;
    values.push_back(value);

    sstable_in.close();
    if (is_delete)
    {
        if (remove((output_path_ + "level" + std::to_string(level) + "/" + filename).c_str()) == -1)
        {
            std::cerr << "Failed to delete file " << output_path_ + "level" + std::to_string(level) + "/" + filename << "\n";
            std::cerr << "Errno: " << errno << "\n";
        }
    }
}

template <class KEY, class VALUE>
void Memory<KEY, VALUE>::MergeSort(typename std::vector<std::pair<KEY, item_index_t>>::const_iterator merge1_it,
               const typename std::vector<std::pair<KEY, item_index_t>>::const_iterator merge1_end,
               typename std::vector<std::pair<KEY, item_index_t>>::const_iterator merge2_it,
               const typename std::vector<std::pair<KEY, item_index_t>>::const_iterator merge2_end,
               std::vector<std::pair<KEY, item_index_t>> &target) const
{
    while (merge1_it != merge1_end && merge2_it != merge2_end)
    {
        if (merge1_it->first < merge2_it->first)
        {
            target.push_back(*merge1_it);
            ++merge1_it;
        }
        else if (merge1_it->first > merge2_it->first)
        {
            target.push_back(*merge2_it);
            ++merge2_it;
        }
        else
        {
            if (std::get<1>(merge1_it->second.first) > std::get<1>(merge2_it->second.first))
            {
                target.push_back(*merge1_it);
            }
            else
            {
                target.push_back(*merge2_it);
            }
            ++merge1_it;
            ++merge2_it;
        }
    }
    while (merge1_it != merge1_end)
    {
        target.push_back(*merge1_it);
        ++merge1_it;
    }
    while (merge2_it != merge2_end)
    {
        target.push_back(*merge2_it);
        ++merge2_it;
    }
}

template <class KEY, class VALUE>
void Memory<KEY, VALUE>::PackSmallSSTable(SmallSSTable* table, int level, std::vector<std::pair<KEY, item_index_t>> &table_package) const
{
    table_package.clear();
    uint64_t timestamp = table->header_.timestamp_;
    uint64_t length = table->header_.length_;
    KEY max_ele_key = table->header_.max_ele_key_;
    KEY min_ele_key = table->header_.min_ele_key_;
    uint32_t pos = 0;
    for (typename std::vector<std::pair<KEY, uint32_t>>::iterator table_it = table->index_.begin();
         table_it != table->index_.end();
         ++table_it)
    {
        item_index_t item_index{{level, timestamp, length, max_ele_key, min_ele_key}, pos};
        table_package.push_back({table_it->first, item_index});
        ++pos;
    }
}

template <class KEY, class VALUE>
void Memory<KEY, VALUE>::DeleteSmallSSTable(int level, SmallSSTable* table)
{
    for (typename std::list<std::pair<int, SmallSSTable>>::iterator buffer_it = buffer_.begin();
         buffer_it != buffer_.end();
         ++buffer_it)
    {
        if (buffer_it->first == level && buffer_it->second.header_ == table->header_)
        {
            buffer_.erase(buffer_it);
            return;
        }
    }
}

// if key is not found in this table, it will not change the value of offset
template <class KEY, class VALUE>
bool Memory<KEY, VALUE>::FindKey(KEY key, const SmallSSTable* table, uint32_t &offset) const
{
    uint64_t index = 0;
    for (typename std::vector<std::pair<KEY, uint32_t>>::const_iterator index_it = table->index_.begin();
         index_it != table->index_.end();
         ++index_it)
    {
        if (key == index_it->first)
        {
            offset = index;
            return true;
        }
        ++index;
    }
    return false;
}

template <class KEY, class VALUE>
VALUE Memory<KEY, VALUE>::FindValue(int level, const SmallSSTable* table, uint32_t offset) const
{
    item_index_t item_index{{level,
                    table->header_.timestamp_,
                    table->header_.length_,
                    table->header_.max_ele_key_,
                    table->header_.min_ele_key_, },
                           offset};
    std::vector<VALUE> values;
    ReadFile(item_index.first, values, false);
    VALUE value = "";
    try
    {
        value = values.at(offset);
    }
    catch (std::out_of_range e)
    {
        std::cerr << e.what() << std::endl;
    }
    return value;
}

template <class KEY, class VALUE>
std::vector<std::string> Memory<KEY, VALUE>::Split(const std::string &str, char delim) const
{
    std::istringstream iss(str);
    std::string item;
    std::vector<std::string> items;
    items.clear();
    while (getline(iss, item, delim))
    {
        items.push_back(item);
    }
    return items;
}

template <class KEY, class VALUE>
void Memory<KEY, VALUE>::PackSmallSSTableRange(std::vector<std::pair<int, const SmallSSTable*>> &tables,
                                               uint64_t min,
                                               uint64_t max,
                                               std::vector<std::vector<std::pair<KEY, item_index_t>>> &tables_package) const
{
    tables_package.reserve(tables.size());
    for (typename std::vector<std::pair<int, const SmallSSTable*>>::const_iterator table = tables.begin();
         table != tables.end();
         ++table)
    {
        uint64_t timestamp = table->second->header_.timestamp_;
        uint64_t length = table->second->header_.length_;
        KEY max_ele_key = table->second->header_.max_ele_key_;
        KEY min_ele_key = table->second->header_.min_ele_key_;
        uint32_t pos = 0;
        std::vector<std::pair<KEY, item_index_t>> table_package;
        for (typename std::vector<std::pair<KEY, uint32_t>>::const_iterator table_it = table->second->index_.begin();
             table_it != table->second->index_.end();
             ++table_it)
        {
            if (table_it->first >= min && table_it->first <= max)
            {
                item_index_t item_index{{table->first, timestamp, length, max_ele_key, min_ele_key}, pos};
                table_package.push_back({table_it->first, item_index});
            }
            ++pos;
        }
        tables_package.push_back(table_package);
    }
}

template <class KEY, class VALUE>
void Memory<KEY, VALUE>::ScanBuffer(uint64_t min, uint64_t max,
                                    std::vector<std::pair<int, const SmallSSTable*>> &files_to_scan) const
{
    for (typename std::list<std::pair<int, SmallSSTable>>::const_iterator buffer_it = buffer_.begin();
         buffer_it != buffer_.end();
         ++buffer_it)
    {
        if (buffer_it->second.header_.min_ele_key_ < max && buffer_it->second.header_.max_ele_key_ > min)
        {
            files_to_scan.push_back({buffer_it->first, &(buffer_it->second)});
        }
    }
}

template <class KEY, class VALUE>
void Memory<KEY, VALUE>::Merge(std::vector<std::vector<std::pair<KEY, item_index_t>>> &tables_package,
                               std::vector<std::pair<KEY, item_index_t>> &merge_result) const
{
    uint64_t merge_length = 0;
    for (typename std::vector<std::vector<std::pair<KEY, item_index_t>>>::const_iterator table_package = tables_package.begin();
         table_package != tables_package.end();
         ++table_package)
    {
        merge_length += table_package->size();
    }

    std::vector<std::pair<KEY, item_index_t>> merge_tapes[2];
    merge_tapes[0].reserve(merge_length);
    merge_tapes[1].reserve(merge_length);

    int circle_index = 0;

    for (typename std::vector<std::vector<std::pair<KEY, item_index_t>>>::const_iterator table_package = tables_package.begin();
         table_package != tables_package.end();
         ++table_package)
    {
        MergeSort(table_package->begin(), table_package->end(),
                  merge_tapes[circle_index % 2].begin(), merge_tapes[circle_index % 2].end(),
                merge_tapes[(circle_index + 1) % 2]);
        merge_tapes[circle_index % 2].clear();
        circle_index = (circle_index + 1) % 2;
    }

    merge_result.reserve(merge_tapes[circle_index].size());

    for (typename std::vector<std::pair<KEY, item_index_t>>::const_iterator result = merge_tapes[circle_index].begin();
         result != merge_tapes[circle_index].end();
         ++result)
    {
        merge_result.push_back(*result);
    }
}

template <class KEY, class VALUE>
void Memory<KEY, VALUE>::ReorganizeScanResult(std::vector<std::pair<KEY, item_index_t>> &files_data,
                          std::list<std::pair<KEY, VALUE>> &list) const
{
    typename std::vector<std::pair<KEY, item_index_t>>::const_iterator data_it = files_data.begin();
    typename std::list<std::pair<KEY, VALUE>>::const_iterator list_it = list.begin();
    std::map<file_index_t, std::vector<VALUE>> file_to_value;
    while (data_it != files_data.end() && list_it != list.end())
    {
        if (data_it->first < list_it->first)
        {
            VALUE value;
            if (file_to_value.count(data_it->second.first) == 0)
            {
                std::vector<VALUE> values;
                ReadFile(data_it->second.first, values, false);
                file_to_value.insert({data_it->second.first, values});
                value = values.at(data_it->second.second);
            }
            else
            {
                std::vector<VALUE> &values = file_to_value.at(data_it->second.first);
                value = values.at(data_it->second.second);
            }
            if (value != "~DELETED~")
            {
                list.insert(list_it, {data_it->first, value});
            }
            ++data_it;
        }
        else if (data_it->first == list_it->first)
        {
            ++data_it;
        }
        else
        {
            ++list_it;
        }
    }
    while (data_it != files_data.end())
    {
        VALUE value;
        if (file_to_value.count(data_it->second.first) == 0)
        {
            std::vector<VALUE> values;
            ReadFile(data_it->second.first, values, false);
            file_to_value.insert({data_it->second.first, values});
            value = values.at(data_it->second.second);
        }
        else
        {
            std::vector<VALUE> &values = file_to_value.at(data_it->second.first);
            value = values.at(data_it->second.second);
        }
        if (value != "~DELETED~")
        {
            list.insert(list_it, {data_it->first, value});
        }
        ++data_it;
    }
}

template <class KEY, class VALUE>
bool Memory<KEY, VALUE>::Exist(const KEY &key) const
{
    if (list_->Exist(key))
    {
        if (list_->Search(key) == "~DELETED~")
        {
            return false;
        }
        return true;
    }

    uint64_t timestamp = 0;
    const SmallSSTable* tmp = nullptr;
    uint32_t offset = 0;
    int level = 0;
    for (typename std::list<std::pair<int, SmallSSTable>>::const_iterator buffer_it = buffer_.begin();
         buffer_it != buffer_.end();
         ++buffer_it)
    {
        if (buffer_it->second.header_.timestamp_ > timestamp && buffer_it->second.filter_.Exist(key))
        {
            if (FindKey(key, &(buffer_it->second), offset))
            {
                timestamp = buffer_it->second.header_.timestamp_;
                tmp = &(buffer_it->second);
                level = buffer_it->first;
            }
        }
    }
    if (tmp != nullptr)
    {
        VALUE value = FindValue(level, tmp, offset);
        if (value != "~DELETED~")
        {
            return true;
        }
    }
    return false;
}

template <class KEY, class VALUE>
void Memory<KEY, VALUE>::Compaction(std::vector<SmallSSTable*> &files_to_compaction, int next_level)
{
    uint64_t min_ele_key = UINT64_MAX;
    uint64_t max_ele_key = 0;
    uint64_t merge_length = 0;

    for (typename std::vector<SmallSSTable*>::iterator file_it = files_to_compaction.begin();
         file_it != files_to_compaction.end();
         ++file_it)
    {
        uint64_t tmp_max_ele_key = (*file_it)->header_.max_ele_key_;
        uint64_t tmp_min_ele_key = (*file_it)->header_.min_ele_key_;
        max_ele_key = (tmp_max_ele_key > max_ele_key)? tmp_max_ele_key : max_ele_key;
        min_ele_key = (tmp_min_ele_key < min_ele_key)? tmp_min_ele_key : min_ele_key;
        merge_length += (*file_it)->header_.length_;
    }

    std::vector<SmallSSTable*> next_level_files_to_compaction;
    merge_length += GetCompactionFilesRange(next_level, min_ele_key, max_ele_key, next_level_files_to_compaction);

    std::vector<std::pair<KEY, item_index_t>> merge_tapes[3];
    merge_tapes[0].reserve(merge_length);
    merge_tapes[1].reserve(merge_length);
    merge_tapes[2].reserve(merge_length);

    int circle_index = 0;

    if (next_level - 1 == 0)
    {
        for (typename std::vector<SmallSSTable*>::iterator file_it = files_to_compaction.begin();
             file_it != files_to_compaction.end();
             ++file_it)
        {
            PackSmallSSTable(*file_it, next_level - 1, merge_tapes[circle_index]);
            MergeSort(merge_tapes[(circle_index) % 3].begin(), merge_tapes[(circle_index) % 3].end(),
                    merge_tapes[(circle_index + 1) % 3].begin(), merge_tapes[(circle_index + 1) % 3].end(),
                    merge_tapes[(circle_index + 2) % 3]);
            merge_tapes[circle_index].clear();
            circle_index = (circle_index + 1) % 3;
            DeleteSmallSSTable(next_level - 1, *file_it);
        }
    }
    else
    {
        for (typename std::vector<SmallSSTable*>::iterator file_it = files_to_compaction.begin();
             file_it != files_to_compaction.end();
             ++file_it)
        {
            PackSmallSSTable(*file_it, next_level - 1, merge_tapes[circle_index]);
            merge_tapes[(circle_index + 2) % 3].insert(merge_tapes[(circle_index + 2) % 3].end(),
                    merge_tapes[circle_index].begin(),
                    merge_tapes[circle_index].end());
            DeleteSmallSSTable(next_level - 1, *file_it);
        }
        merge_tapes[circle_index].clear();
        circle_index = (circle_index + 1) % 3;
    }

    for (typename std::vector<SmallSSTable*>::iterator file_it = next_level_files_to_compaction.begin();
         file_it != next_level_files_to_compaction.end();
         ++file_it)
    {
        PackSmallSSTable(*file_it, next_level, merge_tapes[circle_index]);
        merge_tapes[(circle_index + 2) % 3].insert(merge_tapes[(circle_index + 2) % 3].end(),
                merge_tapes[circle_index].begin(),
                merge_tapes[circle_index].end());
        DeleteSmallSSTable(next_level, *file_it);
    }
    merge_tapes[circle_index].clear();
    circle_index = (circle_index + 1) % 3;

    MergeSort(merge_tapes[(circle_index) % 3].begin(), merge_tapes[(circle_index) % 3].end(),
            merge_tapes[(circle_index + 1) % 3].begin(), merge_tapes[(circle_index + 1) % 3].end(),
            merge_tapes[(circle_index + 2) % 3]);
    merge_tapes[circle_index].clear();

    circle_index = (circle_index + 2) % 3;                              // final tape index

    std::map<file_index_t, std::vector<VALUE>> file_to_value;
    std::vector<std::pair<KEY, VALUE>> data;
    int curr_size = 0;
    for (typename std::vector<std::pair<KEY, item_index_t>>::iterator item_it = merge_tapes[circle_index].begin();
         item_it != merge_tapes[circle_index].end();
         ++item_it)
    {
        VALUE value;
        if (file_to_value.count(item_it->second.first) == 0)
        {
            std::vector<VALUE> values;
            ReadFile(item_it->second.first, values, true);
            file_to_value.insert({item_it->second.first, values});
            value = values.at(item_it->second.second);
        }
        else
        {
            std::vector<VALUE> &values = file_to_value.at(item_it->second.first);
            value = values.at(item_it->second.second);
        }

        if (value != "~DELETED~")
        {
            curr_size += sizeof(KEY) + sizeof(uint32_t) + sizeof(char) * value.length();
            data.push_back({item_it->first, value});
        }
        if (curr_size >= MAX_SIZE_ - BLOOM_FILTER_SIZE_)
        {
            WriteToDisk(next_level, data);
            curr_size = 0;
            data.clear();
        }
    }

    for (typename std::vector<std::pair<KEY, VALUE>>::iterator data_it = data.begin(); data_it != data.end();)
    {
        if (data_it->second == "~DELETED~")
        {
            data_it = data.erase(data_it);
        }
        else
        {
            ++data_it;
        }
    }

    if (!data.empty())
    {
        WriteToDisk(next_level, data);
        curr_size = 0;
        data.clear();
    }

    if (NeedCompaction(next_level))
    {
        std::vector<SmallSSTable*> compaction_files;
        GetCompactionFiles(next_level, compaction_files);
        Compaction(compaction_files, next_level + 1);
    }
}

template <class KEY, class VALUE>
void Memory<KEY, VALUE>::Put(KEY key, VALUE value)
{
    int prev_size = list_->Insert(key, value);
    if (prev_size == 0)
    {
        current_size_ += sizeof(key) + sizeof(char) * value.length() + sizeof(uint32_t);
        element_num_ += 1;
    }
    else if (prev_size > 0)
    {
        current_size_ += sizeof(char) * value.length() - prev_size;
    }
    if (current_size_ >= MAX_SIZE_ - BLOOM_FILTER_SIZE_)
    {
        ++SSTable<KEY, VALUE>::timestamp_;
        std::vector<std::pair<KEY, VALUE>> data = list_->ScanAll();
        WriteToDisk(0, data);
        if (NeedCompaction(0))
        {
            std::vector<SmallSSTable*> compaction_files;
            GetCompactionFiles(0, compaction_files);
            Compaction(compaction_files, 1);
        }
        current_size_ = 0;
        list_->Reset();
    }
}

template <class KEY, class VALUE>
VALUE Memory<KEY, VALUE>::Get(const KEY &key) const
{
    VALUE value = list_->Search(key);
    if (value != "")
    {
        if (value == "~DELETED~")
        {
            return "";
        }
        return value;
    }
    else
    {
        uint64_t timestamp = 0;
        const SmallSSTable* tmp = nullptr;
        uint32_t offset = 0;
        int level = 0;
        for (typename std::list<std::pair<int, SmallSSTable>>::const_iterator buffer_it = buffer_.begin();
             buffer_it != buffer_.end();
             ++buffer_it)
        {
            if (buffer_it->second.header_.timestamp_ > timestamp && buffer_it->second.filter_.Exist(key))
            {
                if (FindKey(key, &(buffer_it->second), offset))
                {
                    timestamp = buffer_it->second.header_.timestamp_;
                    tmp = &(buffer_it->second);
                    level = buffer_it->first;
                }
            }
        }
        if (tmp == nullptr)
        {
            return "";
        }
        VALUE value = FindValue(level, tmp, offset);
        if (value == "~DELETED~")
        {
            return "";
        }
        else
        {
            return value;
        }
    }
}

template <class KEY, class VALUE>
bool Memory<KEY, VALUE>::Del(const KEY &key)
{
    if (Exist(key))
    {
        Put(key, "~DELETED~");
        return true;
    }
    return false;
}

template <class KEY, class VALUE>
void Memory<KEY, VALUE>::Reset()
{
    delete list_;
    list_ = new SkipList<KEY, VALUE>();
}

template <class KEY, class VALUE>
void Memory<KEY, VALUE>::Scan(const KEY &key1, const KEY &key2, std::list<std::pair<KEY, VALUE>> &list) const
{
    list_->Scan(key1, key2, list);
    for (typename std::list<std::pair<KEY, VALUE>>::iterator list_it = list.begin(); list_it != list.end();)
    {
        if (list_it->second == "~DELETED~")
        {
            list_it = list.erase(list_it);
        }
        else
        {
            ++list_it;
        }
    }

    std::vector<std::pair<int, const SmallSSTable*>> files_to_scan;
    ScanBuffer(key1, key2, files_to_scan);
    std::vector<std::vector<std::pair<KEY, item_index_t>>> tables_package;
    PackSmallSSTableRange(files_to_scan, key1, key2, tables_package);
    std::vector<std::pair<KEY, item_index_t>> merge_result;
    Merge(tables_package, merge_result);

    ReorganizeScanResult(merge_result, list);
}

template class Memory<uint64_t, std::string>;
