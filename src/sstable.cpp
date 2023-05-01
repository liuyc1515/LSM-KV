#include "sstable.h"

template <class KEY, class VALUE>
SSTable<KEY, VALUE>::SSTable(const std::vector<std::pair<KEY, VALUE>> &data, int bloom_filter_size):
    header_(), filter_(bloom_filter_size)
{
    header_.timestamp_ = timestamp_;
    index_.clear();
    data_.clear();
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
        data_.push_back(it->second);
    }
}

template <class KEY, class VALUE>
SSTable<KEY, VALUE>::~SSTable()
{

}

template <class KEY, class VALUE>
int SSTable<KEY, VALUE>::makedir(std::string dir_name)
{
#if defined(_MSC_VER)
    return mkdir(dir_name.c_str());
#else
    return mkdir(dir_name.c_str(), 0755);
#endif
}

template <class KEY, class VALUE>
bool SSTable<KEY, VALUE>::SSTableOut(std::string output_path)
{
    std::ofstream out;
    if (output_path[output_path.length() - 1] != '/')
    {
        output_path.append("/");
    }
    if (access(output_path.c_str(), 0) == -1)
    {
        if (makedir(output_path) == -1)
        {
            return false;
        }
    }
    std::string filename = std::to_string(header_.timestamp_) + "-" + std::to_string(header_.length_) + "-" +
            std::to_string(header_.max_ele_key_) + "-" + std::to_string(header_.min_ele_key_) + ".sst";
    out.open(output_path + filename, std::ios::out | std::ios::binary);
    if (!out.is_open())
    {
        return false;
    }
    out.write((char*)&(header_.timestamp_), sizeof(uint64_t));
    out.write((char*)&(header_.length_), sizeof(uint64_t));
    out.write((char*)&(header_.max_ele_key_), sizeof(KEY));
    out.write((char*)&(header_.min_ele_key_), sizeof(KEY));
    out.write((char*)&(filter_.table_), sizeof(bool) * filter_.m_);
    for (typename std::vector<std::pair<KEY, uint32_t>>::iterator index_it = index_.begin(); index_it != index_.end(); ++index_it)
    {
        out.write((char*)&(index_it->first), sizeof(KEY));
        out.write((char*)&(index_it->second), sizeof(uint32_t));
    }
    for (typename std::vector<VALUE>::iterator data_it = data_.begin(); data_it != data_.end(); ++data_it)
    {
        const char* ch = (*data_it).c_str();
        VALUE value = *data_it;
        out.write(ch, sizeof(char) * value.length());
    }
    out.close();
    return true;
}

template class SSTable<uint64_t, std::string>;
