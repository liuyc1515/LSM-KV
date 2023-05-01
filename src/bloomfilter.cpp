#include "bloomfilter.h"

template <class T>
BloomFilter<T>::BloomFilter(int m)
{
    if (m > MAX_SIZE_)
    {
        m_ = MAX_SIZE_;
    }
    else
    {
        m_ = m;
    }
    srand(time(NULL));
    memset(table_, false, m_);
}

template <class T>
BloomFilter<T>::~BloomFilter()
{

}

template <class T>
void BloomFilter<T>::Insert(const T &data)
{

    uint32_t pos[4];
    MurmurHash3_x64_128(&data, sizeof(T), 1, pos);
    for (int i = 0; i < 4; ++i)
    {
        table_[pos[i] % m_] = true;
    }
}

template <class T>
bool BloomFilter<T>::Exist(const T &data) const
{
    uint32_t pos[4];
    MurmurHash3_x64_128(&data, sizeof(T), 1, pos);
    for (int i = 0; i < 4; ++i)
    {
        if (!table_[pos[i] % m_])
        {
            return false;
        }
    }
    return true;
}

template class BloomFilter<uint64_t>;
