#pragma once

#include <vector>
#include <climits>
#include <cstdlib>
#include <string>
#include <iostream>
#include <list>

#define MAX_LEVEL 8



template <class KEY, class VALUE>
class SkipList
{
private:
    enum SKNodeType
    {
        HEAD = 1,
        NORMAL,
        NIL
    };

    struct SKNode
    {
        KEY key;
        VALUE val;
        int height;
        SKNodeType type;
        std::vector<SKNode*> forwards;
        SKNode(KEY _key, VALUE _val,int level,std::vector<SKNode*> &backward,std::vector<SKNode*> &forward);
        SKNode(KEY _key, VALUE _val, SKNodeType _type);
    };

    SKNode *head;
    SKNode *nil;
    unsigned long long s = 1;
    double MyRand();
    int RandomLevel();

public:
    SkipList();
    int Insert(const KEY &key, const VALUE &value);
    VALUE Search(const KEY &key) const;
    bool Exist(const KEY &key) const;
    bool SetDelete(const KEY &key);
    void Delete(const KEY &key);
    void Reset();
    void Scan(const KEY &key1, const KEY &key2, std::list<std::pair<KEY, VALUE>> &list) const;
    std::vector<std::pair<KEY, VALUE>> ScanAll() const;
    void Display() const;
    ~SkipList();
};
