#include <iostream>
#include <stdlib.h>

#include "skiplist.h"

template <class KEY, class VALUE>
SkipList<KEY, VALUE>::SkipList()
{
    head = new SKNode(0, "", HEAD);
    nil = new SKNode(UINT64_MAX, "", NIL);
    for (int i = 0; i < MAX_LEVEL; ++i)
    {
        head->forwards[i] = nil;
    }
    srand(1);
}

template <class KEY, class VALUE>
SkipList<KEY, VALUE>::~SkipList()
{
    SKNode *n1 = head;
    SKNode *n2;
    while (n1)
    {
        n2 = n1->forwards[0];
        delete n1;
        n1 = n2;
    }
}

template <class KEY, class VALUE>
double SkipList<KEY, VALUE>::MyRand()
{
    s = (16807 * s) % 2147483647ULL;
    return (s + 0.0) / 2147483647ULL;
}

template <class KEY, class VALUE>
int SkipList<KEY, VALUE>::RandomLevel()
{
    int result = 1;
    while (result < MAX_LEVEL && MyRand() < 0.5)
    {
        ++result;
    }
    return result;
}

template <class KEY, class VALUE>
SkipList<KEY, VALUE>::SKNode::SKNode(KEY _key, VALUE _val,int level,std::vector<SKNode*> &backward,std::vector<SKNode*> &forward):
    key(_key), val(_val),height(level), type(SKNodeType::NORMAL)
{
    for (int i = 0; i < level; ++i)
    {
        backward[MAX_LEVEL-i-1]->forwards[i] = this;
        forwards.push_back(forward[MAX_LEVEL - i - 1]);
    }
}

template <class KEY, class VALUE>
SkipList<KEY, VALUE>::SKNode::SKNode(KEY _key, VALUE _val, SKNodeType _type)
{
    key = _key;
    val = _val;
    type = _type;
    for (int i = 0; i < MAX_LEVEL; ++i)
    {
        forwards.push_back(NULL);
    }
}

/*
 * return 0 if key is not exist
 * return -1 if insert failed
 * return sizeof(value) if key is already exist
 */
template <class KEY, class VALUE>
int SkipList<KEY, VALUE>::Insert(const KEY &key, const VALUE &value)
{
    SKNode* tmp = head;
    int level = MAX_LEVEL;
    std::vector<SKNode*> backward;
    std::vector<SKNode*> forward;
    while (level)
    {
        backward.push_back(tmp);
        forward.push_back(tmp->forwards[level - 1]);
        while (key>tmp->forwards[level - 1]->key)
        {
            tmp=tmp->forwards[level - 1];
            backward[MAX_LEVEL - level]=tmp;
            forward[MAX_LEVEL - level]=tmp->forwards[level - 1];
        }
        if (key==tmp->forwards[level-1]->key)
        {
            int size = sizeof(char) * (tmp->forwards[level - 1]->val).length();
            tmp->forwards[level-1]->val = value;
            return size;
        }
        level -= 1;
    }
    SKNode* node = new SKNode(key, value, RandomLevel(), backward,forward);
    if (node == NULL)
    {
        return -1;
    }
    else
    {
        return 0;
    }
    // TODO
}

template <class KEY, class VALUE>
VALUE SkipList<KEY, VALUE>::Search(const KEY &key) const
{
    SKNode* tmp = head;
    int level = MAX_LEVEL;
    while (level)
    {
        while (key>tmp->forwards[level-1]->key)
        {
            tmp = tmp->forwards[level-1];
        }
        level -= 1;
    }
    if (key == tmp->forwards[level]->key)
    {
        return tmp->forwards[level]->val;
    }
    return "";
    // TODO
}

template <class KEY, class VALUE>
bool SkipList<KEY, VALUE>::Exist(const KEY &key) const
{
    SKNode* tmp = head;
    int level = MAX_LEVEL;
    while (level)
    {
        while (key > tmp->forwards[level-1]->key)
        {
            tmp = tmp->forwards[level-1];
        }
        level -= 1;
    }
    if (key == tmp->forwards[level]->key)
    {
        return true;
    }
    return false;
    // TODO
}

template <class KEY, class VALUE>
bool SkipList<KEY, VALUE>::SetDelete(const KEY &key)
{
    SKNode* tmp = head;
    int level = MAX_LEVEL;
    while (level)
    {
        while (key > tmp->forwards[level-1]->key)
        {
            tmp = tmp->forwards[level-1];
        }
        level -= 1;
    }
    if (key == tmp->forwards[level]->key)
    {
        if (tmp->forwards[level]->val == "~DELETED~")
        {
            return false;
        }
        tmp->forwards[level]->val = "~DELETED~";
        return true;
    }
    return false;
}

template <class KEY, class VALUE>
void SkipList<KEY, VALUE>::Delete(const KEY &key)
{
    SKNode* tmp = head;
    int level = MAX_LEVEL;
    int deleterLevel = 0;
    bool exist = false;
    SKNode* deleter = NULL;
    std::vector<SKNode*> backward;
    std::vector<SKNode*> forward;
    while (level)
    {
        backward.push_back(tmp);
        forward.push_back(tmp->forwards[level-1]);
        while (key>tmp->forwards[level-1]->key)
        {
            tmp = tmp->forwards[level-1];
            backward[MAX_LEVEL-level] = tmp;
            forward[MAX_LEVEL-level] = tmp->forwards[level-1];
        }
        if (key == tmp->forwards[level-1]->key&&!exist)
        {
            deleterLevel = level;
            exist = true;
            deleter = tmp->forwards[level-1];
        }
        level -= 1;
    }
    if (exist)
    {
        for (int i = 0; i < deleterLevel; ++i)
        {
            backward[MAX_LEVEL-i-1]->forwards[i] = forward[MAX_LEVEL - i - 1]->forwards[i];
        }
        delete deleter;
    }
    // TODO
}

template <class KEY, class VALUE>
void SkipList<KEY, VALUE>::Reset()
{
    SKNode *n1 = head -> forwards[0];
    SKNode *n2;
    while (n1 != nil)
    {
        n2 = n1->forwards[0];
        delete n1;
        n1 = n2;
    }
    for (int i = 0; i < MAX_LEVEL; ++i)
    {
        head->forwards[i] = nil;
    }
}

template <class KEY, class VALUE>
void SkipList<KEY, VALUE>::Scan(const KEY &key1, const KEY &key2, std::list<std::pair<KEY, VALUE>> &list) const
{
    SKNode* tmp = head;
    int level=MAX_LEVEL;
    while (level)
    {
        while (key1 > tmp->forwards[level-1]->key)
        {
            tmp = tmp->forwards[level-1];
        }
        level -= 1;
    }
    while (tmp && key2 >= tmp->key)
    {
        if (tmp->type != HEAD)
            list.push_back(std::pair<KEY, VALUE>(tmp->key, tmp->val));
        tmp = tmp->forwards[0];
    }
}

template <class KEY, class VALUE>
std::vector<std::pair<KEY, VALUE>> SkipList<KEY, VALUE>::ScanAll() const
{
    std::vector<std::pair<KEY, VALUE>> data;
    data.clear();
    SKNode* tmp = head->forwards[0];
    while (tmp != NULL && tmp != nil)
    {
        data.push_back(std::pair<KEY, VALUE>(tmp->key, tmp->val));
        tmp = tmp->forwards[0];
    }
    return data;
}

template <class KEY, class VALUE>
void SkipList<KEY, VALUE>::Display() const
{
    for (int i = MAX_LEVEL - 1; i >= 0; --i)
    {
        std::cout << "Level " << i + 1 << ":h";
        SKNode* node = head->forwards[i];
        while (node->type != SKNodeType::NIL)
        {
            std::cout << "-->(" << node->key << "," << node->val << ")";
            node = node->forwards[i];
        }
        std::cout << "-->N" << std::endl;
    }
}

template class SkipList<uint64_t, std::string>;
