/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duolong
 *
 */

#ifndef __DLINK_LIST__
#define __DLINK_LIST__

namespace tbsys {


	/** 
	 * @brief 链表
	 */
template <typename NodeT>
class LinkList
{
    public:
        typedef NodeT* node_pointer_type;
        typedef LinkList<NodeT> self_type;

    public:
        LinkList();
        ~LinkList();

        void append(NodeT* node);
        void remove(NodeT* node);
        void combine(const LinkList<NodeT>& al);
        void reset();

        NodeT* head() const { return _head; }
        NodeT* tail() const { return _tail; }
        void head(NodeT* h) { _head = h; }
        void tail(NodeT* t) { _tail = t; }


        bool empty() const { return !(_head && _tail); }

    private:
        NodeT * _head;
        NodeT * _tail;
};

template <typename NodeT>
LinkList<NodeT>::LinkList()
{
    reset();
}

template <typename NodeT>
void LinkList<NodeT>::reset()
{
    _head = _tail = NULL;
}

template <typename NodeT>
LinkList<NodeT>::~LinkList()
{
}

template <typename NodeT>
void LinkList<NodeT>::append(NodeT* node)
{
    if (!node) return;

    node->_prev = _tail;
    node->_next = NULL;

    if (!_tail) {
        _head = node;
    } else {
        _tail->_next = node;
    }
    _tail = node;
}

template <typename NodeT>
void LinkList<NodeT>::remove(NodeT* node)
{
    if (!node) return;

    if (node == _head) { // head
        _head = node->_next;
    }
    if (node == _tail) { // tail
        _tail = node->_prev;
    }

    if (node->_prev != NULL)
        node->_prev->_next = node->_next;
    if (node->_next != NULL)
        node->_next->_prev = node->_prev;

}

template <typename NodeT>
void LinkList<NodeT>::combine(const LinkList<NodeT>& al)
{
    if (al.empty()) return;

    if (!_tail) {
        _head = al.head();
    } else {
        _tail->_next = al.head();
        al.head()->_prev = _tail;
    }
    _tail = al.tail();
}

}

#endif














