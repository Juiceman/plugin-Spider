package org.garret.perst.impl;

class LRU {
  LRU next;
  LRU prev;

  LRU() {
    next = prev = this;
  }

  final void link(LRU node) {
    node.next = next;
    node.prev = this;
    next.prev = node;
    next = node;
  }

  final void unlink() {
    next.prev = prev;
    prev.next = next;
  }
}

