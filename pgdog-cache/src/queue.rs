use std::{
    mem,
    ops::{Index, IndexMut},
};

use crate::CachePolicy;

/// Where a [`Node`] is stored in the queue.
/// Stale once its item is removed or the queue is cleared.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeIndex(u32);

impl NodeIndex {
    fn new(index: usize) -> Self {
        // Going past u32 would take billions of prepared statements
        // in one connection's cache.
        Self(u32::try_from(index).expect("NodeIndex must fit in u32"))
    }

    #[inline]
    const fn get(self) -> usize {
        self.0 as usize
    }
}

#[repr(C)] // Stops Rust moving the links behind the item.
pub struct Node<T> {
    prev: NodeIndex,
    next: NodeIndex,

    bucket: BucketIndex,

    /// Where the item's key sat in the cache's table when it was inserted,
    /// so eviction can find it without hashing. A rehash moves keys between
    /// buckets, so a reader has to check before trusting it.
    /// Meaningless when `kind` is not `Item`.
    table_hint: u32,

    kind: NodeKind<T>,
}

#[derive(Default)]
enum NodeKind<T> {
    #[default]
    Vacant,
    Item(T),
    Bucket(Bucket),
}

/// Where a [`Bucket`] is stored in the queue, which lives in the head node of its list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BucketIndex {
    head: NodeIndex,
}

/// Items that share a use count, in a circular list from the head's `next`
/// (least recent) to its `prev` (most recent).
#[derive(Clone, Copy)]
pub struct Bucket {
    /// The use count of every item in this bucket.
    /// Stays 1 under [`CachePolicy::LeastRecentlyUsed`].
    count: u64,

    /// The bucket with the next lower `count`.
    prev: Option<BucketIndex>,

    /// The bucket with the next higher `count`.
    next: Option<BucketIndex>,
}

/// Items in [`CachePolicy`] eviction order, stored as linked nodes in a slab.
pub struct EvictionQueue<T> {
    policy: CachePolicy,
    nodes: Vec<Node<T>>,

    /// Vacant nodes, reused before growing `nodes`.
    free_nodes: Vec<NodeIndex>,

    /// The bucket with the lowest `count`, whose least recent item is evicted next.
    /// Counts strictly increase from here.
    first_bucket: Option<BucketIndex>,
}

impl<T> Default for EvictionQueue<T> {
    fn default() -> Self {
        Self {
            policy: CachePolicy::default(),
            nodes: Vec::new(),
            free_nodes: Vec::new(),
            first_bucket: None,
        }
    }
}

impl<T> EvictionQueue<T> {
    /// Creates an empty `EvictionQueue` with the specified capacity of items.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            policy: CachePolicy::default(),
            // Reserve one extra node for the first bucket.
            nodes: Vec::with_capacity(capacity + 1),
            free_nodes: Vec::new(),
            first_bucket: None,
        }
    }

    /// Returns the current eviction policy.
    pub fn policy(&self) -> CachePolicy {
        self.policy
    }

    /// Sets the eviction policy, keeping the current eviction order.
    pub fn configure(&mut self, policy: CachePolicy) {
        if mem::replace(&mut self.policy, policy) == policy {
            return;
        }

        // LRU keeps every item in one bucket with count 1,
        // which is already a valid LFU state.
        if policy == CachePolicy::LeastFrequentlyUsed {
            return;
        }

        let Some(first) = self.first_bucket else {
            // The queue is empty, so there's nothing to merge.
            return;
        };

        // Merge all buckets from lowest to highest `count`.
        // No unlinking, since every item is relinked.
        while let Some(bucket) = self[first].next {
            let mut node = self[bucket.head].next;
            while node != bucket.head {
                let next = self[node].next;
                self.link_to_back(first, node);
                node = next;
            }

            self.remove_bucket(bucket);
        }

        // Reset the now single bucket's count to 1.
        // `push` only joins the first bucket at count 1.
        self[first].count = 1;
    }

    /// Returns the first node in eviction order, or `None` if the queue is empty.
    #[inline]
    pub fn front(&self) -> Option<NodeIndex> {
        self.first_bucket.map(|bucket| self[bucket.head].next)
    }

    /// Returns a reference to the item at `node`.
    #[inline]
    pub fn get(&self, node: NodeIndex) -> &T {
        match &self[node].kind {
            NodeKind::Item(item) => item,

            _ => unreachable!("NodeIndex should point to an item"),
        }
    }

    /// Returns a mutable reference to the item at `node`.
    #[inline]
    pub fn get_mut(&mut self, node: NodeIndex) -> &mut T {
        match &mut self[node].kind {
            NodeKind::Item(item) => item,

            _ => unreachable!("NodeIndex should point to an item"),
        }
    }

    /// Returns the [`Node::table_hint`] of the item at `node`.
    #[inline]
    pub fn table_hint_mut(&mut self, node: NodeIndex) -> &mut u32 {
        &mut self[node].table_hint
    }

    /// Adds `item` with a use count of 1, after every other item at that count.
    #[inline]
    pub fn push(&mut self, item: T) -> NodeIndex {
        let bucket = match self.first_bucket {
            Some(bucket) if self[bucket].count == 1 => bucket,
            _ => self.insert_bucket_after(None, 1),
        };

        let node = self.allocate(|index| Node {
            kind: NodeKind::Item(item),
            prev: index,
            next: index,
            bucket,
            table_hint: 0,
        });

        self.link_to_back(bucket, node);

        node
    }

    /// Removes the item at `node` and returns it.
    #[inline]
    pub fn remove(&mut self, node: NodeIndex) -> T {
        self.detach(node);

        match self.free(node) {
            NodeKind::Item(item) => item,

            _ => unreachable!("NodeIndex should point to an item"),
        }
    }

    /// Records a **use** of the item at `node`.
    #[inline]
    pub fn promote(&mut self, node: NodeIndex) {
        match self.policy {
            CachePolicy::LeastRecentlyUsed => {
                let Node { next, bucket, .. } = self[node];
                debug_assert_eq!(self[bucket].count, 1);

                if next == bucket.head {
                    // The node is already the most recent node.
                    return;
                }

                self.unlink(node);
                self.link_to_back(bucket, node);
            }

            CachePolicy::LeastFrequentlyUsed => {
                self.promote_lfu(node);
            }
        }
    }

    #[inline(never)] // Out of line to allow `promote` to be inlined for LRU.
    fn promote_lfu(&mut self, node: NodeIndex) {
        let Node {
            prev, next, bucket, ..
        } = self[node];

        let Bucket {
            count,
            next: next_bucket,
            ..
        } = self[bucket];

        let count = count + 1;
        let head = bucket.head;

        if let Some(next_bucket) = next_bucket
            && self[next_bucket].count == count
        {
            // The next bucket has the new count, so move the node to it.
            self.detach(node);
            self.link_to_back(next_bucket, node);
        } else if prev == head && next == head {
            debug_assert!(next_bucket.is_none_or(|next_bucket| count < self[next_bucket].count));

            // The bucket only has this node.
            // Bump the count in place.
            self[bucket].count = count;
        } else {
            // Otherwise unlink the node from its bucket,
            // and insert it into the new bucket with the new count.
            self.unlink(node);
            let bucket = self.insert_bucket_after(Some(bucket), count);
            self.link_to_back(bucket, node);
        }
    }

    /// Removes every item, keeping the policy and the allocated memory.
    pub fn clear(&mut self) {
        self.nodes.clear();
        self.free_nodes.clear();
        self.first_bucket = None;
    }

    /// Creates an empty [`Bucket`] after `prev`, or first if `prev` is `None`.
    /// `count` must be strictly between its neighbors' counts.
    fn insert_bucket_after(&mut self, prev: Option<BucketIndex>, count: u64) -> BucketIndex {
        let next = match prev {
            Some(prev) => self[prev].next,
            None => self.first_bucket,
        };

        // Counts strictly increase from `first_bucket`.
        // `front` relies on that to find the lowest count.
        // `promote` to find count + 1 in the next bucket.
        debug_assert!(prev.is_none_or(|prev| self[prev].count < count));
        debug_assert!(next.is_none_or(|next| count < self[next].count));

        let head = self.allocate(|index| Node {
            prev: index,
            next: index,
            bucket: BucketIndex { head: index },
            table_hint: 0,
            kind: NodeKind::Bucket(Bucket { count, prev, next }),
        });

        let bucket = BucketIndex { head };

        self.link_buckets(prev, Some(bucket));
        self.link_buckets(Some(bucket), next);

        bucket
    }

    /// Removes `bucket`.
    /// Only valid once its items have all been removed or moved to another bucket.
    #[cold]
    #[inline(never)]
    fn remove_bucket(&mut self, bucket: BucketIndex) {
        let head = bucket.head;
        let Bucket { prev, next, .. } = self[bucket];

        // Freeing a head that still has items would orphan them.
        debug_assert!(self[head].next == head || self[self[head].next].bucket != bucket);

        self.link_buckets(prev, next);
        self.free(head);
    }

    /// Makes `next` follow `prev`, where `None` stands for the end of the list.
    fn link_buckets(&mut self, prev: Option<BucketIndex>, next: Option<BucketIndex>) {
        match prev {
            Some(prev) => {
                self[prev].next = next;
            }

            None => {
                self.first_bucket = next;
            }
        }

        if let Some(next) = next {
            self[next].prev = prev;
        }
    }

    /// Makes `node` the most recent item in `bucket`.
    /// Ignores `node`'s old links.
    #[inline]
    fn link_to_back(&mut self, bucket: BucketIndex, node: NodeIndex) {
        let tail = self[bucket.head].prev;

        // Only items move between lists.
        // A node still linked as the back would link to itself.
        debug_assert!(matches!(self[node].kind, NodeKind::Item(_)));
        debug_assert_ne!(node, tail);

        let linked = &mut self[node];
        linked.prev = tail;
        linked.next = bucket.head;
        linked.bucket = bucket;

        self[tail].next = node;
        self[bucket.head].prev = node;
    }

    /// Splices `node` out of its list, leaving its own links stale.
    /// Does not remove a bucket if it becomes empty.
    #[inline]
    fn unlink(&mut self, node: NodeIndex) {
        let Node { prev, next, .. } = self[node];

        // Stale links, from perhaps a double unlink, would rewire the wrong nodes.
        debug_assert_eq!(self[prev].next, node);
        debug_assert_eq!(self[next].prev, node);

        self[prev].next = next;
        self[next].prev = prev;
    }

    /// Unlinks `node` from its list and removes the bucket if it becomes empty.
    #[inline]
    fn detach(&mut self, node: NodeIndex) {
        let bucket = self[node].bucket;
        self.unlink(node);

        if self[bucket.head].next == bucket.head {
            self.remove_bucket(bucket);
        }
    }

    /// Allocates a [`Node`], reusing a freed node if available.
    /// `node` is given its own index so a bucket head can link to itself.
    #[inline]
    fn allocate(&mut self, node: impl FnOnce(NodeIndex) -> Node<T>) -> NodeIndex {
        match self.free_nodes.pop() {
            Some(index) => {
                self[index] = node(index);
                index
            }

            None => {
                let index = NodeIndex::new(self.nodes.len());
                self.nodes.push(node(index));
                index
            }
        }
    }

    /// Frees `node` for reuse by `allocate`, returning what it held.
    #[inline]
    fn free(&mut self, node: NodeIndex) -> NodeKind<T> {
        // A double free would hand the same node to two later allocations.
        debug_assert!(!matches!(self[node].kind, NodeKind::Vacant));

        self.free_nodes.push(node);
        mem::take(&mut self[node].kind)
    }
}

impl<T> Index<NodeIndex> for EvictionQueue<T> {
    type Output = Node<T>;

    #[inline]
    fn index(&self, index: NodeIndex) -> &Node<T> {
        &self.nodes[index.get()]
    }
}

impl<T> IndexMut<NodeIndex> for EvictionQueue<T> {
    #[inline]
    fn index_mut(&mut self, index: NodeIndex) -> &mut Node<T> {
        &mut self.nodes[index.get()]
    }
}

impl<T> Index<BucketIndex> for EvictionQueue<T> {
    type Output = Bucket;

    #[inline]
    fn index(&self, index: BucketIndex) -> &Bucket {
        match &self[index.head].kind {
            NodeKind::Bucket(bucket) => bucket,
            _ => unreachable!("BucketIndex should point to a bucket"),
        }
    }
}

impl<T> IndexMut<BucketIndex> for EvictionQueue<T> {
    #[inline]
    fn index_mut(&mut self, index: BucketIndex) -> &mut Bucket {
        match &mut self[index.head].kind {
            NodeKind::Bucket(bucket) => bucket,
            _ => unreachable!("BucketIndex should point to a bucket"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::iter::from_fn;

    use super::*;

    fn drain<T>(queue: &mut EvictionQueue<T>) -> Vec<T> {
        from_fn(|| queue.front().map(|node| queue.remove(node))).collect()
    }

    #[test]
    fn push_then_remove_round_trip() {
        let mut queue = EvictionQueue::default();
        let node = queue.push(1);

        assert_eq!(queue.front(), Some(node));
        assert_eq!(*queue.get(node), 1);
        assert_eq!(queue.remove(node), 1);
        assert_eq!(queue.front(), None);
    }

    #[test]
    fn evicts_in_push_order_without_promotes() {
        let mut queue = EvictionQueue::default();

        for item in 1..=3 {
            queue.push(item);
        }

        assert_eq!(drain(&mut queue), [1, 2, 3]);
    }

    // -------------------------------------------------------
    // Removal and reuse
    // -------------------------------------------------------

    #[test]
    fn remove_keeps_the_rest_in_order() {
        let mut queue = EvictionQueue::default();

        queue.push(1);
        let two = queue.push(2);
        queue.push(3);

        assert_eq!(queue.remove(two), 2);
        assert_eq!(drain(&mut queue), [1, 3]);
    }

    #[test]
    fn push_reuses_removed_nodes() {
        let mut queue = EvictionQueue::default();

        let node = queue.push(1);
        let len = queue.nodes.len();

        queue.remove(node);
        queue.push(2);

        assert_eq!(queue.nodes.len(), len);
    }

    #[test]
    fn push_after_clear_starts_fresh() {
        let mut queue = EvictionQueue::default();

        // Leave a live bucket and a freed node for `clear` to forget.
        let node = queue.push(1);
        queue.push(2);
        queue.remove(node);
        queue.clear();
        queue.push(3);

        assert_eq!(drain(&mut queue), [3]);
    }

    // -------------------------------------------------------
    // Promote
    // -------------------------------------------------------

    #[test]
    fn lru_promote_moves_the_item_to_the_back() {
        let mut queue = EvictionQueue::default();

        let one = queue.push(1);
        queue.push(2);
        queue.push(3);
        queue.promote(one);

        assert_eq!(drain(&mut queue), [2, 3, 1]);
    }

    #[test]
    fn lfu_evicts_the_least_used_first() {
        let mut queue = EvictionQueue::default();
        queue.configure(CachePolicy::LeastFrequentlyUsed);

        let one = queue.push(1);
        let two = queue.push(2);
        queue.push(3);

        for _ in 0..3 {
            queue.promote(one);
        }

        for _ in 0..2 {
            queue.promote(two);
        }

        assert_eq!(drain(&mut queue), [3, 2, 1]);
    }

    #[test]
    fn lfu_ties_evict_the_least_recent_first() {
        let mut queue = EvictionQueue::default();
        queue.configure(CachePolicy::LeastFrequentlyUsed);

        let one = queue.push(1);
        let two = queue.push(2);
        let three = queue.push(3);

        queue.promote(three);
        queue.promote(one);
        queue.promote(two);

        assert_eq!(drain(&mut queue), [3, 1, 2]);
    }

    #[test]
    fn lfu_promote_keeps_the_only_item_in_front() {
        let mut queue = EvictionQueue::default();
        queue.configure(CachePolicy::LeastFrequentlyUsed);

        let one = queue.push(1);
        queue.promote(one);

        assert_eq!(queue.front(), Some(one));
    }

    #[test]
    fn lfu_push_evicts_before_promoted_items() {
        let mut queue = EvictionQueue::default();
        queue.configure(CachePolicy::LeastFrequentlyUsed);

        let one = queue.push(1);
        queue.promote(one);
        queue.push(2);

        assert_eq!(drain(&mut queue), [2, 1]);
    }

    // -------------------------------------------------------
    // Configure
    // -------------------------------------------------------

    #[test]
    fn lfu_to_lru_keeps_the_order() {
        let mut queue = EvictionQueue::default();
        queue.configure(CachePolicy::LeastFrequentlyUsed);

        let one = queue.push(1);
        let two = queue.push(2);
        queue.push(3);

        queue.promote(one);
        queue.promote(two);
        queue.configure(CachePolicy::LeastRecentlyUsed);

        assert_eq!(drain(&mut queue), [3, 1, 2]);
    }

    #[test]
    fn lfu_to_lru_evicts_new_items_last() {
        let mut queue = EvictionQueue::default();
        queue.configure(CachePolicy::LeastFrequentlyUsed);

        let one = queue.push(1);
        let two = queue.push(2);
        let three = queue.push(3);

        for _ in 0..3 {
            queue.promote(one);
        }

        for _ in 0..2 {
            queue.promote(two);
        }

        queue.promote(three);
        queue.configure(CachePolicy::LeastRecentlyUsed);
        queue.push(4);

        assert_eq!(drain(&mut queue), [3, 2, 1, 4]);
    }
}
