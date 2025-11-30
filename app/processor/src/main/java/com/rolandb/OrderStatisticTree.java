package com.rolandb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * This implements an order statistic tree with support for querying elements at
 * a given index in the sorted list, and to quickly find their index.
 * 
 * @param <T>
 *            The type of object we want to store in the tree.
 */
public class OrderStatisticTree<T extends Comparable<T>> {
    /**
     * An iterator for the ordered statistics tree. The start and end are determined
     * when creating the iterator. The end is moved until it hits the end.
     * 
     * @param <T>
     *            The type of object the tree we are iterating over contains.
     */
    public static class TreeIterator<T extends Comparable<T>> implements Iterator<T> {
        private List<Node<T>> path;
        private final Node<T> end;

        private TreeIterator(List<Node<T>> path, Node<T> end) {
            this.path = path;
            this.end = end;
        }

        private Node<T> start() {
            if (path.isEmpty()) {
                return null;
            } else {
                return path.get(path.size() - 1);
            }
        }

        @Override
        public boolean hasNext() {
            return !path.isEmpty() && start() != end;
        }

        @Override
        public T next() {
            Node<T> start = start();
            if (start == null || start == end) {
                throw new NoSuchElementException("Iterator is empty");
            }
            T value = start.key;
            if (start.right != null) {
                // We can go right and then all the way to the left.
                start = start.right;
                path.add(start);
                while (start.left != null) {
                    start = start.left;
                    path.add(start);
                }
            } else {
                // Can't go right anymore, so we go up.
                path.remove(path.size() - 1);
                while (!path.isEmpty() && start().right == start) {
                    // We just visited the right side, must continue.
                    start = start();
                    path.remove(path.size() - 1);
                }
            }
            return value;
        }
    }

    /**
     * Internal node type. We store size for searching by index, and height for
     * AVL style balancing.
     */
    private static class Node<T> {
        T key;
        Node<T> left, right;
        int size;

        public Node(T key) {
            this.key = key;
            this.size = 1;
        }
    }

    private Node<T> root;

    /**
     * Create an empty tree.
     */
    public OrderStatisticTree() {
        root = null;
    }

    /**
     * Add an element to the collection. Note that duplicates will be ignored.
     *
     * @param key
     *            The element to add.
     */
    public void add(T key) {
        root = add(root, key);
    }

    /**
     * Delete an element from the collection-
     *
     * @param key
     *            The element to delete.
     */
    public void remove(T key) {
        root = remove(root, key);
    }

    /**
     * Find the index of the given element. Like {@code binarySearch}, this will
     * return a negative value of {@code -insertionIndex - 1} if the key does not
     * currently exist in the collection.
     *
     * @param <N>
     *            The type of key to search with. Must be comparable to the element
     *            type of the collection.
     * @param key
     *            The element to search for.
     * @return The index of the element or {@code -index - 1} if not found.
     */
    public <N extends Comparable<T>> int indexOf(N key) {
        Node<T> node = root;
        int index = 0;
        while (node != null) {
            int cmp = key.compareTo(node.key);
            if (cmp < 0) {
                node = node.left;
            } else if (cmp > 0) {
                index += 1 + size(node.left);
                node = node.right;
            } else {
                return index + size(node.left);
            }
        }
        // Key was not found.
        return -index - 1;
    }

    /**
     * Get the element at the given index.
     *
     * @param index
     *            The index to query.
     * @return The element at that index.
     */
    public T get(int index) {
        Node<T> node = root;
        while (node != null) {
            int leftSize = size(node.left);
            if (index < leftSize) {
                node = node.left;
            } else if (index > leftSize) {
                index -= leftSize + 1;
                node = node.right;
            } else {
                return node.key;
            }
        }
        // Key was not found
        return null;
    }

    /**
     * Get the size of the collection.
     *
     * @return Size of the collection.
     */
    public int size() {
        return size(root);
    }

    /**
     * Return an iterator that will iterator through all of the elements in sorted
     * order, from smallest to largest. The iterator starts at the given index.
     *
     * @param index
     *            The index to start at.
     * @return The new iterator.
     */
    public TreeIterator<T> indexIterator(int index) {
        List<Node<T>> path = new ArrayList<>();
        Node<T> node = root;
        while (node != null) {
            path.add(node);
            int leftSize = size(node.left);
            if (index < leftSize) {
                node = node.left;
            } else if (index > leftSize) {
                index -= leftSize + 1;
                node = node.right;
            } else {
                return new TreeIterator<>(path, null);
            }
        }
        path.clear();
        return new TreeIterator<>(path, null);
    }

    private int size(Node<T> n) {
        return n == null ? 0 : n.size;
    }

    private Node<T> rotateRight(Node<T> x) {
        Node<T> y = x.left;
        Node<T> z = y.right;
        y.right = x;
        x.left = z;
        x.size = 1 + size(z) + size(x.right);
        y.size = 1 + size(y.left) + x.size;
        return y;
    }

    private Node<T> rotateLeft(Node<T> x) {
        Node<T> y = x.right;
        Node<T> z = y.left;
        y.left = x;
        x.right = z;
        x.size = 1 + size(x.left) + size(z);
        y.size = 1 + x.size + size(y.right);
        return y;
    }

    private Node<T> updateAndBalance(Node<T> n) {
        if (n == null) {
            return null;
        } else {
            int leftSize = size(n.left);
            int rightSize = size(n.right);
            n.size = 1 + leftSize + rightSize;
            if (leftSize < n.size / 4 || rightSize < n.size / 4) {
                // The node needs rebalancing
                if (leftSize > rightSize) {
                    if (size(n.left.left) < size(n.left.right)) {
                        n.left = rotateLeft(n.left);
                    }
                    return rotateRight(n);
                } else {
                    if (size(n.right.left) > size(n.right.right)) {
                        n.right = rotateRight(n.right);
                    }
                    return rotateLeft(n);
                }
            } else {
                return n;
            }
        }
    }

    private Node<T> add(Node<T> node, T key) {
        if (node == null) {
            return new Node<>(key);
        } else {
            int cmp = key.compareTo(node.key);
            if (cmp < 0) {
                node.left = add(node.left, key);
            } else if (cmp > 0) {
                node.right = add(node.right, key);
            } else {
                // Ignore the duplicate. No change to the node, so no need to
                // do any rebalancing.
                return node;
            }
            return updateAndBalance(node);
        }
    }

    private Node<T> remove(Node<T> node, T key) {
        if (node == null) {
            // We didn't find the value.
            return null;
        } else {
            int cmp = key.compareTo(node.key);
            if (cmp < 0) {
                node.left = remove(node.left, key);
            } else if (cmp > 0) {
                node.right = remove(node.right, key);
            } else {
                // This is the node to remove.
                if (node.left == null || node.right == null) {
                    // We have only one (or no) child. We can just replace ourself.
                    // It will already be updated and balanced.
                    return node.left != null ? node.left : node.right;
                } else {
                    // We need to find the minimum key on the right, and insert it into
                    // this node.
                    node.right = removeMin(node.right, node);
                }
            }
            return updateAndBalance(node);
        }
    }

    private Node<T> removeMin(Node<T> node, Node<T> into) {
        if (node.left == null) {
            // We reached the minimum value.
            into.key = node.key;
            // We can just replace ourself with the subtree on the right.
            return node.right;
        } else {
            // We must remove from the left, and then rebalance.
            node.left = removeMin(node.left, into);
            return updateAndBalance(node);
        }
    }
}
