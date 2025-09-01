package com.rolandb;

/**
 * This implements an AVL tree with support for querying elements at a given
 * index in the sorted list, and to quickly find their index.
 */
public class SortedStatisticsTree<T extends Comparable<T>> {
    /**
     * Internal node type. We store size for searching by index, and height for
     * AVL style balancing.
     */
    private static class Node<T> {
        T key;
        Node<T> left, right;
        int height, size;

        public Node(T key) {
            this.key = key;
            this.height = 1;
            this.size = 1;
        }
    }

    private Node<T> root;

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
     * Find the index of the given element.
     * 
     * @param key
     *            The element to search for.
     * @return The index of the element or {@code -1} if not in the collection.
     */
    public int indexOf(T key) {
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
        // Key was not found
        return -1;
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

    private int size(Node<T> n) {
        return n == null ? 0 : n.size;
    }

    private int height(Node<T> n) {
        return n == null ? 0 : n.height;
    }

    private void update(Node<T> n) {
        n.height = 1 + Math.max(height(n.left), height(n.right));
        n.size = 1 + size(n.left) + size(n.right);
    }

    private int balanceFactor(Node<T> n) {
        return height(n.left) - height(n.right);
    }

    private Node<T> rotateRight(Node<T> x) {
        Node<T> y = x.left;
        Node<T> z = y.right;
        y.right = x;
        x.left = z;
        update(x);
        update(y);
        return y;
    }

    private Node<T> rotateLeft(Node<T> x) {
        Node<T> y = x.right;
        Node<T> z = y.left;
        y.left = x;
        x.right = z;
        update(x);
        update(y);
        return y;
    }

    private Node<T> balance(Node<T> n) {
        if (n == null) {
            return null;
        } else {
            update(n);
            int bf = balanceFactor(n);
            if (bf > 1) {
                if (balanceFactor(n.left) < 0) {
                    n.left = rotateLeft(n.left);
                }
                return rotateRight(n);
            } else if (bf < -1) {
                if (balanceFactor(n.right) > 0) {
                    n.right = rotateRight(n.right);
                }
                return rotateLeft(n);
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
                // Ignore the duplicate.
                return node;
            }
            return balance(node);
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
                    return node.left != null ? node.left : node.right;
                } else {
                    // We need to find the minimum key on the right, and insert it into
                    // this node.
                    node.right = removeMin(node.right, node);
                    return node;
                }
            }
            return balance(node);
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
            return balance(node);
        }
    }
}
