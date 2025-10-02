package com.rolandb;

import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import static org.junit.jupiter.api.Assertions.*;

public class OrderStatisticTreeTests {
    private OrderStatisticTree<Integer> createTree(Integer... elements) {
        OrderStatisticTree<Integer> tree = new OrderStatisticTree<>();
        for (Integer element : elements) {
            tree.add(element);
        }
        return tree;
    }

    @Test
    void testAddAndSize() {
        OrderStatisticTree<Integer> tree = new OrderStatisticTree<>();
        assertEquals(0, tree.size());

        tree.add(10);
        assertEquals(1, tree.size());

        tree.add(20);
        tree.add(5);
        assertEquals(3, tree.size());
    }

    @Test
    void testAddDuplicates() {
        OrderStatisticTree<Integer> tree = new OrderStatisticTree<>();
        tree.add(10);
        tree.add(20);
        tree.add(10);
        tree.add(5);
        assertEquals(3, tree.size());
    }

    @Test
    void testGetElements() {
        OrderStatisticTree<Integer> tree = createTree(30, 10, 40, 20);
        assertEquals(4, tree.size());
        assertEquals(10, tree.get(0));
        assertEquals(20, tree.get(1));
        assertEquals(30, tree.get(2));
        assertEquals(40, tree.get(3));
    }

    @Test
    void testGetEmptyTree() {
        OrderStatisticTree<Integer> tree = new OrderStatisticTree<>();
        assertNull(tree.get(0));
        assertNull(tree.get(5));
    }

    @Test
    void testGetInvalidIndex() {
        OrderStatisticTree<Integer> tree = createTree(1, 2, 3);
        assertNull(tree.get(-1));
        assertNull(tree.get(3));
    }

    @Test
    void testIndexOfExistingElements() {
        OrderStatisticTree<Integer> tree = createTree(30, 10, 40, 20);
        assertEquals(0, tree.indexOf(10));
        assertEquals(1, tree.indexOf(20));
        assertEquals(2, tree.indexOf(30));
        assertEquals(3, tree.indexOf(40));
    }

    @Test
    void testIndexOfNonExistingElements() {
        OrderStatisticTree<Integer> tree = createTree(10, 30, 50);
        assertEquals(-1, tree.indexOf(5));
        assertEquals(-2, tree.indexOf(20));
        assertEquals(-3, tree.indexOf(40));
        assertEquals(-4, tree.indexOf(60));
    }

    @Test
    void testIndexOfEmptyTree() {
        OrderStatisticTree<Integer> tree = new OrderStatisticTree<>();
        assertEquals(-1, tree.indexOf(100));
    }

    @Test
    void testRemoveLeafNode() {
        OrderStatisticTree<Integer> tree = createTree(20, 10, 30);
        tree.remove(10);
        assertEquals(2, tree.size());
        assertEquals(0, tree.indexOf(20));
        assertEquals(-1, tree.indexOf(10));
    }

    @Test
    void testRemoveNodeWithOneChild() {
        OrderStatisticTree<Integer> tree = createTree(20, 10, 15);
        tree.remove(10);
        assertEquals(2, tree.size());
        assertEquals(0, tree.indexOf(15));
        assertEquals(1, tree.indexOf(20));
        assertNull(tree.get(2));
    }

    @Test
    void testRemoveRootWithTwoChildren() {
        OrderStatisticTree<Integer> tree = createTree(30, 10, 50, 5, 20, 40, 60);
        tree.remove(30);
        assertEquals(6, tree.size());
        assertEquals(-4, tree.indexOf(30));
        assertEquals(40, tree.get(3));
        assertEquals(5, tree.get(0));
        assertEquals(60, tree.get(5));
    }

    @Test
    void testRemoveNonExistingElement() {
        OrderStatisticTree<Integer> tree = createTree(10, 20, 30);
        tree.remove(15);
        assertEquals(3, tree.size());
        assertEquals(0, tree.indexOf(10));
    }

    @Test
    void testRemoveLastElement() {
        OrderStatisticTree<Integer> tree = createTree(5);
        tree.remove(5);
        assertEquals(0, tree.size());
        assertEquals(-1, tree.indexOf(5));
        assertNull(tree.get(0));
    }

    @Test
    void testIndexIteratorFullTraversal() {
        OrderStatisticTree<Integer> tree = createTree(30, 10, 40, 20);
        List<Integer> expected = Arrays.asList(10, 20, 30, 40);
        List<Integer> actual = new java.util.ArrayList<>();
        Iterator<Integer> it = tree.indexIterator(0);
        while (it.hasNext()) {
            actual.add(it.next());
        }
        assertEquals(expected, actual);
    }

    @Test
    void testIndexIteratorPartialTraversal() {
        OrderStatisticTree<Integer> tree = createTree(30, 10, 40, 20, 50);
        List<Integer> expected = Arrays.asList(30, 40, 50);
        List<Integer> actual = new java.util.ArrayList<>();
        Iterator<Integer> it = tree.indexIterator(2);
        while (it.hasNext()) {
            actual.add(it.next());
        }
        assertEquals(expected, actual);
    }

    @Test
    void testIndexIteratorEmptyTree() {
        OrderStatisticTree<Integer> tree = new OrderStatisticTree<>();
        Iterator<Integer> it = tree.indexIterator(0);
        assertFalse(it.hasNext());
        assertThrows(NoSuchElementException.class, it::next);
    }

    @Test
    void testIndexIteratorStartOutOfBounds() {
        OrderStatisticTree<Integer> tree = createTree(1, 2, 3);
        Iterator<Integer> itHigh = tree.indexIterator(3);
        assertFalse(itHigh.hasNext());
        assertThrows(NoSuchElementException.class, itHigh::next);
    }

    @Test
    void testIndexIteratorSingleElement() {
        OrderStatisticTree<Integer> tree = createTree(100);
        Iterator<Integer> it = tree.indexIterator(0);
        assertTrue(it.hasNext());
        assertEquals(100, it.next());
        assertFalse(it.hasNext());
        assertThrows(NoSuchElementException.class, it::next);
    }

    @Test
    void testIteratorOnLargerRandomData() {
        OrderStatisticTree<Integer> tree = new OrderStatisticTree<>();
        List<Integer> data = Arrays.asList(50, 25, 75, 12, 37, 63, 87, 6, 18, 31, 43, 56, 69, 81, 93);
        for (int element : data) {
            tree.add(element);
        }
        List<Integer> expected = new java.util.ArrayList<>(data);
        expected.sort(Comparator.naturalOrder());
        List<Integer> actual = new java.util.ArrayList<>();
        Iterator<Integer> it = tree.indexIterator(0);
        while (it.hasNext()) {
            actual.add(it.next());
        }
        assertEquals(expected, actual);
    }
}
