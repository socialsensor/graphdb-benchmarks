package eu.socialsensor.utils;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class PermuteMethod implements Iterator<Method[]> {
  private final int size;
  private final Method[] elements; // copy of original 0 .. size-1
  private final Method[] ar; // array for output, 0 .. size-1
  private final int[] permutation; // perm of nums 1..size, perm[0]=0

  private boolean next = true;

  public PermuteMethod(Method[] e) {
    size = e.length;
    elements = new Method[size];
    System.arraycopy(e, 0, elements, 0, size);
    ar = new Method[size];
    System.arraycopy(e, 0, ar, 0, size);
    permutation = new int[size + 1];
    for (int i = 0; i < size + 1; i++) {
      permutation[i] = i;
    }
  }

  private void formNextPermutation() {
    for (int i = 0; i < size; i++) {
      Array.set(ar, i, elements[permutation[i + 1] - 1]);
    }
  }

  public boolean hasNext() {
    return next;
  }

  public void remove() throws UnsupportedOperationException {
    throw new UnsupportedOperationException();
  }

  private void swap(final int i, final int j) {
    final int x = permutation[i];
    permutation[i] = permutation[j];
    permutation[j] = x;
  }

  public Method[] next() throws NoSuchElementException {
    formNextPermutation(); // copy original elements
    int i = size - 1;
    while (permutation[i] > permutation[i + 1])
      i--;
    if (i == 0) {
      next = false;
      for (int j = 0; j < size + 1; j++) {
        permutation[j] = j;
      }
      return ar;
    }
    int j = size;
    while (permutation[i] > permutation[j])
      j--;
    swap(i, j);
    int r = size;
    int s = i + 1;
    while (r > s) {
      swap(r, s);
      r--;
      s++;
    }
    return ar;
  }
}
