/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.utils;

import java.util.Iterator;
import java.util.List;
import java.util.RandomAccess;

import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterators;
import com.google.common.collect.Range;

public class PairGenerator {

  @VisibleForTesting
  public static <X, L extends List<? extends X> & RandomAccess> FluentIterable<KV<X, X>> allPairs(
      final L list) {
    return FluentIterable.from(
        ContiguousSet.create(Range.closedOpen(0, list.size()), DiscreteDomain.integers()))
        .transformAndConcat(new Function<Integer, Iterable<KV<X, X>>>() {
          @Override
          public Iterable<KV<X, X>> apply(final Integer i) {
            return new Iterable<KV<X, X>>() {
              @Override
              public Iterator<KV<X, X>> iterator() {
                return Iterators.transform(list.listIterator(i), new Function<X, KV<X, X>>() {

                  private final X key = list.get(i);

                  @Override
                  public KV<X, X> apply(X value) {
                    return KV.of(key, value);
                  }
                });
              }
            };
          }
        });
  }

}
