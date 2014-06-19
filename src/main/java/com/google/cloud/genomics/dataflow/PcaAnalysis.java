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
package com.google.cloud.genomics.dataflow;

import Jama.EigenvalueDecomposition;
import Jama.Matrix;
import com.google.api.client.util.Lists;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class PcaAnalysis implements
    SerializableFunction<Iterable<KV<KV<String, String>, Long>>, Iterable<PcaAnalysis.GraphResult>> {

  public static class GraphResult implements Serializable {
    public String name;
    public double graphX;
    public double graphY;

    public GraphResult(String name, double x, double y) {
      this.name = name;
      this.graphX = Math.floor(x * 100) / 100;
      this.graphY = Math.floor(y * 100) / 100;
    }
  }

  @Override
  public Iterable<GraphResult> apply(Iterable<KV<KV<String, String>, Long>> similarityData) {
    // Transform the data into a matrix
    BiMap<String, Integer> dataIndicies = HashBiMap.create();

    // TODO: Clean up this code
    for (KV<KV<String, String>, Long> entry : similarityData) {
      getCallsetIndex(dataIndicies, entry.getKey().getKey());
      getCallsetIndex(dataIndicies, entry.getKey().getValue());
    }

    int callsetCount = dataIndicies.size();
    double[][] matrixData = new double[callsetCount][callsetCount];
    for (KV<KV<String, String>, Long> entry : similarityData) {
      int d1 = getCallsetIndex(dataIndicies, entry.getKey().getKey());
      int d2 = getCallsetIndex(dataIndicies, entry.getKey().getValue());

      matrixData[d1][d2] = entry.getValue();
    }

    // Run the actual pca
    return getPcaData(matrixData, dataIndicies.inverse());
  }

  private int getCallsetIndex(Map<String, Integer> callsetIndicies, String callsetName) {
    if (!callsetIndicies.containsKey(callsetName)) {
      callsetIndicies.put(callsetName, callsetIndicies.size());
    }
    return callsetIndicies.get(callsetName);
  }

  // Convert the similarity matrix to an Eigen matrix.
  private List<GraphResult> getPcaData(double[][] data, BiMap<Integer, String> callsetNames) {
    int rows = data.length;
    int cols = data.length;

    // Center the similarity matrix.
    double matrixSum = 0;
    double[] rowSums = new double[rows];
    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < cols; j++) {
        matrixSum += data[i][j];
        rowSums[i] += data[i][j];
      }
    }
    double matrixMean = matrixSum / rows / cols;
    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < cols; j++) {
        double rowMean = rowSums[i] / rows;
        double colMean = rowSums[j] / rows;
        data[i][j] = data[i][j] - rowMean - colMean + matrixMean;
      }
    }


    // Determine the eigenvectors, and scale them so that their
    // sum of squares equals their associated eigenvalue.
    Matrix matrix = new Matrix(data);
    EigenvalueDecomposition eig = matrix.eig();
    Matrix eigenvectors = eig.getV();
    double[] realEigenvalues = eig.getRealEigenvalues();

    for (int j = 0; j < eigenvectors.getColumnDimension(); j++) {
      double sumSquares = 0;
      for (int i = 0; i < eigenvectors.getRowDimension(); i++) {
        sumSquares += eigenvectors.get(i, j) * eigenvectors.get(i, j);
      }
      for (int i = 0; i < eigenvectors.getRowDimension(); i++) {
        eigenvectors.set(i, j, eigenvectors.get(i,j) * Math.sqrt(realEigenvalues[j] / sumSquares));
      }
    }


    // Find the indices of the top two eigenvalues.
    int maxIndex = -1;
    int secondIndex = -1;
    double maxEigenvalue = 0;
    double secondEigenvalue = 0;

    for (int i = 0; i < realEigenvalues.length; i++) {
      double eigenvector = realEigenvalues[i];
      if (eigenvector > maxEigenvalue) {
        secondEigenvalue = maxEigenvalue;
        secondIndex = maxIndex;
        maxEigenvalue = eigenvector;
        maxIndex = i;
      } else if (eigenvector > secondEigenvalue) {
        secondEigenvalue = eigenvector;
        secondIndex = i;
      }
    }


    // Return projected data
    List<GraphResult> results = Lists.newArrayList();
    for (int i = 0; i < rows; i++) {
      results.add(new GraphResult(callsetNames.get(i),
          eigenvectors.get(i, maxIndex), eigenvectors.get(i, secondIndex)));
    }

    return results;
  }
}
