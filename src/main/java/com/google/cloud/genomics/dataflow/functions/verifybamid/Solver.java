/*
 * Copyright 2015 Google.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.genomics.dataflow.functions.verifybamid;

import org.apache.commons.math3.analysis.UnivariateFunction;
import org.apache.commons.math3.geometry.euclidean.oned.Interval;
import org.apache.commons.math3.optim.MaxEval;
import org.apache.commons.math3.optim.MaxIter;
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;
import org.apache.commons.math3.optim.univariate.BrentOptimizer;
import org.apache.commons.math3.optim.univariate.SearchInterval;
import org.apache.commons.math3.optim.univariate.UnivariateObjectiveFunction;
import org.apache.commons.math3.optim.univariate.UnivariatePointValuePair;

/**
 * Maximize a univariate (likelihood) function.
 */
public class Solver {

  /**
   * Runs a grid search for the maximum value of a univariate function.
   * 
   * @param fn the likelihood function to minimize
   * @param start lower bound of the interval to search
   * @param end upper bound of the interval to search
   * @param step grid step size
   * @return an Interval bracketing the minimum
   */
  static Interval gridSearch(UnivariateFunction fn, double start,
      double end, double step) {
    double lowMax = start;  // lower bound on interval surrounding alphaMax
    double alphaMax = start - step;
    double likMax = 0.0;

    double lastAlpha = start;
    double alpha = start;
    while (alpha < end) {
      double likelihood = fn.value(alpha);
      if (alphaMax < start || likelihood > likMax) {
        lowMax = lastAlpha;
        alphaMax = alpha;
        likMax = likelihood;
      }
      lastAlpha = alpha;
      alpha += step;
    }
    // make sure we've checked the rightmost endpoint (won't happen if 
    // end - start is not an integer multiple of step, because of roundoff
    // errors, etc)
    double likelihood = fn.value(end);
    if (likelihood > likMax) {
      lowMax = lastAlpha;
      alphaMax = end;
      likMax = likelihood;
    }
    return new Interval(lowMax, Math.min(end, alphaMax + step));
  }

  /**
   * Maximizes a univariate function using a grid search followed by Brent's algorithm.
   * 
   * @param fn the likelihood function to minimize
   * @param gridStart the lower bound for the grid search
   * @param gridEnd the upper bound for the grid search
   * @param gridStep step size for the grid search
   * @param relErr relative error tolerance for Brent's algorithm
   * @param absErr absolute error tolerance for Brent's algorithm
   * @param maxIter maximum # of iterations to perform in Brent's algorithm
   * @param maxEval maximum # of Likelihood function evaluations in Brent's algorithm
   * 
   * @return the value of the parameter that maximizes the function
   */
  public static double maximize(UnivariateFunction fn, double gridStart, double gridEnd, 
      double gridStep, double relErr, double absErr, int maxIter, int maxEval) {
    Interval interval = gridSearch(fn, gridStart, gridEnd, gridStep);
    BrentOptimizer bo = new BrentOptimizer(relErr, absErr);
    UnivariatePointValuePair max = bo.optimize(
        new MaxIter(maxIter),
        new MaxEval(maxEval),
        new SearchInterval(interval.getInf(), interval.getSup()),
        new UnivariateObjectiveFunction(fn),
        GoalType.MAXIMIZE);
    return max.getPoint();
  }
}
