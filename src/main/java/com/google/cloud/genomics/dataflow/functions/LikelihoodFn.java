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
package com.google.cloud.genomics.dataflow.functions;

import com.google.api.services.genomics.model.Position;
import com.google.cloud.genomics.dataflow.model.ReadCounts;
import com.google.cloud.genomics.dataflow.model.ReadQualityCount;
import com.google.cloud.genomics.dataflow.model.ReadQualityCount.Base;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.math3.analysis.UnivariateFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * Implementation of the likelihood function in equation (2) in
 * G. Jun, M. Flickinger, K. N. Hetrick, Kurt, J. M. Romm, K. F. Doheny,
 * G. Abecasis, M. Boehnke,and H. M. Kang, Detecting and Estimating
 * Contamination of Human DNA Samples in Sequencing and Array-Based Genotype
 * Data, American journal of human genetics doi:10.1016/j.ajhg.2012.09.004
 * (volume 91 issue 5 pp.839 - 848)
 * http://www.sciencedirect.com/science/article/pii/S0002929712004788
 */
public class LikelihoodFn implements UnivariateFunction {

  /** Possible genotypes for a SNP with a single alternate */
  enum Genotype {
    REF_HOMOZYGOUS, HETEROZYGOUS, NONREF_HOMOZYGOUS
  }
  /** Possible error statuses for a base in a read */
  enum ReadStatus {
    CORRECT, ERROR
  }

  static int toTableIndex(Base observed, Genotype trueGenotype, ReadStatus status) {
    return observed.ordinal()
        + Base.values().length * (status.ordinal()
        + ReadStatus.values().length * trueGenotype.ordinal());
  }

  /*
   * P_OBS_GIVEN_TRUTH contains the probability of observing a particular
   * base (reference, non-reference, or other) given the true genotype
   * and the error status of the read.  See Table 1 of Jun et al.
   */
  private static final ImmutableList<Double> P_OBS_GIVEN_TRUTH;
  static {
    final ImmutableList<Double> pTable = ImmutableList.of(
    //          Observed base
    //    REF        NONREF     OTHER
          1.0,       0.0,       0.0,        // P(base | REF_HOMOZYGOUS, CORRECT)
          0.0,       1.0 / 3.0, 2.0 / 3.0,  // P(base | REF_HOMOZYGOUS, ERROR)
          0.5,       0.5,       0.0,        // P(base | HETEROZYGOUS, CORRECT)
          1.0 / 6.0, 1.0 / 6.0, 2.0 / 3.0,  // P(base | HETEROZYGOUS, ERROR)
          0.0,       1.0,       0.0,        // P(base | NONREF_HOMOZYGOUS, CORRECT)
          1.0 / 3.0, 0.0,       2.0 / 3.0); // P(base | NONREF_HOMOZYGOUS, ERROR)
    Iterator<Double> itProb = pTable.iterator();
    ArrayList<Double> pCond = new ArrayList<>();
    pCond.addAll(Collections.nCopies(
        Base.values().length * ReadStatus.values().length * Genotype.values().length, 0.0));
    for (
        Genotype g : ImmutableList.of(Genotype.REF_HOMOZYGOUS, Genotype.HETEROZYGOUS,
            Genotype.NONREF_HOMOZYGOUS)) {
      for (ReadStatus r : ImmutableList.of(ReadStatus.CORRECT, ReadStatus.ERROR)) {
        for (Base b : ImmutableList.of(Base.REF, Base.NONREF, Base.OTHER)) {
          pCond.set(toTableIndex(b, g, r), itProb.next());
        }
      }
    }
    P_OBS_GIVEN_TRUTH = ImmutableList.copyOf(pCond);
  }

  private final Map<Position, ReadCounts> readCounts;

  /**
   * Create a new LikelihoodFn instance for a given set of read counts.
   *
   * @param readCounts counts of reads by quality for each position of interest
   */
  public LikelihoodFn(Map<Position, ReadCounts> readCounts) {
    // copy the map so the counts don't get changed out from under us
    this.readCounts = ImmutableMap.copyOf(readCounts);
  }

  /**
   * Compute the probability of a genotype given the reference allele probability.
   */
  private static double pGenotype(Genotype g, double refProb) {
    switch(g) {
      case REF_HOMOZYGOUS:
        return refProb * refProb;
      case HETEROZYGOUS:
        return refProb * (1.0 - refProb);
      case NONREF_HOMOZYGOUS:
        return (1.0 - refProb) * (1.0 - refProb);
      default:
        throw new IllegalArgumentException("Illegal genotype");
    }
  }

  /**
   * Look up the probability of an observation conditioned on the underlying state.
   */
  private static double probObsGivenTruth(Base observed, Genotype trueGenotype,
      ReadStatus trueStatus) {
    return P_OBS_GIVEN_TRUTH.get(toTableIndex(observed, trueGenotype, trueStatus));
  }

  /**
   * Compute the likelihood of a contaminant fraction alpha.
   *
   * <p>See equation (2) in Jun et al.
   */
  @Override
  public double value(double alpha) {
    double logLikelihood = 0.0;
    for (ReadCounts rc : readCounts.values()) {
      double refProb = rc.getRefFreq();

      double pPosition = 0.0;
      for (Genotype trueGenotype1 : Genotype.values()) {
        double pGenotype1 = pGenotype(trueGenotype1, refProb);
        for (Genotype trueGenotype2 : Genotype.values()) {
          double pGenotype2 = pGenotype(trueGenotype2, refProb);

          double pObsGivenGenotype = 1.0;

          for (ReadQualityCount rqc : rc.getReadQualityCounts()) {
            Base base = rqc.getBase();
            double pErr = phredToProb(rqc.getQuality());
            double pObs
                = ((1.0 - alpha)
                    * probObsGivenTruth(base, trueGenotype1, ReadStatus.CORRECT)
                    + (alpha)
                    * probObsGivenTruth(base, trueGenotype2, ReadStatus.CORRECT)
                  ) * (1.0 - pErr)
                + ((1.0 - alpha)
                    * probObsGivenTruth(base, trueGenotype1, ReadStatus.ERROR)
                    + (alpha)
                    * probObsGivenTruth(base, trueGenotype2, ReadStatus.ERROR)
                  ) * pErr;
            pObsGivenGenotype *= Math.pow(pObs, rqc.getCount());
          }
          pPosition += pObsGivenGenotype * pGenotype1 * pGenotype2;
        }
      }
      logLikelihood += Math.log(pPosition);
    }
    return logLikelihood;
  }
  
  /**
   * Convert a Phred score to a probability.
   */
  private static double phredToProb(int phred) {
    return Math.pow(10.0, -(double) phred / 10.0);
  }
}
