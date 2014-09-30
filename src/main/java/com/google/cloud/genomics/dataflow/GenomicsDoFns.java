/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.genomics.dataflow;

import com.google.api.services.genomics.Genomics;
import com.google.api.services.genomics.GenomicsRequest;
import com.google.api.services.genomics.model.Beacon;
import com.google.api.services.genomics.model.CallSet;
import com.google.api.services.genomics.model.CoverageBucket;
import com.google.api.services.genomics.model.Dataset;
import com.google.api.services.genomics.model.ExportReadsetsRequest;
import com.google.api.services.genomics.model.ExportReadsetsResponse;
import com.google.api.services.genomics.model.ExportVariantsRequest;
import com.google.api.services.genomics.model.ExportVariantsResponse;
import com.google.api.services.genomics.model.ImportReadsetsRequest;
import com.google.api.services.genomics.model.ImportReadsetsResponse;
import com.google.api.services.genomics.model.ImportVariantsRequest;
import com.google.api.services.genomics.model.ImportVariantsResponse;
import com.google.api.services.genomics.model.Job;
import com.google.api.services.genomics.model.ListCoverageBucketsResponse;
import com.google.api.services.genomics.model.ListDatasetsResponse;
import com.google.api.services.genomics.model.Read;
import com.google.api.services.genomics.model.Readset;
import com.google.api.services.genomics.model.SearchCallSetsRequest;
import com.google.api.services.genomics.model.SearchCallSetsResponse;
import com.google.api.services.genomics.model.SearchJobsRequest;
import com.google.api.services.genomics.model.SearchJobsResponse;
import com.google.api.services.genomics.model.SearchReadsRequest;
import com.google.api.services.genomics.model.SearchReadsResponse;
import com.google.api.services.genomics.model.SearchReadsetsRequest;
import com.google.api.services.genomics.model.SearchReadsetsResponse;
import com.google.api.services.genomics.model.SearchVariantSetsRequest;
import com.google.api.services.genomics.model.SearchVariantSetsResponse;
import com.google.api.services.genomics.model.SearchVariantsRequest;
import com.google.api.services.genomics.model.SearchVariantsResponse;
import com.google.api.services.genomics.model.Variant;
import com.google.api.services.genomics.model.VariantSet;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.utils.GenomicsSupplier;
import com.google.cloud.genomics.utils.Paginator;
import com.google.cloud.genomics.utils.RetryPolicy;
import com.google.common.base.Optional;

import java.io.IOException;
import java.security.GeneralSecurityException;

public abstract class GenomicsDoFns<A, B, C extends GenomicsRequest<D>, D, E> {

  private abstract static class BaseGenomicsDoFns<A, B, C extends GenomicsRequest<D>, D>
      extends GenomicsDoFns<A, B, C, D, D> {

    static abstract class Beacons<B, C extends GenomicsRequest<D>, D>
        extends BaseGenomicsDoFns<Genomics.Beacons, B, C, D> {
      @Override final Genomics.Beacons getApi(Genomics genomics) {
        return genomics.beacons();
      }
    }

    static abstract class Callsets<B, C extends GenomicsRequest<D>, D>
        extends BaseGenomicsDoFns<Genomics.Callsets, B, C, D> {
      @Override final Genomics.Callsets getApi(Genomics genomics) {
        return genomics.callsets();
      }
    }

    static abstract class Datasets<B, C extends GenomicsRequest<D>, D>
        extends BaseGenomicsDoFns<Genomics.Datasets, B, C, D> {
      @Override final Genomics.Datasets getApi(Genomics genomics) {
        return genomics.datasets();
      }
    }

    static abstract class Jobs<B, C extends GenomicsRequest<D>, D>
        extends BaseGenomicsDoFns<Genomics.Jobs, B, C, D> {
      @Override final Genomics.Jobs getApi(Genomics genomics) {
        return genomics.jobs();
      }
    }

    static abstract class Readsets<B, C extends GenomicsRequest<D>, D>
        extends BaseGenomicsDoFns<Genomics.Readsets, B, C, D> {
      @Override final Genomics.Readsets getApi(Genomics genomics) {
        return genomics.readsets();
      }
    }

    static abstract class Variants<B, C extends GenomicsRequest<D>, D>
        extends BaseGenomicsDoFns<Genomics.Variants, B, C, D> {
      @Override final Genomics.Variants getApi(Genomics genomics) {
        return genomics.variants();
      }
    }

    static abstract class Variantsets<B, C extends GenomicsRequest<D>, D>
        extends BaseGenomicsDoFns<Genomics.Variantsets, B, C, D> {
      @Override final Genomics.Variantsets getApi(Genomics genomics) {
        return genomics.variantsets();
      }
    }

    @Override final DoFn<B, D> create(
        final GenomicsSupplier supplier,
        final Optional<String> fields,
        final RetryPolicy<C, D> retryPolicy) {
      return new DoFn<B, D>() {

            private A api;

            @Override public void processElement(ProcessContext context) throws IOException {
              for (RetryPolicy<C, D>.Instance instance = retryPolicy.createInstance(); true;) {
                C request = createRequest(api, context.element());
                if (fields.isPresent()) {
                  request.setFields(fields.get());
                }
                try {
                  context.output(request.execute());
                } catch (IOException e) {
                  if (!instance.shouldRetry(request, e)) {
                    throw e;
                  }
                }
              }
            }

            @Override
            public void startBatch(Context context) throws GeneralSecurityException, IOException {
              api = getApi(supplier.getGenomics());
            }
          };
    }

    abstract C createRequest(A api, B request) throws IOException;

    abstract A getApi(Genomics genomics);
  }

  public static final class Beacons {

    public static final BaseGenomicsDoFns.Beacons<String, Genomics.Beacons.Get, Beacon> GET =
        new BaseGenomicsDoFns.Beacons<String, Genomics.Beacons.Get, Beacon>() {
          @Override Genomics.Beacons.Get createRequest(
              Genomics.Beacons beacons, String variantSetId) throws IOException {
            return beacons.get(variantSetId);
          }
        };

    private Beacons() {}
  }

  public static class Callsets {

    public static final BaseGenomicsDoFns.Callsets<CallSet, Genomics.Callsets.Create, CallSet>
        CREATE = new BaseGenomicsDoFns.Callsets<CallSet, Genomics.Callsets.Create, CallSet>() {
          @Override
          Genomics.Callsets.Create createRequest(Genomics.Callsets callsets, CallSet callSet)
              throws IOException {
            return callsets.create(callSet);
          }
        };

    public static final BaseGenomicsDoFns.Callsets<String, Genomics.Callsets.Delete, Void> DELETE =
        new BaseGenomicsDoFns.Callsets<String, Genomics.Callsets.Delete, Void>() {
          @Override
          Genomics.Callsets.Delete createRequest(Genomics.Callsets callsets, String callSetId)
              throws IOException {
            return callsets.delete(callSetId);
          }
        };

    public static final BaseGenomicsDoFns.Callsets<String, Genomics.Callsets.Get, CallSet> GET =
        new BaseGenomicsDoFns.Callsets<String, Genomics.Callsets.Get, CallSet>() {
          @Override
          Genomics.Callsets.Get createRequest(Genomics.Callsets callsets, String callSetId)
              throws IOException {
            return callsets.get(callSetId);
          }
        };

    public static final
        BaseGenomicsDoFns.Callsets<KV<String, CallSet>, Genomics.Callsets.Patch, CallSet> PATCH =
          new BaseGenomicsDoFns.Callsets<KV<String, CallSet>, Genomics.Callsets.Patch, CallSet>() {
          @Override
          Genomics.Callsets.Patch createRequest(Genomics.Callsets callsets,
              KV<String, CallSet> keyValuePair) throws IOException {
            return callsets.patch(keyValuePair.getKey(), keyValuePair.getValue());
          }
        };

    public static final PaginatingGenomicsDoFns<Genomics.Callsets, SearchCallSetsRequest,
        Genomics.Callsets.Search, SearchCallSetsResponse, CallSet> SEARCH =
        PaginatingGenomicsDoFns.create(Paginator.CALLSETS);

    public static final
        BaseGenomicsDoFns.Callsets<KV<String, CallSet>, Genomics.Callsets.Update, CallSet> UPDATE =
          new BaseGenomicsDoFns.Callsets<KV<String, CallSet>, Genomics.Callsets.Update, CallSet>() {
          @Override
          Genomics.Callsets.Update createRequest(Genomics.Callsets callsets,
              KV<String, CallSet> keyValuePair) throws IOException {
            return callsets.update(keyValuePair.getKey(), keyValuePair.getValue());
          }
        };

    private Callsets() {}
  }

  public static class Datasets {

    public static final BaseGenomicsDoFns.Datasets<Dataset, Genomics.Datasets.Create, Dataset>
        CREATE = new BaseGenomicsDoFns.Datasets<Dataset, Genomics.Datasets.Create, Dataset>() {
          @Override
          Genomics.Datasets.Create createRequest(Genomics.Datasets datasets, Dataset dataset)
              throws IOException {
            return datasets.create(dataset);
          }
        };

    public static final BaseGenomicsDoFns.Datasets<String, Genomics.Datasets.Delete, Void> DELETE =
        new BaseGenomicsDoFns.Datasets<String, Genomics.Datasets.Delete, Void>() {
          @Override
          Genomics.Datasets.Delete createRequest(Genomics.Datasets datasets, String datasetId)
              throws IOException {
            return datasets.delete(datasetId);
          }
        };

    public static final BaseGenomicsDoFns.Datasets<String, Genomics.Datasets.Get, Dataset> GET =
        new BaseGenomicsDoFns.Datasets<String, Genomics.Datasets.Get, Dataset>() {
          @Override
          Genomics.Datasets.Get createRequest(Genomics.Datasets datasets, String datasetId)
              throws IOException {
            return datasets.get(datasetId);
          }
        };

    public static final PaginatingGenomicsDoFns<Genomics.Datasets, Long, Genomics.Datasets.List,
        ListDatasetsResponse, Dataset> LIST = PaginatingGenomicsDoFns.create(Paginator.DATASETS);

    public static final
        BaseGenomicsDoFns.Datasets<KV<String, Dataset>, Genomics.Datasets.Patch, Dataset> PATCH =
          new BaseGenomicsDoFns.Datasets<KV<String, Dataset>, Genomics.Datasets.Patch, Dataset>() {
          @Override
          Genomics.Datasets.Patch createRequest(Genomics.Datasets datasets,
              KV<String, Dataset> keyValuePair) throws IOException {
            return datasets.patch(keyValuePair.getKey(), keyValuePair.getValue());
          }
        };

    public static final BaseGenomicsDoFns.Datasets<String, Genomics.Datasets.Undelete, Dataset>
        UNDELETE = new BaseGenomicsDoFns.Datasets<String, Genomics.Datasets.Undelete, Dataset>() {
          @Override
          Genomics.Datasets.Undelete createRequest(Genomics.Datasets datasets, String datasetId)
              throws IOException {
            return datasets.undelete(datasetId);
          }
        };

    public static final
        BaseGenomicsDoFns.Datasets<KV<String, Dataset>, Genomics.Datasets.Update, Dataset> UPDATE =
          new BaseGenomicsDoFns.Datasets<KV<String, Dataset>, Genomics.Datasets.Update, Dataset>() {
          @Override
          Genomics.Datasets.Update createRequest(Genomics.Datasets datasets,
              KV<String, Dataset> keyValuePair) throws IOException {
            return datasets.update(keyValuePair.getKey(), keyValuePair.getValue());
          }
        };

    private Datasets() {}
  }

  public static class Jobs {

    public static final BaseGenomicsDoFns.Jobs<String, Genomics.Jobs.Cancel, Void> CANCEL =
        new BaseGenomicsDoFns.Jobs<String, Genomics.Jobs.Cancel, Void>() {
          @Override
          Genomics.Jobs.Cancel createRequest(Genomics.Jobs jobs, String jobId) throws IOException {
            return jobs.cancel(jobId);
          }
        };

    public static final BaseGenomicsDoFns.Jobs<String, Genomics.Jobs.Get, Job> GET =
        new BaseGenomicsDoFns.Jobs<String, Genomics.Jobs.Get, Job>() {
          @Override
          Genomics.Jobs.Get createRequest(Genomics.Jobs jobs, String jobId) throws IOException {
            return jobs.get(jobId);
          }
        };

    public static final PaginatingGenomicsDoFns<Genomics.Jobs, SearchJobsRequest,
        Genomics.Jobs.Search, SearchJobsResponse, Job> SEARCH =
        PaginatingGenomicsDoFns.create(Paginator.JOBS);

    private Jobs() {}
  }

  private static final class PaginatingGenomicsDoFns<A, B, C extends GenomicsRequest<D>, D, E>
      extends GenomicsDoFns<A, B, C, D, E> {

    static <A, B, C extends GenomicsRequest<D>, D, E> PaginatingGenomicsDoFns<A, B, C, D, E> create(
        Paginator.Factory<? extends Paginator<A, B, C, D, E>, C, D> paginatorFactory) {
      return new PaginatingGenomicsDoFns<A, B, C, D, E>(paginatorFactory);
    }

    private Paginator.Factory<? extends Paginator<A, B, C, D, E>, C, D> paginatorFactory;

    private PaginatingGenomicsDoFns(
        Paginator.Factory<? extends Paginator<A, B, C, D, E>, C, D> paginatorFactory) {
      this.paginatorFactory = paginatorFactory;
    }

    @Override
    final DoFn<B, E> create(final GenomicsSupplier supplier, final Optional<String> fields,
        final RetryPolicy<C, D> retryPolicy) {
      return new DoFn<B, E>() {

        private Paginator<A, B, C, D, E> paginator;

        @Override
        public void processElement(final ProcessContext context) throws IOException {
          paginator.search(context.element(), fields.orNull(), new Paginator.Callback<E, Void>() {
            @Override
            public Void consumeResponses(Iterable<E> responses) {
              for (E response : responses) {
                context.output(response);
              }
              return null;
            }
          });
        }

        @Override
        public void startBatch(Context context) throws GeneralSecurityException, IOException {
          paginator = paginatorFactory.createPaginator(supplier.getGenomics(), retryPolicy);
        }
      };
    }
  }

  public static class Reads {

    public static final PaginatingGenomicsDoFns<Genomics.Reads, SearchReadsRequest,
        Genomics.Reads.Search, SearchReadsResponse, Read> SEARCH =
        PaginatingGenomicsDoFns.create(Paginator.READS);

    private Reads() {}
  }

  public static class Readsets {

    public static class Coveragebuckets {

      public static final PaginatingGenomicsDoFns<Genomics.Readsets.Coveragebuckets, String,
          Genomics.Readsets.Coveragebuckets.List, ListCoverageBucketsResponse, CoverageBucket>
          LIST = PaginatingGenomicsDoFns.create(Paginator.READSETS.COVERAGEBUCKETS);

      private Coveragebuckets() {}
    }

    public static final BaseGenomicsDoFns.Readsets<String, Genomics.Readsets.Delete, Void> DELETE =
        new BaseGenomicsDoFns.Readsets<String, Genomics.Readsets.Delete, Void>() {
          @Override
          Genomics.Readsets.Delete createRequest(Genomics.Readsets readsets, String readsetId)
              throws IOException {
            return readsets.delete(readsetId);
          }
        };

    public static final BaseGenomicsDoFns.Readsets<ExportReadsetsRequest, Genomics.Readsets.Export,
        ExportReadsetsResponse> EXPORT = new BaseGenomicsDoFns.Readsets<ExportReadsetsRequest,
        Genomics.Readsets.Export, ExportReadsetsResponse>() {
      @Override
      Genomics.Readsets.Export createRequest(Genomics.Readsets readsets,
          ExportReadsetsRequest request) throws IOException {
        return readsets.export(request);
      }
    };

    public static final BaseGenomicsDoFns.Readsets<ImportReadsetsRequest,
        Genomics.Readsets.GenomicsImport,
        ImportReadsetsResponse> GENOMICSIMPORT = new BaseGenomicsDoFns.Readsets<
        ImportReadsetsRequest, Genomics.Readsets.GenomicsImport, ImportReadsetsResponse>() {
      @Override
      Genomics.Readsets.GenomicsImport createRequest(Genomics.Readsets readsets,
          ImportReadsetsRequest request) throws IOException {
        return readsets.genomicsImport(request);
      }
    };

    public static final BaseGenomicsDoFns.Readsets<String, Genomics.Readsets.Get, Readset> GET =
        new BaseGenomicsDoFns.Readsets<String, Genomics.Readsets.Get, Readset>() {
          @Override
          Genomics.Readsets.Get createRequest(Genomics.Readsets readsets, String callSetId)
              throws IOException {
            return readsets.get(callSetId);
          }
        };

    public static final
        BaseGenomicsDoFns.Readsets<KV<String, Readset>, Genomics.Readsets.Patch, Readset> PATCH =
          new BaseGenomicsDoFns.Readsets<KV<String, Readset>, Genomics.Readsets.Patch, Readset>() {
          @Override
          Genomics.Readsets.Patch createRequest(Genomics.Readsets readsets,
              KV<String, Readset> keyValuePair) throws IOException {
            return readsets.patch(keyValuePair.getKey(), keyValuePair.getValue());
          }
        };

    public static final PaginatingGenomicsDoFns<Genomics.Readsets, SearchReadsetsRequest,
        Genomics.Readsets.Search, SearchReadsetsResponse, Readset> SEARCH =
        PaginatingGenomicsDoFns.create(Paginator.READSETS);

    public static final
        BaseGenomicsDoFns.Readsets<KV<String, Readset>, Genomics.Readsets.Update, Readset> UPDATE =
          new BaseGenomicsDoFns.Readsets<KV<String, Readset>, Genomics.Readsets.Update, Readset>() {
          @Override
          Genomics.Readsets.Update createRequest(Genomics.Readsets readsets,
              KV<String, Readset> keyValuePair) throws IOException {
            return readsets.update(keyValuePair.getKey(), keyValuePair.getValue());
          }
        };

    private Readsets() {}
  }

  public static class Variants {

    public static final BaseGenomicsDoFns.Variants<Variant, Genomics.Variants.Create, Variant>
        CREATE = new BaseGenomicsDoFns.Variants<Variant, Genomics.Variants.Create, Variant>() {
          @Override
          Genomics.Variants.Create createRequest(Genomics.Variants variants, Variant variant)
              throws IOException {
            return variants.create(variant);
          }
        };

    public static final BaseGenomicsDoFns.Variants<String, Genomics.Variants.Delete, Void> DELETE =
        new BaseGenomicsDoFns.Variants<String, Genomics.Variants.Delete, Void>() {
          @Override
          Genomics.Variants.Delete createRequest(Genomics.Variants variants, String variantId)
              throws IOException {
            return variants.delete(variantId);
          }
        };

    public static final BaseGenomicsDoFns.Variants<ExportVariantsRequest, Genomics.Variants.Export,
        ExportVariantsResponse> EXPORT = new BaseGenomicsDoFns.Variants<ExportVariantsRequest,
        Genomics.Variants.Export, ExportVariantsResponse>() {
      @Override
      Genomics.Variants.Export createRequest(Genomics.Variants variants,
          ExportVariantsRequest request) throws IOException {
        return variants.export(request);
      }
    };

    public static final BaseGenomicsDoFns.Variants<ImportVariantsRequest,
        Genomics.Variants.GenomicsImport,
        ImportVariantsResponse> GENOMICSIMPORT = new BaseGenomicsDoFns.Variants<
        ImportVariantsRequest, Genomics.Variants.GenomicsImport, ImportVariantsResponse>() {
      @Override
      Genomics.Variants.GenomicsImport createRequest(Genomics.Variants variants,
          ImportVariantsRequest request) throws IOException {
        return variants.genomicsImport(request);
      }
    };

    public static final BaseGenomicsDoFns.Variants<String, Genomics.Variants.Get, Variant> GET =
        new BaseGenomicsDoFns.Variants<String, Genomics.Variants.Get, Variant>() {
          @Override
          Genomics.Variants.Get createRequest(Genomics.Variants variants, String variantId)
              throws IOException {
            return variants.get(variantId);
          }
        };

    public static final PaginatingGenomicsDoFns<Genomics.Variants, SearchVariantsRequest,
        Genomics.Variants.Search, SearchVariantsResponse, Variant> SEARCH =
        PaginatingGenomicsDoFns.create(Paginator.VARIANTS);

    public static final
        BaseGenomicsDoFns.Variants<KV<String, Variant>, Genomics.Variants.Update, Variant> UPDATE =
          new BaseGenomicsDoFns.Variants<KV<String, Variant>, Genomics.Variants.Update, Variant>() {
          @Override
          Genomics.Variants.Update createRequest(Genomics.Variants variants,
              KV<String, Variant> keyValuePair) throws IOException {
            return variants.update(keyValuePair.getKey(), keyValuePair.getValue());
          }
        };

    private Variants() {}
  }

  public static class Variantsets {

    public static final BaseGenomicsDoFns.Variantsets<String, Genomics.Variantsets.Delete, Void>
        DELETE = new BaseGenomicsDoFns.Variantsets<String, Genomics.Variantsets.Delete, Void>() {
          @Override
          Genomics.Variantsets.Delete createRequest(Genomics.Variantsets variantsets,
              String variantSetId) throws IOException {
            return variantsets.delete(variantSetId);
          }
        };

    public static final BaseGenomicsDoFns.Variantsets<String, Genomics.Variantsets.Get, VariantSet>
        GET = new BaseGenomicsDoFns.Variantsets<String, Genomics.Variantsets.Get, VariantSet>() {
          @Override
          Genomics.Variantsets.Get createRequest(Genomics.Variantsets variantsets,
              String variantSetId) throws IOException {
            return variantsets.get(variantSetId);
          }
        };

    public static final PaginatingGenomicsDoFns<Genomics.Variantsets, SearchVariantSetsRequest,
        Genomics.Variantsets.Search, SearchVariantSetsResponse, VariantSet> SEARCH =
        PaginatingGenomicsDoFns.create(Paginator.VARIANTSETS);

    private Variantsets() {}
  }

  public final DoFn<B, E> create(GenomicsSupplier supplier) {
    return create(supplier, Optional.<String>absent(), RetryPolicy.<C, D>defaultPolicy());
  }

  abstract DoFn<B, E> create(GenomicsSupplier supplier, Optional<String> fields,
      RetryPolicy<C, D> retryPolicy);

  public final DoFn<B, E> create(GenomicsSupplier supplier, RetryPolicy<C, D> retryPolicy) {
    return create(supplier, Optional.<String>absent(), retryPolicy);
  }

  public final DoFn<B, E> create(GenomicsSupplier supplier, String fields) {
    return create(supplier, Optional.of(fields), RetryPolicy.<C, D>defaultPolicy());
  }

  public final DoFn<B, E> create(GenomicsSupplier supplier, String fields,
      RetryPolicy<C, D> retryPolicy) {
    return create(supplier, Optional.of(fields), retryPolicy);
  }
}
