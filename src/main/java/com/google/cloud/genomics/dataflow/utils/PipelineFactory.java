package com.google.cloud.genomics.dataflow.utils;

import com.google.api.client.auth.oauth2.TokenErrorResponse;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.auth.openidconnect.IdToken;
import com.google.api.client.auth.openidconnect.IdTokenResponse;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken;
import com.google.api.client.googleapis.auth.oauth2.GoogleTokenResponse;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonErrorContainer;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.webtoken.JsonWebSignature;
import com.google.api.client.json.webtoken.JsonWebToken;
import com.google.api.services.bigquery.model.DatasetList;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationExtract;
import com.google.api.services.bigquery.model.JobConfigurationLink;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobConfigurationTableCopy;
import com.google.api.services.bigquery.model.JobList;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.JobStatistics2;
import com.google.api.services.bigquery.model.JobStatistics3;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.api.services.bigquery.model.JsonObject;
import com.google.api.services.bigquery.model.ProjectList;
import com.google.api.services.bigquery.model.ProjectReference;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableDataList;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableList;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.ViewDefinition;
import com.google.api.services.compute.model.AccessConfig;
import com.google.api.services.compute.model.Address;
import com.google.api.services.compute.model.AddressAggregatedList;
import com.google.api.services.compute.model.AddressList;
import com.google.api.services.compute.model.AddressesScopedList;
import com.google.api.services.compute.model.AttachedDisk;
import com.google.api.services.compute.model.AttachedDiskInitializeParams;
import com.google.api.services.compute.model.Backend;
import com.google.api.services.compute.model.BackendService;
import com.google.api.services.compute.model.BackendServiceGroupHealth;
import com.google.api.services.compute.model.BackendServiceList;
import com.google.api.services.compute.model.DeprecationStatus;
import com.google.api.services.compute.model.DiskAggregatedList;
import com.google.api.services.compute.model.DiskList;
import com.google.api.services.compute.model.DiskType;
import com.google.api.services.compute.model.DiskTypeAggregatedList;
import com.google.api.services.compute.model.DiskTypeList;
import com.google.api.services.compute.model.DiskTypesScopedList;
import com.google.api.services.compute.model.DisksScopedList;
import com.google.api.services.compute.model.Firewall;
import com.google.api.services.compute.model.FirewallList;
import com.google.api.services.compute.model.ForwardingRule;
import com.google.api.services.compute.model.ForwardingRuleAggregatedList;
import com.google.api.services.compute.model.ForwardingRuleList;
import com.google.api.services.compute.model.ForwardingRulesScopedList;
import com.google.api.services.compute.model.HealthCheckReference;
import com.google.api.services.compute.model.HealthStatus;
import com.google.api.services.compute.model.HostRule;
import com.google.api.services.compute.model.HttpHealthCheck;
import com.google.api.services.compute.model.HttpHealthCheckList;
import com.google.api.services.compute.model.Image;
import com.google.api.services.compute.model.ImageList;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.InstanceAggregatedList;
import com.google.api.services.compute.model.InstanceList;
import com.google.api.services.compute.model.InstanceReference;
import com.google.api.services.compute.model.InstancesScopedList;
import com.google.api.services.compute.model.License;
import com.google.api.services.compute.model.MachineType;
import com.google.api.services.compute.model.MachineTypeAggregatedList;
import com.google.api.services.compute.model.MachineTypeList;
import com.google.api.services.compute.model.MachineTypesScopedList;
import com.google.api.services.compute.model.Network;
import com.google.api.services.compute.model.NetworkInterface;
import com.google.api.services.compute.model.NetworkList;
import com.google.api.services.compute.model.Operation;
import com.google.api.services.compute.model.OperationAggregatedList;
import com.google.api.services.compute.model.OperationList;
import com.google.api.services.compute.model.OperationsScopedList;
import com.google.api.services.compute.model.PathMatcher;
import com.google.api.services.compute.model.PathRule;
import com.google.api.services.compute.model.Project;
import com.google.api.services.compute.model.Quota;
import com.google.api.services.compute.model.Region;
import com.google.api.services.compute.model.RegionList;
import com.google.api.services.compute.model.ResourceGroupReference;
import com.google.api.services.compute.model.Route;
import com.google.api.services.compute.model.RouteList;
import com.google.api.services.compute.model.Scheduling;
import com.google.api.services.compute.model.SerialPortOutput;
import com.google.api.services.compute.model.ServiceAccount;
import com.google.api.services.compute.model.Snapshot;
import com.google.api.services.compute.model.SnapshotList;
import com.google.api.services.compute.model.Tags;
import com.google.api.services.compute.model.TargetHttpProxy;
import com.google.api.services.compute.model.TargetHttpProxyList;
import com.google.api.services.compute.model.TargetInstance;
import com.google.api.services.compute.model.TargetInstanceAggregatedList;
import com.google.api.services.compute.model.TargetInstanceList;
import com.google.api.services.compute.model.TargetInstancesScopedList;
import com.google.api.services.compute.model.TargetPool;
import com.google.api.services.compute.model.TargetPoolAggregatedList;
import com.google.api.services.compute.model.TargetPoolInstanceHealth;
import com.google.api.services.compute.model.TargetPoolList;
import com.google.api.services.compute.model.TargetPoolsAddHealthCheckRequest;
import com.google.api.services.compute.model.TargetPoolsAddInstanceRequest;
import com.google.api.services.compute.model.TargetPoolsRemoveHealthCheckRequest;
import com.google.api.services.compute.model.TargetPoolsRemoveInstanceRequest;
import com.google.api.services.compute.model.TargetPoolsScopedList;
import com.google.api.services.compute.model.TargetReference;
import com.google.api.services.compute.model.TestFailure;
import com.google.api.services.compute.model.UrlMap;
import com.google.api.services.compute.model.UrlMapList;
import com.google.api.services.compute.model.UrlMapReference;
import com.google.api.services.compute.model.UrlMapTest;
import com.google.api.services.compute.model.UrlMapValidationResult;
import com.google.api.services.compute.model.UrlMapsValidateRequest;
import com.google.api.services.compute.model.UrlMapsValidateResponse;
import com.google.api.services.compute.model.UsageExportLocation;
import com.google.api.services.compute.model.Zone;
import com.google.api.services.compute.model.ZoneList;
import com.google.api.services.dataflow.model.ApproximatePosition;
import com.google.api.services.dataflow.model.CloudCounter;
import com.google.api.services.dataflow.model.CloudEncoding;
import com.google.api.services.dataflow.model.CloudFlattenInstruction;
import com.google.api.services.dataflow.model.CloudInstructionInput;
import com.google.api.services.dataflow.model.CloudInstructionOutput;
import com.google.api.services.dataflow.model.CloudLocation;
import com.google.api.services.dataflow.model.CloudMapTask;
import com.google.api.services.dataflow.model.CloudMetric;
import com.google.api.services.dataflow.model.CloudMultiOutputInfo;
import com.google.api.services.dataflow.model.CloudNamedParameter;
import com.google.api.services.dataflow.model.CloudParDoInstruction;
import com.google.api.services.dataflow.model.CloudParallelInstruction;
import com.google.api.services.dataflow.model.CloudPartialGroupByKeyInstruction;
import com.google.api.services.dataflow.model.CloudPosition;
import com.google.api.services.dataflow.model.CloudReadInstruction;
import com.google.api.services.dataflow.model.CloudSeqMapTask;
import com.google.api.services.dataflow.model.CloudSeqMapTaskOutputInfo;
import com.google.api.services.dataflow.model.CloudShellTask;
import com.google.api.services.dataflow.model.CloudSideInputInfo;
import com.google.api.services.dataflow.model.CloudSink;
import com.google.api.services.dataflow.model.CloudSource;
import com.google.api.services.dataflow.model.CloudStackTrace;
import com.google.api.services.dataflow.model.CloudWindmillSetupTask;
import com.google.api.services.dataflow.model.CloudWork;
import com.google.api.services.dataflow.model.CloudWorkerError;
import com.google.api.services.dataflow.model.CloudWorkerObject;
import com.google.api.services.dataflow.model.CloudWorkflow;
import com.google.api.services.dataflow.model.CloudWorkflowEnvironment;
import com.google.api.services.dataflow.model.CloudWorkflowPackage;
import com.google.api.services.dataflow.model.CloudWorkflowSpecification;
import com.google.api.services.dataflow.model.CloudWorkflowStep;
import com.google.api.services.dataflow.model.CloudWorkflowStepDefinition;
import com.google.api.services.dataflow.model.CloudWorkflowWorkerPool;
import com.google.api.services.dataflow.model.CloudWriteInstruction;
import com.google.api.services.dataflow.model.ComputationTopology;
import com.google.api.services.dataflow.model.Element;
import com.google.api.services.dataflow.model.GCSLocation;
import com.google.api.services.dataflow.model.InputDefinition;
import com.google.api.services.dataflow.model.JobMessage;
import com.google.api.services.dataflow.model.KeyRangeLocation;
import com.google.api.services.dataflow.model.ListJobMessagesResponse;
import com.google.api.services.dataflow.model.Location;
import com.google.api.services.dataflow.model.ModifyWorkflowRequest;
import com.google.api.services.dataflow.model.OutputDefinition;
import com.google.api.services.dataflow.model.PortReference;
import com.google.api.services.dataflow.model.PubsubLocation;
import com.google.api.services.dataflow.model.RequestWorkRequest;
import com.google.api.services.dataflow.model.TaskRunnerConfig;
import com.google.api.services.dataflow.model.TopologyConfig;
import com.google.api.services.dataflow.model.VMMetadataItem;
import com.google.api.services.dataflow.model.WaitPortDefinition;
import com.google.api.services.dataflow.model.WindmillLocation;
import com.google.api.services.dataflow.model.WorkerFlags;
import com.google.api.services.dataflow.model.WorkflowJob;
import com.google.api.services.dataflow.model.WorkflowJobStatus;
import com.google.api.services.genomics.model.Beacon;
import com.google.api.services.genomics.model.Call;
import com.google.api.services.genomics.model.CallSet;
import com.google.api.services.genomics.model.CoverageBucket;
import com.google.api.services.genomics.model.ExperimentalCreateJobRequest;
import com.google.api.services.genomics.model.ExperimentalCreateJobResponse;
import com.google.api.services.genomics.model.ExportReadsetsRequest;
import com.google.api.services.genomics.model.ExportReadsetsResponse;
import com.google.api.services.genomics.model.ExportVariantsRequest;
import com.google.api.services.genomics.model.ExportVariantsResponse;
import com.google.api.services.genomics.model.GenomicRange;
import com.google.api.services.genomics.model.Header;
import com.google.api.services.genomics.model.HeaderSection;
import com.google.api.services.genomics.model.ImportReadsetsRequest;
import com.google.api.services.genomics.model.ImportReadsetsResponse;
import com.google.api.services.genomics.model.ImportVariantsRequest;
import com.google.api.services.genomics.model.ImportVariantsResponse;
import com.google.api.services.genomics.model.JobRequest;
import com.google.api.services.genomics.model.ListCoverageBucketsResponse;
import com.google.api.services.genomics.model.ListDatasetsResponse;
import com.google.api.services.genomics.model.Program;
import com.google.api.services.genomics.model.Read;
import com.google.api.services.genomics.model.ReadGroup;
import com.google.api.services.genomics.model.Readset;
import com.google.api.services.genomics.model.ReferenceBound;
import com.google.api.services.genomics.model.ReferenceSequence;
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
import com.google.api.services.pubsub.model.AcknowledgeRequest;
import com.google.api.services.pubsub.model.Label;
import com.google.api.services.pubsub.model.ListSubscriptionsResponse;
import com.google.api.services.pubsub.model.ListTopicsResponse;
import com.google.api.services.pubsub.model.ModifyAckDeadlineRequest;
import com.google.api.services.pubsub.model.ModifyPushConfigRequest;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PubsubEvent;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.PullRequest;
import com.google.api.services.pubsub.model.PullResponse;
import com.google.api.services.pubsub.model.PushConfig;
import com.google.api.services.pubsub.model.Subscription;
import com.google.api.services.pubsub.model.Topic;
import com.google.api.services.shuffle.model.InputBatch;
import com.google.api.services.shuffle.model.ListEntriesResponse;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.BucketAccessControl;
import com.google.api.services.storage.model.BucketAccessControls;
import com.google.api.services.storage.model.Buckets;
import com.google.api.services.storage.model.Channel;
import com.google.api.services.storage.model.ComposeRequest;
import com.google.api.services.storage.model.ObjectAccessControl;
import com.google.api.services.storage.model.ObjectAccessControls;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;

import java.util.Arrays;
import java.util.List;

public class PipelineFactory {

  public static Pipeline create() {
    Pipeline pipeline = Pipeline.create();
    CoderRegistry registry = pipeline.getCoderRegistry();
    for (final Class<? extends GenericJson> type : Arrays.asList(
        AccessConfig.class,
        AcknowledgeRequest.class,
        Address.class,
        AddressAggregatedList.class,
        AddressList.class,
        AddressesScopedList.class,
        AddressesScopedList.Warning.class,
        AddressesScopedList.Warning.Data.class,
        ApproximatePosition.class,
        AttachedDisk.class,
        AttachedDiskInitializeParams.class,
        Backend.class,
        BackendService.class,
        BackendServiceGroupHealth.class,
        BackendServiceList.class,
        Beacon.class,
        Bucket.class,
        Bucket.Cors.class,
        Bucket.Lifecycle.class,
        Bucket.Lifecycle.Rule.class,
        Bucket.Lifecycle.Rule.Action.class,
        Bucket.Lifecycle.Rule.Condition.class,
        Bucket.Logging.class,
        Bucket.Owner.class,
        Bucket.Versioning.class,
        Bucket.Website.class,
        BucketAccessControl.class,
        BucketAccessControl.ProjectTeam.class,
        BucketAccessControls.class,
        Buckets.class,
        Call.class,
        CallSet.class,
        Channel.class,
        CloudCounter.class,
        CloudEncoding.class,
        CloudFlattenInstruction.class,
        CloudInstructionInput.class,
        CloudInstructionOutput.class,
        CloudLocation.class,
        CloudMapTask.class,
        CloudMetric.class,
        CloudMultiOutputInfo.class,
        CloudNamedParameter.class,
        CloudParDoInstruction.class,
        CloudParallelInstruction.class,
        CloudPartialGroupByKeyInstruction.class,
        CloudPosition.class,
        CloudReadInstruction.class,
        CloudSeqMapTask.class,
        CloudSeqMapTaskOutputInfo.class,
        CloudShellTask.class,
        CloudSideInputInfo.class,
        CloudSink.class,
        CloudSource.class,
        CloudStackTrace.class,
        CloudWindmillSetupTask.class,
        CloudWork.class,
        CloudWorkerError.class,
        CloudWorkerObject.class,
        CloudWorkflow.class,
        CloudWorkflowEnvironment.class,
        CloudWorkflowPackage.class,
        CloudWorkflowSpecification.class,
        CloudWorkflowStep.class,
        CloudWorkflowStepDefinition.class,
        CloudWorkflowWorkerPool.class,
        CloudWriteInstruction.class,
        ComposeRequest.class,
        ComposeRequest.SourceObjects.class,
        ComposeRequest.SourceObjects.ObjectPreconditions.class,
        ComputationTopology.class,
        CoverageBucket.class,
        DatasetList.class,
        DatasetList.Datasets.class,
        DatasetReference.class,
        DeprecationStatus.class,
        DiskAggregatedList.class,
        DiskList.class,
        DiskType.class,
        DiskTypeAggregatedList.class,
        DiskTypeList.class,
        DiskTypesScopedList.class,
        DiskTypesScopedList.Warning.class,
        DiskTypesScopedList.Warning.Data.class,
        DisksScopedList.class,
        DisksScopedList.Warning.class,
        DisksScopedList.Warning.Data.class,
        Element.class,
        ErrorProto.class,
        ExperimentalCreateJobRequest.class,
        ExperimentalCreateJobResponse.class,
        ExportReadsetsRequest.class,
        ExportReadsetsResponse.class,
        ExportVariantsRequest.class,
        ExportVariantsResponse.class,
        Firewall.class,
        Firewall.Allowed.class,
        FirewallList.class,
        ForwardingRule.class,
        ForwardingRuleAggregatedList.class,
        ForwardingRuleList.class,
        ForwardingRulesScopedList.class,
        ForwardingRulesScopedList.Warning.class,
        ForwardingRulesScopedList.Warning.Data.class,
        GCSLocation.class,
        GenomicRange.class,
        GetQueryResultsResponse.class,
        GoogleClientSecrets.class,
        GoogleClientSecrets.Details.class,
        GoogleIdToken.Payload.class,
        GoogleJsonError.class,
        GoogleJsonError.ErrorInfo.class,
        GoogleJsonErrorContainer.class,
        GoogleTokenResponse.class,
        Header.class,
        HeaderSection.class,
        HealthCheckReference.class,
        HealthStatus.class,
        HostRule.class,
        HttpHealthCheck.class,
        HttpHealthCheckList.class,
        IdToken.Payload.class,
        IdTokenResponse.class,
        Image.class,
        Image.RawDisk.class,
        ImageList.class,
        ImportReadsetsRequest.class,
        ImportReadsetsResponse.class,
        ImportVariantsRequest.class,
        ImportVariantsResponse.class,
        InputBatch.class,
        InputDefinition.class,
        Instance.class,
        InstanceAggregatedList.class,
        InstanceList.class,
        InstanceReference.class,
        InstancesScopedList.class,
        InstancesScopedList.Warning.class,
        InstancesScopedList.Warning.Data.class,
        JobConfiguration.class,
        JobConfigurationExtract.class,
        JobConfigurationLink.class,
        JobConfigurationLoad.class,
        JobConfigurationQuery.class,
        JobConfigurationTableCopy.class,
        JobList.class,
        JobList.Jobs.class,
        JobMessage.class,
        JobReference.class,
        JobRequest.class,
        JobStatistics.class,
        JobStatistics2.class,
        JobStatistics3.class,
        JobStatus.class,
        JsonObject.class,
        JsonWebSignature.Header.class,
        JsonWebToken.Header.class,
        JsonWebToken.Payload.class,
        KeyRangeLocation.class,
        Label.class,
        License.class,
        ListCoverageBucketsResponse.class,
        ListDatasetsResponse.class,
        ListEntriesResponse.class,
        ListJobMessagesResponse.class,
        ListSubscriptionsResponse.class,
        ListTopicsResponse.class,
        Location.class,
        MachineType.class,
        MachineType.ScratchDisks.class,
        MachineTypeAggregatedList.class,
        MachineTypeList.class,
        MachineTypesScopedList.class,
        MachineTypesScopedList.Warning.class,
        MachineTypesScopedList.Warning.Data.class,
        ModifyAckDeadlineRequest.class,
        ModifyPushConfigRequest.class,
        ModifyWorkflowRequest.class,
        Network.class,
        NetworkInterface.class,
        NetworkList.class,
        ObjectAccessControl.class,
        ObjectAccessControl.ProjectTeam.class,
        ObjectAccessControls.class,
        Objects.class,
        Operation.class,
        Operation.Error.class,
        Operation.Error.Errors.class,
        Operation.Warnings.class,
        Operation.Warnings.Data.class,
        OperationAggregatedList.class,
        OperationList.class,
        OperationsScopedList.class,
        OperationsScopedList.Warning.class,
        OperationsScopedList.Warning.Data.class,
        OutputDefinition.class,
        PathMatcher.class,
        PathRule.class,
        PortReference.class,
        Program.class,
        Project.class,
        ProjectList.class,
        ProjectList.Projects.class,
        ProjectReference.class,
        PublishRequest.class,
        PubsubEvent.class,
        PubsubLocation.class,
        PubsubMessage.class,
        PullRequest.class,
        PullResponse.class,
        PushConfig.class,
        QueryRequest.class,
        QueryResponse.class,
        Quota.class,
        Read.class,
        ReadGroup.class,
        Readset.class,
        ReferenceBound.class,
        ReferenceSequence.class,
        Region.class,
        RegionList.class,
        RequestWorkRequest.class,
        ResourceGroupReference.class,
        Route.class,
        Route.Warnings.class,
        Route.Warnings.Data.class,
        RouteList.class,
        Scheduling.class,
        SearchCallSetsRequest.class,
        SearchCallSetsResponse.class,
        SearchJobsRequest.class,
        SearchJobsResponse.class,
        SearchReadsRequest.class,
        SearchReadsResponse.class,
        SearchReadsetsRequest.class,
        SearchReadsetsResponse.class,
        SearchVariantSetsRequest.class,
        SearchVariantSetsResponse.class,
        SearchVariantsRequest.class,
        SearchVariantsResponse.class,
        SerialPortOutput.class,
        ServiceAccount.class,
        Snapshot.class,
        SnapshotList.class,
        StorageObject.class,
        StorageObject.Owner.class,
        Subscription.class,
        Table.class,
        TableCell.class,
        TableDataInsertAllRequest.class,
        TableDataInsertAllRequest.Rows.class,
        TableDataInsertAllResponse.class,
        TableDataInsertAllResponse.InsertErrors.class,
        TableDataList.class,
        TableFieldSchema.class,
        TableList.class,
        TableList.Tables.class,
        TableReference.class,
        TableSchema.class,
        Tags.class,
        TargetHttpProxy.class,
        TargetHttpProxyList.class,
        TargetInstance.class,
        TargetInstanceAggregatedList.class,
        TargetInstanceList.class,
        TargetInstancesScopedList.class,
        TargetInstancesScopedList.Warning.class,
        TargetInstancesScopedList.Warning.Data.class,
        TargetPool.class,
        TargetPoolAggregatedList.class,
        TargetPoolInstanceHealth.class,
        TargetPoolList.class,
        TargetPoolsAddHealthCheckRequest.class,
        TargetPoolsAddInstanceRequest.class,
        TargetPoolsRemoveHealthCheckRequest.class,
        TargetPoolsRemoveInstanceRequest.class,
        TargetPoolsScopedList.class,
        TargetPoolsScopedList.Warning.class,
        TargetPoolsScopedList.Warning.Data.class,
        TargetReference.class,
        TaskRunnerConfig.class,
        TestFailure.class,
        TokenErrorResponse.class,
        TokenResponse.class,
        Topic.class,
        TopologyConfig.class,
        UrlMap.class,
        UrlMapList.class,
        UrlMapReference.class,
        UrlMapTest.class,
        UrlMapValidationResult.class,
        UrlMapsValidateRequest.class,
        UrlMapsValidateResponse.class,
        UsageExportLocation.class,
        VMMetadataItem.class,
        Variant.class,
        VariantSet.class,
        ViewDefinition.class,
        WaitPortDefinition.class,
        WindmillLocation.class,
        WorkerFlags.class,
        WorkflowJob.class,
        WorkflowJobStatus.class,
        Zone.class,
        Zone.MaintenanceWindows.class,
        ZoneList.class,
        com.google.api.services.bigquery.model.Dataset.class,
        com.google.api.services.bigquery.model.Dataset.Access.class,
        com.google.api.services.bigquery.model.Job.class,
        com.google.api.services.compute.model.Disk.class,
        com.google.api.services.compute.model.Metadata.class,
        com.google.api.services.compute.model.Metadata.Items.class,
        com.google.api.services.dataflow.model.Disk.class,
        com.google.api.services.genomics.model.Dataset.class,
        com.google.api.services.genomics.model.Job.class,
        com.google.api.services.genomics.model.Metadata.class)) {
      registry.registerCoder(type,
          new CoderRegistry.CoderFactory() {
            @Override public Coder<?> create(
                @SuppressWarnings("rawtypes") List<? extends Coder> unused) {
              return GenericJsonCoder.of(type);
            }
          });
    }
    return pipeline;
  }
}
