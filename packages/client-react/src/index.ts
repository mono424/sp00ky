import type {
  GetTable,
  SchemaStructure,
  TableModel,
  TableNames,
  QueryResult,
  RelatedFieldsMap,
  RelationshipFieldsFromSchema,
  GetRelationship,
  RelatedFieldMapEntry,
  QueryModifier,
  QueryModifierBuilder,
  QueryInfo,
  RelationshipsMetadata,
  RelationshipDefinition,
  InferRelatedModelFromMetadata,
  GetCardinality,
} from '@spooky-sync/query-builder';

export { RecordId, Uuid } from 'surrealdb';
export { SyncedDb } from './db';

export { Sp00kyProvider, useDb, type Sp00kyProviderProps } from './provider';
export {
  useQuery,
  usePreload,
  type UseQueryResult,
  type QueryData,
  type QueryOptions,
  type UsePreloadOptions,
} from './query';
export {
  usePendingMutations,
  useSyncStatus,
  useSyncActivity,
  useStorageStatus,
  useImpersonation,
  useFeatureFlag,
  useAppRelease,
  useSubmission,
  type UseSyncStatus,
  type UseSyncActivity,
  type UseSyncActivityOptions,
  type UseStorageStatus,
  type UseImpersonation,
  type UseImpersonationOptions,
  type UseFeatureFlag,
  type UseAppRelease,
  type UseAppReleaseOptions,
  type Submission,
} from './status';
export {
  useCrdtField,
  useChannel,
  type UseChannel,
  type ChannelPeers,
  type ChannelQueries,
} from './realtime';
export {
  useFileUpload,
  useDownloadFile,
  useBlurhash,
  useBucketImage,
  Blurhash,
  BucketImage,
  type FileUploadResult,
  type UseDownloadFileOptions,
  type UseDownloadFileResult,
  type UseBlurhashResult,
  type UseBucketImageOptions,
  type UseBucketImageResult,
  type BlurhashProps,
  type BucketImageProps,
} from './files';

export type { PreloadOptions, PreloadRefresh } from '@spooky-sync/core';
export type {
  SyncHealth,
  SyncHealthStatus,
  SyncHealthConfig,
  ConnectionState,
  ReconnectConfig,
  StorageHealth,
  StorageHealthStatus,
  ChannelState,
  ChannelStatus,
  ChannelOp,
  ChannelsConfig,
  Sp00kyChannel,
} from '@spooky-sync/core';
export {
  WebPushError,
  type WebPushModule,
  type WebPushConfig,
  type WebPushSupport,
  type WebPushUnsupportedReason,
  type WebPushInfo,
  type WebPushDevice,
  type PushDeviceKind,
  type WebPushSubscribeOptions,
  type WebPushUnsubscribeOptions,
  type WebPushSyncOptions,
  type WebPushSyncStatus,
  type WebPushUpdateOptions,
  type WebPushMessageInput,
  type WebPushMessage,
  type WebPushErrorCode,
  type WebPushClientMessage,
  type PushPayload,
  type PushNotification,
} from '@spooky-sync/core';

// Re-export query builder types for convenience
export type {
  QueryModifier,
  QueryModifierBuilder,
  QueryInfo,
  RelationshipsMetadata,
  RelationshipDefinition,
  InferRelatedModelFromMetadata,
  GetCardinality,
  GetTable,
  TableModel,
  TableNames,
  QueryResult,
};

export type RelationshipField<
  Schema extends SchemaStructure,
  TableName extends TableNames<Schema>,
  Field extends RelationshipFieldsFromSchema<Schema, TableName>,
> = GetRelationship<Schema, TableName, Field>;

export type RelatedFieldsTableScoped<
  Schema extends SchemaStructure,
  TableName extends TableNames<Schema>,
  RelatedFields extends RelationshipFieldsFromSchema<Schema, TableName> =
    RelationshipFieldsFromSchema<Schema, TableName>,
> = {
  [K in RelatedFields]: {
    to: RelationshipField<Schema, TableName, K>['to'];
    relatedFields: RelatedFieldsMap;
    cardinality: RelationshipField<Schema, TableName, K>['cardinality'];
  };
};

export type InferModel<
  Schema extends SchemaStructure,
  TableName extends TableNames<Schema>,
  RelatedFields extends RelatedFieldsTableScoped<Schema, TableName>,
> = QueryResult<Schema, TableName, RelatedFields, true>;

export type WithRelated<Field extends string, RelatedFields extends RelatedFieldsMap = {}> = {
  [K in Field]: Omit<RelatedFieldMapEntry, 'relatedFields'> & {
    relatedFields: RelatedFields;
  };
};

export type WithRelatedMany<Field extends string, RelatedFields extends RelatedFieldsMap = {}> = {
  [K in Field]: {
    to: Field;
    relatedFields: RelatedFields;
    cardinality: 'many';
  };
};

export * from './types';
