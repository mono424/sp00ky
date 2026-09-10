// The client lives in client/; this path is kept for `index.ts` and deep imports.
export { Sp00kyClient } from './client/sp00ky-client';
export type { Sp00kyClientDeps } from './client/sp00ky-client';
export { BucketHandle, bucketContentToBlob } from './client/bucket-handle';
export type { BucketPutOptions, BucketPutResult } from './client/bucket-handle';
