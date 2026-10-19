import { HeadBucketCommand, ListObjectsV2Command, S3Client } from "@aws-sdk/client-s3";
import { createPresignedPost } from "@aws-sdk/s3-presigned-post";
import type { Config } from "../config.js";

export interface StoredObject {
  key: string;
  size: number;
}

export function createStorage(config: Config["s3"]) {
  const clientFor = (endpoint: string) =>
    new S3Client({
      endpoint,
      region: config.region,
      credentials: { accessKeyId: config.accessKey, secretAccessKey: config.secretKey },
      forcePathStyle: true, // MinIO serves buckets as paths, not subdomains
    });
  const internal = clientFor(config.endpoint);
  const publicClient = clientFor(config.publicEndpoint);
  const bucket = config.bucket;

  return {
    async ping() {
      await internal.send(new HeadBucketCommand({ Bucket: bucket }));
    },

    /** All objects under a prefix (S3 returns at most 1000 per page). */
    async listPrefix(prefix: string): Promise<StoredObject[]> {
      const objects: StoredObject[] = [];
      let token: string | undefined;
      do {
        const page = await internal.send(
          new ListObjectsV2Command({ Bucket: bucket, Prefix: prefix, ContinuationToken: token }),
        );
        for (const o of page.Contents ?? []) {
          if (o.Key) objects.push({ key: o.Key, size: o.Size ?? 0 });
        }
        token = page.IsTruncated ? page.NextContinuationToken : undefined;
      } while (token);
      return objects;
    },

    /**
     * One POST policy for a whole "directory": the client may upload any key under prefix.
     * `${filename}` in the key is replaced by S3 with the uploaded file's name.
     */
    presignPrefixUpload(prefix: string, maxFileBytes: number, expiresInSeconds: number) {
      return createPresignedPost(publicClient, {
        Bucket: bucket,
        Key: `${prefix}\${filename}`,
        Conditions: [
          ["starts-with", "$key", prefix],
          ["content-length-range", 1, maxFileBytes],
        ],
        Expires: expiresInSeconds,
      });
    },
  };
}

export type Storage = ReturnType<typeof createStorage>;
