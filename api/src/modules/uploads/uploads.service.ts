import type { Deps } from "../../app.js";
import type { UploadResponse } from "./uploads.dto.js";
import { insertUpload } from "./uploads.repo.js";

export const uploadPrefix = (uploadUuid: string) => `inputs/${uploadUuid}/`;

export async function createUpload({ config, pool, storage }: Deps): Promise<UploadResponse> {
  const upload = await insertUpload(pool, config.uploads.ttlHours);
  const prefix = uploadPrefix(upload.uuid);
  const expiresIn = Math.floor((upload.expires_at.getTime() - Date.now()) / 1000);
  const policy = await storage.presignPrefixUpload(prefix, config.uploads.maxFileBytes, expiresIn);

  return {
    upload_id: upload.uuid,
    prefix,
    expires_at: upload.expires_at.toISOString(),
    max_file_bytes: config.uploads.maxFileBytes,
    url: policy.url,
    fields: policy.fields,
  };
}
