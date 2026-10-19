import { z } from "zod";

// ---- Requests ----

// No options yet; strict so a client sending fields we'd ignore finds out.
export const CreateUploadRequest = z.strictObject({});

// ---- Responses ----

export const UploadResponse = z.object({
  upload_id: z.uuid(),
  prefix: z.string(),
  expires_at: z.iso.datetime(),
  max_file_bytes: z.int(),
  // POST each file as multipart/form-data to `url` with every field below,
  // plus `key` = prefix + file name (or keep `${filename}`) and `file` last.
  url: z.url(),
  fields: z.record(z.string(), z.string()),
});
export type UploadResponse = z.infer<typeof UploadResponse>;
