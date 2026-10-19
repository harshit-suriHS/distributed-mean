import { Router } from "express";
import type { Deps } from "../../app.js";
import { send } from "../../lib/http.js";
import { CreateUploadRequest, UploadResponse } from "./uploads.dto.js";
import { createUpload } from "./uploads.service.js";

export function uploadsRouter(deps: Deps) {
  const router = Router();

  router.post("/", async (req, res) => {
    CreateUploadRequest.parse(req.body ?? {}); // Express 5 leaves body undefined when none is sent
    send(res, 201, UploadResponse, await createUpload(deps));
  });

  return router;
}
