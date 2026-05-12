import { randomUUID } from "node:crypto";

const IMAGE_AUDIT_FIELDS = [
  "id",
  "uuid",
  "owner_user_id",
  "uploaded_by_user_id",
  "original_filename",
  "storage_path",
  "mime_type",
  "file_size_bytes",
  "width",
  "height",
  "title",
  "alt_text",
  "caption",
  "status",
  "visibility",
  "metadata_json",
  "created_at",
  "updated_at",
  "deleted_at",
];
const IMAGEABLE_AUDIT_FIELDS = [
  "id",
  "image_id",
  "imageable_type",
  "imageable_id",
  "role",
  "sort_order",
  "crop_json",
];
const IMAGE_VARIANT_AUDIT_FIELDS = [
  "id",
  "image_id",
  "variant",
  "storage_path",
  "public_url",
  "mime_type",
  "file_size_bytes",
  "width",
  "height",
];
const IMAGE_STATUSES = ["uploaded", "processing", "ready", "failed", "archived"];
const IMAGE_VISIBILITIES = ["private", "unlisted", "public"];

function requireAuthenticated(req, res, next) {
  if (!req.user) {
    return res.status(401).json({ ok: false, message: "Unauthorized" });
  }
  return next();
}

function validationError(message) {
  const error = new Error(message);
  error.status = 400;
  return error;
}

function hasPayloadField(payload, ...fieldNames) {
  if (!payload || typeof payload !== "object") return false;
  return fieldNames.some((fieldName) => Object.prototype.hasOwnProperty.call(payload, fieldName));
}

function normalizeNonNegativeIntegerOrNull(value) {
  const raw = String(value ?? "").trim();
  if (!raw) return null;
  const normalized = Number.parseInt(raw, 10);
  return Number.isInteger(normalized) && normalized >= 0 ? normalized : null;
}

function normalizeImageStatus(value, fallback = "uploaded") {
  const normalized = String(value || "").trim().toLowerCase();
  if (IMAGE_STATUSES.includes(normalized)) return normalized;
  return fallback;
}

function normalizeImageVisibility(value, fallback = "private") {
  const normalized = String(value || "").trim().toLowerCase();
  if (IMAGE_VISIBILITIES.includes(normalized)) return normalized;
  return fallback;
}

function isImageVariantUniqueError(error) {
  const message = String(error?.message || "");
  return message.includes("idx_image_variants_unique")
    || message.includes("image_variants.image_id")
    || message.includes("image_variants.variant");
}

function createImageService(deps) {
  const {
    db,
    dbAllAsync,
    dbGetAsync,
    dbRunAsync,
    normalizeIntegerOrNull,
    normalizeNullableText,
    normalizePositiveInteger,
    parseJsonOrNull,
    safeStringifyJson,
  } = deps;

  function normalizeJsonText(value, fieldName, fallback = null) {
    if (value === undefined || value === null || value === "") return fallback;
    if (typeof value === "string") {
      const raw = value.trim();
      if (!raw) return fallback;
      try {
        JSON.parse(raw);
        return raw;
      } catch (_error) {
        throw validationError(`${fieldName} must be valid JSON`);
      }
    }
    return safeStringifyJson(value);
  }

  function normalizeImageDimension(value, fieldName) {
    const normalized = normalizeNonNegativeIntegerOrNull(value);
    if (normalized === null && value !== undefined && value !== null && String(value).trim() !== "") {
      throw validationError(`${fieldName} must be a non-negative integer`);
    }
    return normalized;
  }

  function normalizeImageRow(row) {
    if (!row) return null;
    return {
      ...row,
      metadata_json: parseJsonOrNull(row.metadata_json) || {},
    };
  }

  function normalizeImageableRow(row) {
    if (!row) return null;
    return {
      ...row,
      crop_json: parseJsonOrNull(row.crop_json),
    };
  }

  async function loadImageByIdentifier(identifier, { includeDeleted = false } = {}) {
    const raw = String(identifier || "").trim();
    if (!raw) return null;
    const numericId = Number(raw);
    const isNumericId = Number.isInteger(numericId) && numericId > 0;
    const row = await dbGetAsync(
      `
        SELECT
          i.id,
          i.uuid,
          i.owner_user_id,
          i.uploaded_by_user_id,
          i.original_filename,
          i.storage_path,
          i.mime_type,
          i.file_size_bytes,
          i.width,
          i.height,
          i.title,
          i.alt_text,
          i.caption,
          i.status,
          i.visibility,
          i.metadata_json,
          i.created_at,
          i.updated_at,
          i.deleted_at,
          owner.email AS owner_email,
          owner.name AS owner_name,
          uploader.email AS uploaded_by_email,
          uploader.name AS uploaded_by_name
        FROM images i
        LEFT JOIN users owner
          ON owner.id = i.owner_user_id
        LEFT JOIN users uploader
          ON uploader.id = i.uploaded_by_user_id
        WHERE ${isNumericId ? "i.id = ?" : "i.uuid = ?"}
          ${includeDeleted ? "" : "AND i.deleted_at IS NULL"}
        LIMIT 1
      `,
      [isNumericId ? numericId : raw]
    );
    return normalizeImageRow(row);
  }

  async function loadImageableById(imageableId) {
    const normalizedId = normalizePositiveInteger(imageableId);
    if (!normalizedId) return null;
    const row = await dbGetAsync(
      `
        SELECT
          id,
          image_id,
          imageable_type,
          imageable_id,
          role,
          sort_order,
          crop_json,
          created_at,
          updated_at
        FROM imageables
        WHERE id = ?
        LIMIT 1
      `,
      [normalizedId]
    );
    return normalizeImageableRow(row);
  }

  async function loadImageVariantById(variantId) {
    const normalizedId = normalizePositiveInteger(variantId);
    if (!normalizedId) return null;
    return dbGetAsync(
      `
        SELECT
          id,
          image_id,
          variant,
          storage_path,
          public_url,
          mime_type,
          file_size_bytes,
          width,
          height,
          created_at,
          updated_at
        FROM image_variants
        WHERE id = ?
        LIMIT 1
      `,
      [normalizedId]
    );
  }

  function canAccessImage(user, image) {
    if (!user || !image) return false;
    if (Number(user.admin) === 1) return true;
    const userId = Number(user.id);
    return Number(image.owner_user_id) === userId
      || Number(image.uploaded_by_user_id) === userId
      || String(image.visibility || "").trim().toLowerCase() === "public";
  }

  function canManageImage(user, image) {
    if (!user || !image) return false;
    if (Number(user.admin) === 1) return true;
    const userId = Number(user.id);
    return Number(image.owner_user_id) === userId || Number(image.uploaded_by_user_id) === userId;
  }

  return {
    canAccessImage,
    canManageImage,
    db,
    dbAllAsync,
    dbGetAsync,
    dbRunAsync,
    loadImageByIdentifier,
    loadImageableById,
    loadImageVariantById,
    normalizeImageDimension,
    normalizeImageRow,
    normalizeImageableRow,
    normalizeIntegerOrNull,
    normalizeJsonText,
    normalizeNullableText,
    normalizePositiveInteger,
    safeStringifyJson,
  };
}

export function ensureImagesSchema({ db, addColumnIfMissing }) {
  db.run(`
    CREATE TABLE IF NOT EXISTS images (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      uuid TEXT NOT NULL UNIQUE,
      owner_user_id INTEGER,
      uploaded_by_user_id INTEGER,
      original_filename TEXT,
      storage_path TEXT NOT NULL,
      mime_type TEXT NOT NULL,
      file_size_bytes INTEGER,
      width INTEGER,
      height INTEGER,
      title TEXT,
      alt_text TEXT,
      caption TEXT,
      status TEXT NOT NULL DEFAULT 'uploaded',
      visibility TEXT NOT NULL DEFAULT 'private',
      metadata_json TEXT NOT NULL DEFAULT '{}',
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      deleted_at TEXT
    )
  `, (createErr) => {
    if (createErr) {
      console.error("Failed to ensure images schema", createErr);
      return;
    }

    db.all("PRAGMA table_info(images)", (pragmaErr, columns) => {
      if (pragmaErr) {
        console.error("Failed to inspect images schema", pragmaErr);
        return;
      }
      if (!Array.isArray(columns) || columns.length === 0) return;
      addColumnIfMissing(columns, "images", "uuid", "TEXT");
      addColumnIfMissing(columns, "images", "owner_user_id", "INTEGER");
      addColumnIfMissing(columns, "images", "uploaded_by_user_id", "INTEGER");
      addColumnIfMissing(columns, "images", "original_filename", "TEXT");
      addColumnIfMissing(columns, "images", "storage_path", "TEXT");
      addColumnIfMissing(columns, "images", "mime_type", "TEXT");
      addColumnIfMissing(columns, "images", "file_size_bytes", "INTEGER");
      addColumnIfMissing(columns, "images", "width", "INTEGER");
      addColumnIfMissing(columns, "images", "height", "INTEGER");
      addColumnIfMissing(columns, "images", "title", "TEXT");
      addColumnIfMissing(columns, "images", "alt_text", "TEXT");
      addColumnIfMissing(columns, "images", "caption", "TEXT");
      addColumnIfMissing(columns, "images", "status", "TEXT NOT NULL DEFAULT 'uploaded'");
      addColumnIfMissing(columns, "images", "visibility", "TEXT NOT NULL DEFAULT 'private'");
      addColumnIfMissing(columns, "images", "metadata_json", "TEXT NOT NULL DEFAULT '{}'");
      addColumnIfMissing(columns, "images", "created_at", "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP");
      addColumnIfMissing(columns, "images", "updated_at", "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP");
      addColumnIfMissing(columns, "images", "deleted_at", "TEXT");

      db.run(
        `
          UPDATE images
          SET
            uuid = CASE
              WHEN trim(COALESCE(uuid, '')) = '' THEN lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' || substr(lower(hex(randomblob(2))), 2) || '-' || substr('89ab', 1 + (abs(random()) % 4), 1) || substr(lower(hex(randomblob(2))), 2) || '-' || lower(hex(randomblob(6)))
              ELSE uuid
            END,
            original_filename = NULLIF(trim(original_filename), ''),
            storage_path = trim(COALESCE(storage_path, '')),
            mime_type = trim(COALESCE(mime_type, '')),
            title = NULLIF(trim(title), ''),
            alt_text = NULLIF(trim(alt_text), ''),
            caption = NULLIF(trim(caption), ''),
            status = CASE
              WHEN lower(trim(COALESCE(status, ''))) IN ('uploaded', 'processing', 'ready', 'failed', 'archived') THEN lower(trim(status))
              ELSE 'uploaded'
            END,
            visibility = CASE
              WHEN lower(trim(COALESCE(visibility, ''))) IN ('private', 'unlisted', 'public') THEN lower(trim(visibility))
              ELSE 'private'
            END,
            metadata_json = CASE
              WHEN trim(COALESCE(metadata_json, '')) = '' THEN '{}'
              ELSE metadata_json
            END
        `,
        (normalizeErr) => {
          if (normalizeErr) console.error("Failed to normalize images schema data", normalizeErr);
        }
      );

      db.run("CREATE UNIQUE INDEX IF NOT EXISTS idx_images_uuid ON images(uuid)", (indexErr) => {
        if (indexErr) console.error("Failed to ensure images.uuid index", indexErr);
      });
      db.run("CREATE INDEX IF NOT EXISTS idx_images_owner ON images(owner_user_id, deleted_at)", (indexErr) => {
        if (indexErr) console.error("Failed to ensure images.owner index", indexErr);
      });
      db.run("CREATE INDEX IF NOT EXISTS idx_images_visibility_status ON images(visibility, status, deleted_at)", (indexErr) => {
        if (indexErr) console.error("Failed to ensure images visibility/status index", indexErr);
      });
    });
  });

  db.run(`
    CREATE TABLE IF NOT EXISTS imageables (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      image_id INTEGER NOT NULL,
      imageable_type TEXT NOT NULL,
      imageable_id TEXT NOT NULL,
      role TEXT,
      sort_order INTEGER NOT NULL DEFAULT 0,
      crop_json TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
  `, (createErr) => {
    if (createErr) {
      console.error("Failed to ensure imageables schema", createErr);
      return;
    }

    db.all("PRAGMA table_info(imageables)", (pragmaErr, columns) => {
      if (pragmaErr) {
        console.error("Failed to inspect imageables schema", pragmaErr);
        return;
      }
      if (!Array.isArray(columns) || columns.length === 0) return;
      addColumnIfMissing(columns, "imageables", "image_id", "INTEGER");
      addColumnIfMissing(columns, "imageables", "imageable_type", "TEXT");
      addColumnIfMissing(columns, "imageables", "imageable_id", "TEXT");
      addColumnIfMissing(columns, "imageables", "role", "TEXT");
      addColumnIfMissing(columns, "imageables", "sort_order", "INTEGER NOT NULL DEFAULT 0");
      addColumnIfMissing(columns, "imageables", "crop_json", "TEXT");
      addColumnIfMissing(columns, "imageables", "created_at", "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP");
      addColumnIfMissing(columns, "imageables", "updated_at", "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP");
    });

    db.run("CREATE INDEX IF NOT EXISTS idx_imageables_image_id ON imageables(image_id)", (indexErr) => {
      if (indexErr) console.error("Failed to ensure imageables.image_id index", indexErr);
    });
    db.run(
      "CREATE INDEX IF NOT EXISTS idx_imageables_entity ON imageables(imageable_type, imageable_id, role, sort_order, id)",
      (indexErr) => {
        if (indexErr) console.error("Failed to ensure imageables entity index", indexErr);
      }
    );
  });

  db.run(`
    CREATE TABLE IF NOT EXISTS image_variants (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      image_id INTEGER NOT NULL,
      variant TEXT NOT NULL,
      storage_path TEXT NOT NULL,
      public_url TEXT,
      mime_type TEXT,
      file_size_bytes INTEGER,
      width INTEGER,
      height INTEGER,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
  `, (createErr) => {
    if (createErr) {
      console.error("Failed to ensure image_variants schema", createErr);
      return;
    }

    db.all("PRAGMA table_info(image_variants)", (pragmaErr, columns) => {
      if (pragmaErr) {
        console.error("Failed to inspect image_variants schema", pragmaErr);
        return;
      }
      if (!Array.isArray(columns) || columns.length === 0) return;
      addColumnIfMissing(columns, "image_variants", "image_id", "INTEGER");
      addColumnIfMissing(columns, "image_variants", "variant", "TEXT");
      addColumnIfMissing(columns, "image_variants", "storage_path", "TEXT");
      addColumnIfMissing(columns, "image_variants", "public_url", "TEXT");
      addColumnIfMissing(columns, "image_variants", "mime_type", "TEXT");
      addColumnIfMissing(columns, "image_variants", "file_size_bytes", "INTEGER");
      addColumnIfMissing(columns, "image_variants", "width", "INTEGER");
      addColumnIfMissing(columns, "image_variants", "height", "INTEGER");
      addColumnIfMissing(columns, "image_variants", "created_at", "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP");
      addColumnIfMissing(columns, "image_variants", "updated_at", "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP");
    });

    db.run(
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_image_variants_unique ON image_variants(image_id, variant)",
      (indexErr) => {
        if (indexErr) console.error("Failed to ensure image_variants unique index", indexErr);
      }
    );
  });
}

export function registerImageRoutes(app, deps) {
  const {
    buildAuditChanges,
    buildAuditCreationChanges,
    buildAuditDeletionChanges,
    getAuditActor,
    logAuditEvent,
  } = deps;
  const service = createImageService(deps);

  app.get("/public/images", async (req, res) => {
    try {
      const status = req.query?.status === undefined
        ? "ready"
        : normalizeImageStatus(req.query.status, null);
      if (!status) {
        return res.status(400).json({ ok: false, message: "Invalid image status" });
      }
      const rows = await service.dbAllAsync(
        `
          SELECT
            id,
            uuid,
            owner_user_id,
            uploaded_by_user_id,
            original_filename,
            storage_path,
            mime_type,
            file_size_bytes,
            width,
            height,
            title,
            alt_text,
            caption,
            status,
            visibility,
            metadata_json,
            created_at,
            updated_at,
            deleted_at
          FROM images
          WHERE deleted_at IS NULL
            AND visibility = 'public'
            AND status = ?
          ORDER BY datetime(updated_at) DESC, id DESC
        `,
        [status]
      );
      return res.json({ ok: true, images: (rows || []).map(service.normalizeImageRow) });
    } catch (error) {
      console.error("Failed to load public images", error);
      return res.status(500).json({ ok: false, message: "Failed to load images" });
    }
  });

  app.get("/images", requireAuthenticated, async (req, res) => {
    try {
      const isAdmin = Number(req.user?.admin) === 1;
      const includeDeleted = isAdmin && String(req.query?.include_deleted || "").trim() === "1";
      const filters = [];
      const params = [];

      if (!includeDeleted) filters.push("i.deleted_at IS NULL");

      if (!isAdmin) {
        filters.push("(i.owner_user_id = ? OR i.uploaded_by_user_id = ? OR i.visibility = 'public')");
        params.push(Number(req.user.id), Number(req.user.id));
      } else if (req.query?.owner_user_id !== undefined) {
        const ownerUserId = service.normalizePositiveInteger(req.query.owner_user_id);
        if (!ownerUserId) {
          return res.status(400).json({ ok: false, message: "owner_user_id must be a positive integer" });
        }
        filters.push("i.owner_user_id = ?");
        params.push(ownerUserId);
      }

      if (req.query?.status !== undefined) {
        const status = normalizeImageStatus(req.query.status, null);
        if (!status) return res.status(400).json({ ok: false, message: "Invalid image status" });
        filters.push("i.status = ?");
        params.push(status);
      }

      if (req.query?.visibility !== undefined) {
        const visibility = normalizeImageVisibility(req.query.visibility, null);
        if (!visibility) return res.status(400).json({ ok: false, message: "Invalid image visibility" });
        filters.push("i.visibility = ?");
        params.push(visibility);
      }

      const rows = await service.dbAllAsync(
        `
          SELECT
            i.id,
            i.uuid,
            i.owner_user_id,
            i.uploaded_by_user_id,
            i.original_filename,
            i.storage_path,
            i.mime_type,
            i.file_size_bytes,
            i.width,
            i.height,
            i.title,
            i.alt_text,
            i.caption,
            i.status,
            i.visibility,
            i.metadata_json,
            i.created_at,
            i.updated_at,
            i.deleted_at,
            owner.email AS owner_email,
            owner.name AS owner_name,
            uploader.email AS uploaded_by_email,
            uploader.name AS uploaded_by_name
          FROM images i
          LEFT JOIN users owner
            ON owner.id = i.owner_user_id
          LEFT JOIN users uploader
            ON uploader.id = i.uploaded_by_user_id
          ${filters.length ? `WHERE ${filters.join(" AND ")}` : ""}
          ORDER BY datetime(i.updated_at) DESC, i.id DESC
        `,
        params
      );
      return res.json({ ok: true, images: (rows || []).map(service.normalizeImageRow) });
    } catch (error) {
      console.error("Failed to load images", error);
      return res.status(500).json({ ok: false, message: "Failed to load images" });
    }
  });

  app.get("/images/:id", requireAuthenticated, async (req, res) => {
    try {
      const image = await service.loadImageByIdentifier(req.params.id, {
        includeDeleted: Number(req.user?.admin) === 1,
      });
      if (!image) return res.status(404).json({ ok: false, message: "Image not found" });
      if (!service.canAccessImage(req.user, image)) return res.status(403).json({ ok: false, message: "Forbidden" });
      return res.json({ ok: true, image });
    } catch (error) {
      console.error("Failed to load image", error);
      return res.status(500).json({ ok: false, message: "Failed to load image" });
    }
  });

  app.post("/images", requireAuthenticated, async (req, res) => {
    try {
      const isAdmin = Number(req.user?.admin) === 1;
      const ownerUserId = isAdmin
        ? (service.normalizePositiveInteger(req.body?.owner_user_id ?? req.body?.ownerUserId) || Number(req.user.id))
        : Number(req.user.id);
      const uploadedByUserId = Number(req.user.id);
      const originalFilename = service.normalizeNullableText(req.body?.original_filename ?? req.body?.originalFilename);
      const storagePath = service.normalizeNullableText(req.body?.storage_path ?? req.body?.storagePath);
      const mimeType = service.normalizeNullableText(req.body?.mime_type ?? req.body?.mimeType);
      const fileSizeBytes = service.normalizeImageDimension(req.body?.file_size_bytes ?? req.body?.fileSizeBytes, "file_size_bytes");
      const width = service.normalizeImageDimension(req.body?.width, "width");
      const height = service.normalizeImageDimension(req.body?.height, "height");
      const title = service.normalizeNullableText(req.body?.title);
      const altText = service.normalizeNullableText(req.body?.alt_text ?? req.body?.altText);
      const caption = service.normalizeNullableText(req.body?.caption);
      const status = normalizeImageStatus(req.body?.status, "uploaded");
      const visibility = normalizeImageVisibility(req.body?.visibility, "private");
      const metadataJson = service.normalizeJsonText(req.body?.metadata_json ?? req.body?.metadataJson, "metadata_json", "{}");

      if (hasPayloadField(req.body, "status") && !IMAGE_STATUSES.includes(String(req.body.status || "").trim().toLowerCase())) {
        return res.status(400).json({ ok: false, message: "Invalid image status" });
      }
      if (hasPayloadField(req.body, "visibility") && !IMAGE_VISIBILITIES.includes(String(req.body.visibility || "").trim().toLowerCase())) {
        return res.status(400).json({ ok: false, message: "Invalid image visibility" });
      }
      if (!storagePath) return res.status(400).json({ ok: false, message: "storage_path is required" });
      if (!mimeType) return res.status(400).json({ ok: false, message: "mime_type is required" });
      if (!mimeType.toLowerCase().startsWith("image/")) {
        return res.status(400).json({ ok: false, message: "mime_type must be an image type" });
      }

      if (ownerUserId) {
        const ownerRow = await service.dbGetAsync("SELECT id FROM users WHERE id = ? LIMIT 1", [ownerUserId]);
        if (!ownerRow) {
          return res.status(400).json({ ok: false, message: "owner_user_id must reference an existing user" });
        }
      }

      const insertResult = await service.dbRunAsync(
        `
          INSERT INTO images (
            uuid,
            owner_user_id,
            uploaded_by_user_id,
            original_filename,
            storage_path,
            mime_type,
            file_size_bytes,
            width,
            height,
            title,
            alt_text,
            caption,
            status,
            visibility,
            metadata_json,
            created_at,
            updated_at
          )
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        `,
        [
          randomUUID(),
          ownerUserId,
          uploadedByUserId,
          originalFilename,
          storagePath,
          mimeType,
          fileSizeBytes,
          width,
          height,
          title,
          altText,
          caption,
          status,
          visibility,
          metadataJson,
        ]
      );

      const createdRow = await service.loadImageByIdentifier(insertResult?.lastID);
      return logAuditEvent(
        {
          ...getAuditActor(req.user),
          event_type: "image.created",
          entity_type: "image",
          action: "create",
          record_id: String(insertResult?.lastID || ""),
          changes: buildAuditCreationChanges(createdRow || {}, IMAGE_AUDIT_FIELDS),
        },
        () => res.status(201).json({ ok: true, image: createdRow || null })
      );
    } catch (error) {
      console.error("Failed to create image", error);
      return res.status(error.status || 500).json({ ok: false, message: error.status ? error.message : "Failed to create image" });
    }
  });

  app.patch("/images/:id", requireAuthenticated, async (req, res) => {
    try {
      const image = await service.loadImageByIdentifier(req.params.id, {
        includeDeleted: Number(req.user?.admin) === 1,
      });
      if (!image) return res.status(404).json({ ok: false, message: "Image not found" });
      if (!service.canManageImage(req.user, image)) return res.status(403).json({ ok: false, message: "Forbidden" });

      const isAdmin = Number(req.user?.admin) === 1;
      const payloadId = service.normalizePositiveInteger(req.body?.id);
      if (payloadId && payloadId !== Number(image.id)) {
        return res.status(400).json({ ok: false, message: "Image id cannot be changed" });
      }

      const ownerUserId = isAdmin && hasPayloadField(req.body, "owner_user_id", "ownerUserId")
        ? service.normalizePositiveInteger(req.body?.owner_user_id ?? req.body?.ownerUserId)
        : Number(image.owner_user_id);
      const originalFilename = hasPayloadField(req.body, "original_filename", "originalFilename")
        ? service.normalizeNullableText(req.body?.original_filename ?? req.body?.originalFilename)
        : image.original_filename;
      const storagePath = hasPayloadField(req.body, "storage_path", "storagePath")
        ? service.normalizeNullableText(req.body?.storage_path ?? req.body?.storagePath)
        : image.storage_path;
      const mimeType = hasPayloadField(req.body, "mime_type", "mimeType")
        ? service.normalizeNullableText(req.body?.mime_type ?? req.body?.mimeType)
        : image.mime_type;
      const fileSizeBytes = hasPayloadField(req.body, "file_size_bytes", "fileSizeBytes")
        ? service.normalizeImageDimension(req.body?.file_size_bytes ?? req.body?.fileSizeBytes, "file_size_bytes")
        : image.file_size_bytes;
      const width = hasPayloadField(req.body, "width")
        ? service.normalizeImageDimension(req.body?.width, "width")
        : image.width;
      const height = hasPayloadField(req.body, "height")
        ? service.normalizeImageDimension(req.body?.height, "height")
        : image.height;
      const title = hasPayloadField(req.body, "title")
        ? service.normalizeNullableText(req.body?.title)
        : image.title;
      const altText = hasPayloadField(req.body, "alt_text", "altText")
        ? service.normalizeNullableText(req.body?.alt_text ?? req.body?.altText)
        : image.alt_text;
      const caption = hasPayloadField(req.body, "caption")
        ? service.normalizeNullableText(req.body?.caption)
        : image.caption;
      const status = hasPayloadField(req.body, "status")
        ? normalizeImageStatus(req.body?.status, null)
        : String(image.status || "uploaded");
      const visibility = hasPayloadField(req.body, "visibility")
        ? normalizeImageVisibility(req.body?.visibility, null)
        : String(image.visibility || "private");
      const metadataJson = hasPayloadField(req.body, "metadata_json", "metadataJson")
        ? service.normalizeJsonText(req.body?.metadata_json ?? req.body?.metadataJson, "metadata_json", "{}")
        : service.safeStringifyJson(image.metadata_json || {});

      if (!status) return res.status(400).json({ ok: false, message: "Invalid image status" });
      if (!visibility) return res.status(400).json({ ok: false, message: "Invalid image visibility" });
      if (!storagePath) return res.status(400).json({ ok: false, message: "storage_path is required" });
      if (!mimeType) return res.status(400).json({ ok: false, message: "mime_type is required" });
      if (!mimeType.toLowerCase().startsWith("image/")) {
        return res.status(400).json({ ok: false, message: "mime_type must be an image type" });
      }
      if (isAdmin && ownerUserId) {
        const ownerRow = await service.dbGetAsync("SELECT id FROM users WHERE id = ? LIMIT 1", [ownerUserId]);
        if (!ownerRow) {
          return res.status(400).json({ ok: false, message: "owner_user_id must reference an existing user" });
        }
      }

      await service.dbRunAsync(
        `
          UPDATE images
          SET
            owner_user_id = ?,
            original_filename = ?,
            storage_path = ?,
            mime_type = ?,
            file_size_bytes = ?,
            width = ?,
            height = ?,
            title = ?,
            alt_text = ?,
            caption = ?,
            status = ?,
            visibility = ?,
            metadata_json = ?,
            updated_at = CURRENT_TIMESTAMP
          WHERE id = ?
        `,
        [
          ownerUserId || null,
          originalFilename,
          storagePath,
          mimeType,
          fileSizeBytes,
          width,
          height,
          title,
          altText,
          caption,
          status,
          visibility,
          metadataJson,
          image.id,
        ]
      );

      const updatedRow = await service.loadImageByIdentifier(image.id, { includeDeleted: true });
      const changes = buildAuditChanges(image, updatedRow, IMAGE_AUDIT_FIELDS);
      if (!Object.keys(changes).length) return res.json({ ok: true, image: updatedRow || null });
      return logAuditEvent(
        {
          ...getAuditActor(req.user),
          event_type: "image.updated",
          entity_type: "image",
          action: "update",
          record_id: String(image.id),
          changes,
        },
        () => res.json({ ok: true, image: updatedRow || null })
      );
    } catch (error) {
      console.error("Failed to update image", error);
      return res.status(error.status || 500).json({ ok: false, message: error.status ? error.message : "Failed to update image" });
    }
  });

  app.delete("/images/:id", requireAuthenticated, async (req, res) => {
    try {
      const image = await service.loadImageByIdentifier(req.params.id);
      if (!image) return res.status(404).json({ ok: false, message: "Image not found" });
      if (!service.canManageImage(req.user, image)) return res.status(403).json({ ok: false, message: "Forbidden" });

      await service.dbRunAsync(
        `
          UPDATE images
          SET
            deleted_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
          WHERE id = ?
        `,
        [image.id]
      );
      const deletedRow = await service.loadImageByIdentifier(image.id, { includeDeleted: true });
      return logAuditEvent(
        {
          ...getAuditActor(req.user),
          event_type: "image.deleted",
          entity_type: "image",
          action: "delete",
          record_id: String(image.id),
          changes: buildAuditChanges(image, deletedRow, IMAGE_AUDIT_FIELDS),
        },
        () => res.json({ ok: true })
      );
    } catch (error) {
      console.error("Failed to delete image", error);
      return res.status(500).json({ ok: false, message: "Failed to delete image" });
    }
  });

  app.get("/images/:id/imageables", requireAuthenticated, async (req, res) => {
    try {
      const image = await service.loadImageByIdentifier(req.params.id);
      if (!image) return res.status(404).json({ ok: false, message: "Image not found" });
      if (!service.canAccessImage(req.user, image)) return res.status(403).json({ ok: false, message: "Forbidden" });
      const rows = await service.dbAllAsync(
        `
          SELECT
            id,
            image_id,
            imageable_type,
            imageable_id,
            role,
            sort_order,
            crop_json,
            created_at,
            updated_at
          FROM imageables
          WHERE image_id = ?
          ORDER BY sort_order ASC, id ASC
        `,
        [image.id]
      );
      return res.json({ ok: true, imageables: (rows || []).map(service.normalizeImageableRow) });
    } catch (error) {
      console.error("Failed to load imageables", error);
      return res.status(500).json({ ok: false, message: "Failed to load imageables" });
    }
  });

  app.post("/imageables", requireAuthenticated, async (req, res) => {
    try {
      const imageId = service.normalizePositiveInteger(req.body?.image_id ?? req.body?.imageId);
      const imageableType = service.normalizeNullableText(req.body?.imageable_type ?? req.body?.imageableType);
      const imageableId = service.normalizeNullableText(req.body?.imageable_id ?? req.body?.imageableId);
      const role = service.normalizeNullableText(req.body?.role);
      const sortOrder = service.normalizeIntegerOrNull(req.body?.sort_order ?? req.body?.sortOrder) ?? 0;
      const cropJson = service.normalizeJsonText(req.body?.crop_json ?? req.body?.cropJson, "crop_json", null);

      if (!imageId) return res.status(400).json({ ok: false, message: "image_id is required" });
      if (!imageableType) return res.status(400).json({ ok: false, message: "imageable_type is required" });
      if (!imageableId) return res.status(400).json({ ok: false, message: "imageable_id is required" });

      const image = await service.loadImageByIdentifier(imageId);
      if (!image) return res.status(400).json({ ok: false, message: "image_id must reference an existing image" });
      if (!service.canManageImage(req.user, image)) return res.status(403).json({ ok: false, message: "Forbidden" });

      const insertResult = await service.dbRunAsync(
        `
          INSERT INTO imageables (
            image_id,
            imageable_type,
            imageable_id,
            role,
            sort_order,
            crop_json,
            created_at,
            updated_at
          )
          VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        `,
        [imageId, imageableType, imageableId, role, sortOrder, cropJson]
      );
      const createdRow = await service.loadImageableById(insertResult?.lastID);
      return logAuditEvent(
        {
          ...getAuditActor(req.user),
          event_type: "imageable.created",
          entity_type: "imageable",
          action: "create",
          record_id: String(insertResult?.lastID || ""),
          changes: buildAuditCreationChanges(createdRow || {}, IMAGEABLE_AUDIT_FIELDS),
        },
        () => res.status(201).json({ ok: true, imageable: createdRow || null })
      );
    } catch (error) {
      console.error("Failed to create imageable", error);
      return res.status(error.status || 500).json({ ok: false, message: error.status ? error.message : "Failed to create imageable" });
    }
  });

  app.get("/imageables/:id", requireAuthenticated, async (req, res) => {
    try {
      const imageable = await service.loadImageableById(req.params.id);
      if (!imageable) return res.status(404).json({ ok: false, message: "Imageable not found" });
      const image = await service.loadImageByIdentifier(imageable.image_id, {
        includeDeleted: Number(req.user?.admin) === 1,
      });
      if (!service.canAccessImage(req.user, image)) return res.status(403).json({ ok: false, message: "Forbidden" });
      return res.json({ ok: true, imageable });
    } catch (error) {
      console.error("Failed to load imageable", error);
      return res.status(500).json({ ok: false, message: "Failed to load imageable" });
    }
  });

  app.patch("/imageables/:id", requireAuthenticated, async (req, res) => {
    try {
      const beforeRow = await service.loadImageableById(req.params.id);
      if (!beforeRow) return res.status(404).json({ ok: false, message: "Imageable not found" });
      const image = await service.loadImageByIdentifier(beforeRow.image_id, { includeDeleted: Number(req.user?.admin) === 1 });
      if (!service.canManageImage(req.user, image)) return res.status(403).json({ ok: false, message: "Forbidden" });

      const payloadId = service.normalizePositiveInteger(req.body?.id);
      const payloadImageId = service.normalizePositiveInteger(req.body?.image_id ?? req.body?.imageId);
      if (payloadId && payloadId !== Number(beforeRow.id)) {
        return res.status(400).json({ ok: false, message: "Imageable id cannot be changed" });
      }
      if (payloadImageId && payloadImageId !== Number(beforeRow.image_id)) {
        return res.status(400).json({ ok: false, message: "image_id cannot be changed; create a new imageable instead" });
      }

      const imageableType = hasPayloadField(req.body, "imageable_type", "imageableType")
        ? service.normalizeNullableText(req.body?.imageable_type ?? req.body?.imageableType)
        : beforeRow.imageable_type;
      const imageableId = hasPayloadField(req.body, "imageable_id", "imageableId")
        ? service.normalizeNullableText(req.body?.imageable_id ?? req.body?.imageableId)
        : beforeRow.imageable_id;
      const role = hasPayloadField(req.body, "role")
        ? service.normalizeNullableText(req.body?.role)
        : beforeRow.role;
      const sortOrder = hasPayloadField(req.body, "sort_order", "sortOrder")
        ? (service.normalizeIntegerOrNull(req.body?.sort_order ?? req.body?.sortOrder) ?? 0)
        : beforeRow.sort_order;
      const cropJson = hasPayloadField(req.body, "crop_json", "cropJson")
        ? service.normalizeJsonText(req.body?.crop_json ?? req.body?.cropJson, "crop_json", null)
        : (beforeRow.crop_json === null ? null : service.safeStringifyJson(beforeRow.crop_json));
      if (!imageableType) return res.status(400).json({ ok: false, message: "imageable_type is required" });
      if (!imageableId) return res.status(400).json({ ok: false, message: "imageable_id is required" });

      await service.dbRunAsync(
        `
          UPDATE imageables
          SET
            imageable_type = ?,
            imageable_id = ?,
            role = ?,
            sort_order = ?,
            crop_json = ?,
            updated_at = CURRENT_TIMESTAMP
          WHERE id = ?
        `,
        [imageableType, imageableId, role, sortOrder, cropJson, beforeRow.id]
      );
      const updatedRow = await service.loadImageableById(beforeRow.id);
      const changes = buildAuditChanges(beforeRow, updatedRow, IMAGEABLE_AUDIT_FIELDS);
      if (!Object.keys(changes).length) return res.json({ ok: true, imageable: updatedRow || null });
      return logAuditEvent(
        {
          ...getAuditActor(req.user),
          event_type: "imageable.updated",
          entity_type: "imageable",
          action: "update",
          record_id: String(beforeRow.id),
          changes,
        },
        () => res.json({ ok: true, imageable: updatedRow || null })
      );
    } catch (error) {
      console.error("Failed to update imageable", error);
      return res.status(error.status || 500).json({ ok: false, message: error.status ? error.message : "Failed to update imageable" });
    }
  });

  app.delete("/imageables/:id", requireAuthenticated, async (req, res) => {
    try {
      const beforeRow = await service.loadImageableById(req.params.id);
      if (!beforeRow) return res.status(404).json({ ok: false, message: "Imageable not found" });
      const image = await service.loadImageByIdentifier(beforeRow.image_id, { includeDeleted: Number(req.user?.admin) === 1 });
      if (!service.canManageImage(req.user, image)) return res.status(403).json({ ok: false, message: "Forbidden" });
      await service.dbRunAsync("DELETE FROM imageables WHERE id = ?", [beforeRow.id]);
      return logAuditEvent(
        {
          ...getAuditActor(req.user),
          event_type: "imageable.deleted",
          entity_type: "imageable",
          action: "delete",
          record_id: String(beforeRow.id),
          changes: buildAuditDeletionChanges(beforeRow || {}, IMAGEABLE_AUDIT_FIELDS),
        },
        () => res.json({ ok: true })
      );
    } catch (error) {
      console.error("Failed to delete imageable", error);
      return res.status(500).json({ ok: false, message: "Failed to delete imageable" });
    }
  });

  app.get("/imageables", requireAuthenticated, async (req, res) => {
    try {
      const imageableType = service.normalizeNullableText(req.query?.imageable_type ?? req.query?.imageableType);
      const imageableId = service.normalizeNullableText(req.query?.imageable_id ?? req.query?.imageableId);
      if (!imageableType || !imageableId) {
        return res.status(400).json({ ok: false, message: "imageable_type and imageable_id are required" });
      }
      const filters = [
        "ia.imageable_type = ?",
        "ia.imageable_id = ?",
        "i.deleted_at IS NULL",
      ];
      const params = [imageableType, imageableId];
      if (Number(req.user?.admin) !== 1) {
        filters.push("(i.owner_user_id = ? OR i.uploaded_by_user_id = ? OR i.visibility = 'public')");
        params.push(Number(req.user.id), Number(req.user.id));
      }
      const rows = await service.dbAllAsync(
        `
          SELECT
            ia.id,
            ia.image_id,
            ia.imageable_type,
            ia.imageable_id,
            ia.role,
            ia.sort_order,
            ia.crop_json,
            ia.created_at,
            ia.updated_at
          FROM imageables ia
          INNER JOIN images i
            ON i.id = ia.image_id
          WHERE ${filters.join(" AND ")}
          ORDER BY ia.sort_order ASC, ia.id ASC
        `,
        params
      );
      return res.json({ ok: true, imageables: (rows || []).map(service.normalizeImageableRow) });
    } catch (error) {
      console.error("Failed to load imageables", error);
      return res.status(500).json({ ok: false, message: "Failed to load imageables" });
    }
  });

  app.get("/images/:id/variants", requireAuthenticated, async (req, res) => {
    try {
      const image = await service.loadImageByIdentifier(req.params.id);
      if (!image) return res.status(404).json({ ok: false, message: "Image not found" });
      if (!service.canAccessImage(req.user, image)) return res.status(403).json({ ok: false, message: "Forbidden" });
      const rows = await service.dbAllAsync(
        `
          SELECT
            id,
            image_id,
            variant,
            storage_path,
            public_url,
            mime_type,
            file_size_bytes,
            width,
            height,
            created_at,
            updated_at
          FROM image_variants
          WHERE image_id = ?
          ORDER BY id ASC
        `,
        [image.id]
      );
      return res.json({ ok: true, variants: rows || [] });
    } catch (error) {
      console.error("Failed to load image variants", error);
      return res.status(500).json({ ok: false, message: "Failed to load image variants" });
    }
  });

  app.post("/images/:id/variants", requireAuthenticated, async (req, res) => {
    try {
      const image = await service.loadImageByIdentifier(req.params.id);
      if (!image) return res.status(404).json({ ok: false, message: "Image not found" });
      if (!service.canManageImage(req.user, image)) return res.status(403).json({ ok: false, message: "Forbidden" });

      const variant = service.normalizeNullableText(req.body?.variant);
      const storagePath = service.normalizeNullableText(req.body?.storage_path ?? req.body?.storagePath);
      const publicUrl = service.normalizeNullableText(req.body?.public_url ?? req.body?.publicUrl);
      const mimeType = service.normalizeNullableText(req.body?.mime_type ?? req.body?.mimeType);
      const fileSizeBytes = service.normalizeImageDimension(req.body?.file_size_bytes ?? req.body?.fileSizeBytes, "file_size_bytes");
      const width = service.normalizeImageDimension(req.body?.width, "width");
      const height = service.normalizeImageDimension(req.body?.height, "height");

      if (!variant) return res.status(400).json({ ok: false, message: "variant is required" });
      if (!storagePath) return res.status(400).json({ ok: false, message: "storage_path is required" });
      if (mimeType && !mimeType.toLowerCase().startsWith("image/")) {
        return res.status(400).json({ ok: false, message: "mime_type must be an image type" });
      }

      const insertResult = await service.dbRunAsync(
        `
          INSERT INTO image_variants (
            image_id,
            variant,
            storage_path,
            public_url,
            mime_type,
            file_size_bytes,
            width,
            height,
            created_at,
            updated_at
          )
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        `,
        [image.id, variant, storagePath, publicUrl, mimeType, fileSizeBytes, width, height]
      );
      const createdRow = await service.loadImageVariantById(insertResult?.lastID);
      return logAuditEvent(
        {
          ...getAuditActor(req.user),
          event_type: "image_variant.created",
          entity_type: "image_variant",
          action: "create",
          record_id: String(insertResult?.lastID || ""),
          changes: buildAuditCreationChanges(createdRow || {}, IMAGE_VARIANT_AUDIT_FIELDS),
        },
        () => res.status(201).json({ ok: true, variant: createdRow || null })
      );
    } catch (error) {
      const isUniqueError = isImageVariantUniqueError(error);
      console.error("Failed to create image variant", error);
      return res.status(isUniqueError ? 409 : error.status || 500).json({
        ok: false,
        message: isUniqueError ? "variant already exists for this image" : error.status ? error.message : "Failed to create image variant",
      });
    }
  });

  app.get("/image-variants/:id", requireAuthenticated, async (req, res) => {
    try {
      const variant = await service.loadImageVariantById(req.params.id);
      if (!variant) return res.status(404).json({ ok: false, message: "Image variant not found" });
      const image = await service.loadImageByIdentifier(variant.image_id, {
        includeDeleted: Number(req.user?.admin) === 1,
      });
      if (!service.canAccessImage(req.user, image)) return res.status(403).json({ ok: false, message: "Forbidden" });
      return res.json({ ok: true, variant });
    } catch (error) {
      console.error("Failed to load image variant", error);
      return res.status(500).json({ ok: false, message: "Failed to load image variant" });
    }
  });

  app.patch("/image-variants/:id", requireAuthenticated, async (req, res) => {
    try {
      const beforeRow = await service.loadImageVariantById(req.params.id);
      if (!beforeRow) return res.status(404).json({ ok: false, message: "Image variant not found" });
      const image = await service.loadImageByIdentifier(beforeRow.image_id, { includeDeleted: Number(req.user?.admin) === 1 });
      if (!service.canManageImage(req.user, image)) return res.status(403).json({ ok: false, message: "Forbidden" });

      const payloadId = service.normalizePositiveInteger(req.body?.id);
      const payloadImageId = service.normalizePositiveInteger(req.body?.image_id ?? req.body?.imageId);
      if (payloadId && payloadId !== Number(beforeRow.id)) {
        return res.status(400).json({ ok: false, message: "Image variant id cannot be changed" });
      }
      if (payloadImageId && payloadImageId !== Number(beforeRow.image_id)) {
        return res.status(400).json({ ok: false, message: "image_id cannot be changed; create a new variant instead" });
      }

      const variant = hasPayloadField(req.body, "variant")
        ? service.normalizeNullableText(req.body?.variant)
        : beforeRow.variant;
      const storagePath = hasPayloadField(req.body, "storage_path", "storagePath")
        ? service.normalizeNullableText(req.body?.storage_path ?? req.body?.storagePath)
        : beforeRow.storage_path;
      const publicUrl = hasPayloadField(req.body, "public_url", "publicUrl")
        ? service.normalizeNullableText(req.body?.public_url ?? req.body?.publicUrl)
        : beforeRow.public_url;
      const mimeType = hasPayloadField(req.body, "mime_type", "mimeType")
        ? service.normalizeNullableText(req.body?.mime_type ?? req.body?.mimeType)
        : beforeRow.mime_type;
      const fileSizeBytes = hasPayloadField(req.body, "file_size_bytes", "fileSizeBytes")
        ? service.normalizeImageDimension(req.body?.file_size_bytes ?? req.body?.fileSizeBytes, "file_size_bytes")
        : beforeRow.file_size_bytes;
      const width = hasPayloadField(req.body, "width")
        ? service.normalizeImageDimension(req.body?.width, "width")
        : beforeRow.width;
      const height = hasPayloadField(req.body, "height")
        ? service.normalizeImageDimension(req.body?.height, "height")
        : beforeRow.height;
      if (!variant) return res.status(400).json({ ok: false, message: "variant is required" });
      if (!storagePath) return res.status(400).json({ ok: false, message: "storage_path is required" });
      if (mimeType && !mimeType.toLowerCase().startsWith("image/")) {
        return res.status(400).json({ ok: false, message: "mime_type must be an image type" });
      }

      await service.dbRunAsync(
        `
          UPDATE image_variants
          SET
            variant = ?,
            storage_path = ?,
            public_url = ?,
            mime_type = ?,
            file_size_bytes = ?,
            width = ?,
            height = ?,
            updated_at = CURRENT_TIMESTAMP
          WHERE id = ?
        `,
        [variant, storagePath, publicUrl, mimeType, fileSizeBytes, width, height, beforeRow.id]
      );
      const updatedRow = await service.loadImageVariantById(beforeRow.id);
      const changes = buildAuditChanges(beforeRow, updatedRow, IMAGE_VARIANT_AUDIT_FIELDS);
      if (!Object.keys(changes).length) return res.json({ ok: true, variant: updatedRow || null });
      return logAuditEvent(
        {
          ...getAuditActor(req.user),
          event_type: "image_variant.updated",
          entity_type: "image_variant",
          action: "update",
          record_id: String(beforeRow.id),
          changes,
        },
        () => res.json({ ok: true, variant: updatedRow || null })
      );
    } catch (error) {
      const isUniqueError = isImageVariantUniqueError(error);
      console.error("Failed to update image variant", error);
      return res.status(isUniqueError ? 409 : error.status || 500).json({
        ok: false,
        message: isUniqueError ? "variant already exists for this image" : error.status ? error.message : "Failed to update image variant",
      });
    }
  });

  app.delete("/image-variants/:id", requireAuthenticated, async (req, res) => {
    try {
      const beforeRow = await service.loadImageVariantById(req.params.id);
      if (!beforeRow) return res.status(404).json({ ok: false, message: "Image variant not found" });
      const image = await service.loadImageByIdentifier(beforeRow.image_id, { includeDeleted: Number(req.user?.admin) === 1 });
      if (!service.canManageImage(req.user, image)) return res.status(403).json({ ok: false, message: "Forbidden" });
      await service.dbRunAsync("DELETE FROM image_variants WHERE id = ?", [beforeRow.id]);
      return logAuditEvent(
        {
          ...getAuditActor(req.user),
          event_type: "image_variant.deleted",
          entity_type: "image_variant",
          action: "delete",
          record_id: String(beforeRow.id),
          changes: buildAuditDeletionChanges(beforeRow || {}, IMAGE_VARIANT_AUDIT_FIELDS),
        },
        () => res.json({ ok: true })
      );
    } catch (error) {
      console.error("Failed to delete image variant", error);
      return res.status(500).json({ ok: false, message: "Failed to delete image variant" });
    }
  });
}
