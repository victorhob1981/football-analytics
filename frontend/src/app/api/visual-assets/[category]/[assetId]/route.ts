import { promises as fs } from "fs";
import path from "path";

import { NextResponse, type NextRequest } from "next/server";

type ManifestEntry = {
  entity_id?: number;
  local_path?: string;
  content_type?: string;
};

type ManifestFile = {
  entries?: ManifestEntry[];
};

const ALLOWED_CATEGORIES = new Set(["clubs", "competitions", "players"]);
type CachedManifest = {
  entries: ManifestEntry[];
  mtimeMs: number;
};

const manifestCache = new Map<string, CachedManifest>();

function resolveManifestPath(category: string): string {
  return path.resolve(
    process.cwd(),
    "..",
    "data",
    "visual_assets",
    "manifests",
    `${category}.json`,
  );
}

async function loadManifestEntries(category: string): Promise<ManifestEntry[]> {
  const manifestPath = resolveManifestPath(category);
  const manifestStat = await fs.stat(manifestPath);
  const cached = manifestCache.get(category);

  if (cached && cached.mtimeMs === manifestStat.mtimeMs) {
    return cached.entries;
  }

  const rawManifest = await fs.readFile(manifestPath, "utf8");
  const parsedManifest = JSON.parse(rawManifest) as ManifestFile;
  const entries = Array.isArray(parsedManifest.entries) ? parsedManifest.entries : [];

  manifestCache.set(category, {
    entries,
    mtimeMs: manifestStat.mtimeMs,
  });
  return entries;
}

export async function GET(
  _request: NextRequest,
  context: { params: Promise<{ category: string; assetId: string }> },
) {
  const { category, assetId } = await context.params;

  if (!ALLOWED_CATEGORIES.has(category) || !/^\d+$/.test(assetId)) {
    return NextResponse.json({ message: "Asset não encontrado." }, { status: 404 });
  }

  const entries = await loadManifestEntries(category);
  const entry = entries.find(
    (candidate) =>
      typeof candidate.entity_id === "number" &&
      String(candidate.entity_id) === assetId &&
      typeof candidate.local_path === "string",
  );

  if (!entry?.local_path) {
    return NextResponse.json({ message: "Asset não encontrado." }, { status: 404 });
  }

  const assetPath = path.resolve(process.cwd(), "..", entry.local_path);
  const buffer = await fs.readFile(assetPath);

  return new NextResponse(buffer, {
    headers: {
      "Cache-Control": "public, max-age=86400, immutable",
      "Content-Type": entry.content_type ?? "image/png",
    },
  });
}
