import { NextResponse, type NextRequest } from "next/server";

function resolvePublicImagesBaseUrl(): string | null {
  const baseUrl =
    process.env.FOOTBALL_PUBLIC_IMAGES_BASE_URL?.trim() ??
    process.env.PUBLIC_IMAGES_BASE_URL?.trim();

  return baseUrl && /^https?:\/\//i.test(baseUrl) ? baseUrl : null;
}

function normalizePathSegments(pathSegments: string[]): string | null {
  if (pathSegments.length === 0 || pathSegments.some((segment) => segment === "" || segment === "..")) {
    return null;
  }

  return pathSegments.map((segment) => encodeURIComponent(segment)).join("/");
}

export async function GET(
  _request: NextRequest,
  context: { params: Promise<{ path?: string[] }> },
) {
  const baseUrl = resolvePublicImagesBaseUrl();
  const { path = [] } = await context.params;
  const relativePath = normalizePathSegments(path);

  if (!baseUrl || !relativePath) {
    return NextResponse.json({ message: "Imagem não encontrada." }, { status: 404 });
  }

  const upstreamUrl = new URL(relativePath, baseUrl.endsWith("/") ? baseUrl : `${baseUrl}/`);
  const upstreamResponse = await fetch(upstreamUrl, {
    headers: { Accept: "image/*,*/*;q=0.8" },
    cache: "force-cache",
    next: { revalidate: 86_400 },
  });

  if (!upstreamResponse.ok) {
    return NextResponse.json({ message: "Imagem não encontrada." }, { status: upstreamResponse.status });
  }

  const headers = new Headers();
  headers.set("Cache-Control", "public, max-age=86400, immutable");
  headers.set("Content-Type", upstreamResponse.headers.get("Content-Type") ?? "image/jpeg");

  return new NextResponse(upstreamResponse.body, {
    status: upstreamResponse.status,
    headers,
  });
}
