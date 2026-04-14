import { NextResponse, type NextRequest } from "next/server";

const HOP_BY_HOP_HEADERS = new Set([
  "connection",
  "content-encoding",
  "content-length",
  "host",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
]);

function resolveBffOrigin(): string | null {
  const origin = process.env.FOOTBALL_BFF_ORIGIN?.trim() ?? process.env.BFF_ORIGIN?.trim();
  return origin && /^https?:\/\//i.test(origin) ? origin : null;
}

function buildTargetUrl(origin: string, pathSegments: string[], search: string): string {
  const baseUrl = origin.endsWith("/") ? origin : `${origin}/`;
  const relativePath = pathSegments.map((segment) => encodeURIComponent(segment)).join("/");
  const targetUrl = new URL(relativePath, baseUrl);
  targetUrl.search = search;
  return targetUrl.toString();
}

function buildProxyHeaders(request: NextRequest): Headers {
  const headers = new Headers();
  request.headers.forEach((value, key) => {
    if (!HOP_BY_HOP_HEADERS.has(key.toLowerCase())) {
      headers.set(key, value);
    }
  });
  return headers;
}

function buildResponseHeaders(response: Response): Headers {
  const headers = new Headers();
  response.headers.forEach((value, key) => {
    if (!HOP_BY_HOP_HEADERS.has(key.toLowerCase())) {
      headers.set(key, value);
    }
  });
  headers.set("Cache-Control", "no-store");
  return headers;
}

async function proxyBffRequest(
  request: NextRequest,
  context: { params: Promise<{ path?: string[] }> },
) {
  const origin = resolveBffOrigin();
  if (!origin) {
    return NextResponse.json({ message: "BFF origin não configurado." }, { status: 500 });
  }

  const { path = [] } = await context.params;
  const targetUrl = buildTargetUrl(origin, path, request.nextUrl.search);
  const method = request.method.toUpperCase();
  const body = method === "GET" || method === "HEAD" ? undefined : await request.arrayBuffer();

  const upstreamResponse = await fetch(targetUrl, {
    method,
    headers: buildProxyHeaders(request),
    body,
    redirect: "manual",
    cache: "no-store",
  });

  return new NextResponse(upstreamResponse.body, {
    status: upstreamResponse.status,
    headers: buildResponseHeaders(upstreamResponse),
  });
}

export const GET = proxyBffRequest;
export const HEAD = proxyBffRequest;
export const POST = proxyBffRequest;
export const PUT = proxyBffRequest;
export const PATCH = proxyBffRequest;
export const DELETE = proxyBffRequest;
