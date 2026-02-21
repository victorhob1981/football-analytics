import type { QueryKey } from "@tanstack/react-query";

export type QueryKeyPart = string | number | boolean | object | null;

/**
 * Guideline:
 * Feature query keys devem seguir o padrao ['domain', 'action', ...parts].
 * Cada feature deve manter seu proprio queryKeys.ts e usar este helper.
 */
export function buildQueryKey(
  domain: string,
  action: string,
  ...parts: Array<QueryKeyPart | undefined>
): QueryKey {
  return [domain, action, ...parts.filter((part) => part !== undefined)];
}
