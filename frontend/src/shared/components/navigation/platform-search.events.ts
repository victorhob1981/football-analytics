"use client";

export const PLATFORM_SEARCH_OPEN_EVENT = "platform-search-open";

export function dispatchPlatformSearchOpen(): void {
  if (typeof window === "undefined") {
    return;
  }

  window.dispatchEvent(new Event(PLATFORM_SEARCH_OPEN_EVENT));
}
