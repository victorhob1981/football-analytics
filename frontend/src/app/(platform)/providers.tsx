"use client";

import { QueryClientProvider } from "@tanstack/react-query";
import { useState, type ReactNode } from "react";

import { createQueryClient } from "@/shared/services/query-client";

type ProvidersProps = {
  children: ReactNode;
};

export function PlatformProviders({ children }: ProvidersProps) {
  const [queryClient] = useState(() => createQueryClient());

  return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
}

