import type { ReactNode } from "react";

import { PlatformProviders } from "./providers";
import { PlatformShell } from "./PlatformShell";

type PlatformLayoutProps = {
  children: ReactNode;
};

export default function PlatformLayout({ children }: PlatformLayoutProps) {
  return (
    <PlatformProviders>
      <PlatformShell>{children}</PlatformShell>
    </PlatformProviders>
  );
}
