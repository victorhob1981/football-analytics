import type { ReactNode } from "react";

import { ClubComparisonPanel } from "@/features/clubs/components";
import { GlobalErrorBoundary } from "@/shared/components/feedback/GlobalErrorBoundary";
import { TopNavBar } from "@/shared/components/navigation/TopNavBar";

import { PlatformProviders } from "./providers";

type PlatformLayoutProps = {
  children: ReactNode;
};

export default function PlatformLayout({ children }: PlatformLayoutProps) {
  return (
    <PlatformProviders>
      <div>
        <TopNavBar />
        <GlobalErrorBoundary>
          <main>{children}</main>
          <ClubComparisonPanel />
        </GlobalErrorBoundary>
      </div>
    </PlatformProviders>
  );
}
