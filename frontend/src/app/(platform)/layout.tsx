import Link from "next/link";
import { Suspense, type ReactNode } from "react";

import { PlayerComparisonPanel } from "@/features/players/components/PlayerComparisonPanel";
import { GlobalErrorBoundary } from "@/shared/components/feedback/GlobalErrorBoundary";
import { GlobalFilterBar } from "@/shared/components/filters/GlobalFilterBar";

import { PlatformProviders } from "./providers";

type PlatformLayoutProps = {
  children: ReactNode;
};

const platformNavLinks = [
  { href: "/", label: "Home" },
  { href: "/competition/placeholder", label: "Competition" },
  { href: "/matches", label: "Matches" },
  { href: "/clubs", label: "Clubs" },
  { href: "/players", label: "Players" },
  { href: "/rankings/placeholder", label: "Rankings" },
  { href: "/market", label: "Market" },
  { href: "/head-to-head", label: "Head-to-head" },
  { href: "/coaches/placeholder", label: "Coaches" },
  { href: "/audit", label: "Audit" },
];

export default function PlatformLayout({ children }: PlatformLayoutProps) {
  return (
    <PlatformProviders>
      <div>
        <header>
          <h1>Football Analytics Platform</h1>
          <nav aria-label="Navegacao principal da plataforma">
            <ul>
              {platformNavLinks.map((item) => (
                <li key={item.href}>
                  <Link href={item.href}>{item.label}</Link>
                </li>
              ))}
            </ul>
          </nav>
        </header>

        <section aria-label="Area reservada para filtros globais">
          <Suspense fallback={<p>Loading GlobalFilterBar...</p>}>
            <GlobalFilterBar />
          </Suspense>
        </section>

        <GlobalErrorBoundary>
          <main>{children}</main>
          <PlayerComparisonPanel />
        </GlobalErrorBoundary>
      </div>
    </PlatformProviders>
  );
}
