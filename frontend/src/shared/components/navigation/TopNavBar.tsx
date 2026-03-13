"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useState } from "react";

const NAV_LINKS = [
    { label: "Home", href: "/" },
    { label: "Competição", href: "/competition" },
    { label: "Partidas", href: "/matches" },
    { label: "Clubes", href: "/clubs" },
    { label: "Jogadores", href: "/players" },
    { label: "Rankings", href: "/rankings" },
    { label: "Head-to-Head", href: "/head-to-head" },
    { label: "Mercado", href: "/market" },
    { label: "Técnicos", href: "/coaches" },
] as const;

export function TopNavBar() {
    const pathname = usePathname();
    const [mobileOpen, setMobileOpen] = useState(false);

    function isActive(href: string) {
        if (href === "/") return pathname === "/";
        return pathname.startsWith(href);
    }

    return (
        <header className="sticky top-0 z-50 w-full border-b border-slate-700/70 bg-slate-900/95 shadow-lg shadow-slate-950/40 backdrop-blur-sm">
            <div className="mx-auto flex max-w-[1400px] items-center justify-between px-4 md:px-6">
                {/* Logo */}
                <Link
                    className="flex items-center gap-2 py-3 no-underline"
                    href="/"
                >
                    <span className="flex h-7 w-7 items-center justify-center rounded-md bg-emerald-500 text-sm font-black text-white shadow">
                        ⚽
                    </span>
                    <span className="hidden text-sm font-bold tracking-tight text-slate-100 sm:block">
                        Football<span className="text-emerald-400">Analytics</span>
                    </span>
                </Link>

                {/* Links desktop */}
                <nav aria-label="Navegação principal" className="hidden md:flex">
                    <ul className="flex items-center gap-0.5">
                        {NAV_LINKS.map((link) => (
                            <li key={link.href}>
                                <Link
                                    className={`relative block rounded-md px-3 py-2 text-xs font-medium no-underline transition-colors ${isActive(link.href)
                                            ? "bg-emerald-500/15 text-emerald-400 hover:text-emerald-300"
                                            : "text-slate-300 hover:bg-slate-800 hover:text-slate-100"
                                        }`}
                                    href={link.href}
                                >
                                    {link.label}
                                    {isActive(link.href) && (
                                        <span className="absolute inset-x-2 -bottom-px h-0.5 rounded-full bg-emerald-400" />
                                    )}
                                </Link>
                            </li>
                        ))}
                    </ul>
                </nav>

                {/* Botão hamburguer mobile */}
                <button
                    aria-controls="top-nav-mobile-menu"
                    aria-expanded={mobileOpen}
                    aria-label={mobileOpen ? "Fechar menu" : "Abrir menu"}
                    className="rounded-md p-2 text-slate-400 transition-colors hover:bg-slate-800 hover:text-slate-100 md:hidden"
                    onClick={() => setMobileOpen((prev) => !prev)}
                    type="button"
                >
                    {mobileOpen ? (
                        <svg
                            aria-hidden="true"
                            fill="none"
                            height={20}
                            stroke="currentColor"
                            strokeWidth={2}
                            viewBox="0 0 24 24"
                            width={20}
                        >
                            <path d="M6 18L18 6M6 6l12 12" strokeLinecap="round" strokeLinejoin="round" />
                        </svg>
                    ) : (
                        <svg
                            aria-hidden="true"
                            fill="none"
                            height={20}
                            stroke="currentColor"
                            strokeWidth={2}
                            viewBox="0 0 24 24"
                            width={20}
                        >
                            <path d="M4 6h16M4 12h16M4 18h16" strokeLinecap="round" strokeLinejoin="round" />
                        </svg>
                    )}
                </button>
            </div>

            {/* Menu mobile */}
            {mobileOpen && (
                <nav
                    aria-label="Navegação principal (mobile)"
                    className="border-t border-slate-700/50 bg-slate-900 md:hidden"
                    id="top-nav-mobile-menu"
                >
                    <ul className="flex flex-col px-4 py-2">
                        {NAV_LINKS.map((link) => (
                            <li key={link.href}>
                                <Link
                                    className={`flex items-center gap-2 rounded-md px-3 py-2.5 text-sm font-medium no-underline transition-colors ${isActive(link.href)
                                            ? "bg-emerald-500/15 text-emerald-400"
                                            : "text-slate-300 hover:bg-slate-800 hover:text-slate-100"
                                        }`}
                                    href={link.href}
                                    onClick={() => setMobileOpen(false)}
                                >
                                    {isActive(link.href) && (
                                        <span className="h-1.5 w-1.5 rounded-full bg-emerald-400" />
                                    )}
                                    {link.label}
                                </Link>
                            </li>
                        ))}
                    </ul>
                </nav>
            )}
        </header>
    );
}
