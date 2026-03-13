"use client";

import { Clock, Construction } from "lucide-react";

type ComingSoonPageProps = {
    title: string;
    description: string;
    icon?: React.ElementType;
};

export function ComingSoonPage({ title, description, icon: Icon = Construction }: ComingSoonPageProps) {
    return (
        <div className="home-gradient-bg -mx-4 -mt-4 px-4 pb-8 pt-6 md:-mx-6 md:-mt-6 md:px-6 md:pt-8">
            <div className="mx-auto max-w-3xl space-y-6">
                <header>
                    <h1 className="text-2xl font-semibold tracking-tight text-slate-100">{title}</h1>
                    <p className="mt-1 text-sm text-slate-400">{description}</p>
                </header>

                <section className="flex flex-col items-center rounded-xl border border-slate-700/80 bg-slate-900/80 px-8 py-16 shadow-lg shadow-slate-950/30 backdrop-blur-sm">
                    <div className="mb-6 rounded-2xl border border-emerald-500/20 bg-emerald-500/10 p-5">
                        <Icon className="h-12 w-12 text-emerald-400" />
                    </div>

                    <h2 className="text-xl font-bold text-slate-100">Em breve</h2>
                    <p className="mt-2 max-w-md text-center text-sm text-slate-400">
                        Esta funcionalidade está sendo desenvolvida e será disponibilizada em uma próxima atualização.
                    </p>

                    <div className="mt-6 flex items-center gap-2 rounded-full border border-slate-700 bg-slate-800/60 px-4 py-2">
                        <Clock className="h-4 w-4 text-amber-400" />
                        <span className="text-xs font-medium text-slate-300">Previsão: quando a ingestão de dados estiver completa</span>
                    </div>
                </section>
            </div>
        </div>
    );
}
