"use client";

import { ChevronLeft, ChevronRight } from "lucide-react";

interface RoundNavigatorProps {
    currentRound: number;
    totalRounds: number;
    selectedRound: number;
    onSelect: (round: number) => void;
}

export function RoundNavigator({
    currentRound,
    totalRounds,
    selectedRound,
    onSelect,
}: RoundNavigatorProps) {
    const rounds = Array.from({ length: totalRounds }, (_, i) => i + 1);

    return (
        <div className="space-y-3">
            {/* Controles prev/next + display */}
            <div className="flex items-center gap-3">
                <button
                    aria-label="Rodada anterior"
                    className="flex h-8 w-8 items-center justify-center rounded-lg border border-slate-600 bg-slate-800 text-slate-300 transition-colors hover:border-emerald-500/60 hover:text-emerald-300 disabled:opacity-40"
                    disabled={selectedRound <= 1}
                    onClick={() => { onSelect(selectedRound - 1); }}
                    type="button"
                >
                    <ChevronLeft className="h-4 w-4" />
                </button>

                <div className="flex-1 text-center">
                    <p className="text-base font-semibold text-slate-100">Rodada {selectedRound}</p>
                    {selectedRound === currentRound && (
                        <p className="text-[10px] font-medium text-emerald-400">Rodada atual</p>
                    )}
                    {selectedRound < currentRound && (
                        <p className="text-[10px] text-slate-500">Histórico</p>
                    )}
                    {selectedRound > currentRound && (
                        <p className="text-[10px] text-amber-400">Futura</p>
                    )}
                </div>

                <button
                    aria-label="Próxima rodada"
                    className="flex h-8 w-8 items-center justify-center rounded-lg border border-slate-600 bg-slate-800 text-slate-300 transition-colors hover:border-emerald-500/60 hover:text-emerald-300 disabled:opacity-40"
                    disabled={selectedRound >= totalRounds}
                    onClick={() => { onSelect(selectedRound + 1); }}
                    type="button"
                >
                    <ChevronRight className="h-4 w-4" />
                </button>
            </div>

            {/* Pills de rodada (scroll horizontal) */}
            <div className="flex gap-1.5 overflow-x-auto pb-1">
                {rounds.map((round) => (
                    <button
                        className={`shrink-0 rounded-md px-2.5 py-1 text-xs font-medium transition-colors ${round === selectedRound
                                ? "bg-emerald-500/20 text-emerald-300 border border-emerald-500/40"
                                : round === currentRound
                                    ? "border border-amber-500/40 bg-amber-500/10 text-amber-300"
                                    : round < currentRound
                                        ? "border border-slate-700 bg-slate-800/60 text-slate-400 hover:border-slate-600 hover:text-slate-300"
                                        : "border border-slate-700/40 bg-slate-800/30 text-slate-600 hover:text-slate-400"
                            }`}
                        key={round}
                        onClick={() => { onSelect(round); }}
                        type="button"
                    >
                        {round}
                    </button>
                ))}
            </div>
        </div>
    );
}
