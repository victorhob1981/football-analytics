"use client";

import { Users } from "lucide-react";

import { ComingSoonPage } from "@/shared/components/feedback/ComingSoonPage";

export default function CoachesPage() {
    return (
        <ComingSoonPage
            description="Ranking e análise de técnicos por aproveitamento, gols por jogo e vitórias"
            icon={Users}
            title="Técnicos"
        />
    );
}
