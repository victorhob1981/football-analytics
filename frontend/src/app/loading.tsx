import { PlatformStateSurface } from "@/shared/components/feedback/PlatformStateSurface";

export default function RootLoading() {
  return (
    <PlatformStateSurface
      description="Estamos preparando a aplicação e conectando a navegação principal."
      kicker="Aplicação"
      loading
      title="Preparando a aplicação"
    />
  );
}
