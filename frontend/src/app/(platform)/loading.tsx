import { PlatformStateSurface } from "@/shared/components/feedback/PlatformStateSurface";

export default function PlatformLoading() {
  return (
    <PlatformStateSurface
      description="Estamos abrindo a próxima área e preservando o recorte atual da navegação."
      loading
      title="Preparando a próxima área"
    />
  );
}
