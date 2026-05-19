import { PlatformStateSurface } from "@/shared/components/feedback/PlatformStateSurface";

export default function PlatformHomeLoading() {
  return (
    <PlatformStateSurface
      description="Estamos montando a visão inicial com os blocos editoriais e atalhos principais do produto."
      kicker="Home"
      loading
      title="Preparando a home"
    />
  );
}
