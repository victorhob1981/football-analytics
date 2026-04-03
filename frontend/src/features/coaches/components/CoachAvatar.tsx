import Image from "next/image";

type CoachAvatarProps = {
  coachName: string;
  photoUrl?: string | null;
  hasRealPhoto: boolean;
  size?: "card" | "profile";
};

const SIZE_CONFIG = {
  card: {
    className: "h-14 w-14 text-[0.62rem]",
    pixels: 56,
  },
  profile: {
    className: "h-20 w-20 text-[0.72rem]",
    pixels: 80,
  },
} as const;

export function CoachAvatar({
  coachName,
  photoUrl,
  hasRealPhoto,
  size = "card",
}: CoachAvatarProps) {
  const { className: sizeClassName, pixels } = SIZE_CONFIG[size];

  if (hasRealPhoto && photoUrl) {
    return (
      <div
        className={`${sizeClassName} relative shrink-0 overflow-hidden rounded-full border border-white/20 shadow-[0_16px_34px_-24px_rgba(17,28,45,0.42)]`}
      >
        <Image
          alt={`Foto de ${coachName}`}
          className="object-cover"
          height={pixels}
          referrerPolicy="no-referrer"
          src={photoUrl}
          unoptimized
          width={pixels}
        />
      </div>
    );
  }

  return (
    <div
      aria-label={`Foto indisponivel de ${coachName}`}
      className={`${sizeClassName} flex shrink-0 items-center justify-center rounded-full border border-[rgba(191,201,195,0.7)] bg-[linear-gradient(135deg,rgba(235,241,252,0.96),rgba(245,247,250,0.96))] px-2 text-center font-semibold uppercase tracking-[0.16em] text-[#57657a]`}
      role="img"
    >
      <span>Sem foto</span>
    </div>
  );
}
