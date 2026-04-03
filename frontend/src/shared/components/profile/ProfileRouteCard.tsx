import Link from "next/link";

function joinClasses(...classes: Array<string | false | null | undefined>) {
  return classes.filter(Boolean).join(" ");
}

export function ProfileRouteCard({
  className,
  description,
  href,
  label,
  title,
}: {
  className?: string;
  description: string;
  href: string;
  label: string;
  title: string;
}) {
  return (
    <Link
      className={joinClasses(
        "rounded-[1.35rem] border border-[rgba(191,201,195,0.55)] bg-white/88 px-4 py-4 transition-[transform,border-color,background-color,box-shadow] duration-200 ease-[cubic-bezier(0.23,1,0.32,1)] hover:-translate-y-0.5 hover:border-[#8bd6b6] hover:bg-white hover:shadow-[0_22px_56px_-42px_rgba(17,28,45,0.2)] active:scale-[0.985]",
        className,
      )}
      href={href}
    >
      <p className="text-[0.68rem] font-semibold uppercase tracking-[0.16em] text-[#57657a]">
        {label}
      </p>
      <h3 className="mt-3 font-[family:var(--font-profile-headline)] text-[1.35rem] font-extrabold tracking-[-0.035em] text-[#111c2d]">
        {title}
      </h3>
      <p className="mt-2 text-sm/6 text-[#57657a]">{description}</p>
      <div className="mt-4 inline-flex items-center gap-2 text-[0.72rem] font-semibold uppercase tracking-[0.16em] text-[#003526]">
        <span>Abrir</span>
        <span aria-hidden="true">+</span>
      </div>
    </Link>
  );
}
