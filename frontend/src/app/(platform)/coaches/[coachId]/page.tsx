import { CoachProfileContent } from "@/features/coaches";

type CoachPageProps = {
  params: Promise<{ coachId: string }>;
};

export default async function CoachPage({ params }: CoachPageProps) {
  const { coachId } = await params;

  return <CoachProfileContent coachId={coachId} />;
}
