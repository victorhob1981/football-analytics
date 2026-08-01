export interface CatalogScope {
  kind: "archive" | "filtered";
  label: string;
  isExhaustive: false;
  updatedAt?: string | null;
}
