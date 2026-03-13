import type { Config } from "tailwindcss";

const config: Config = {
  content: ["./src/app/**/*.{js,ts,jsx,tsx,mdx}", "./src/features/**/*.{js,ts,jsx,tsx,mdx}", "./src/shared/**/*.{js,ts,jsx,tsx,mdx}", "./src/config/**/*.{js,ts,jsx,tsx,mdx}"],
  theme: {
    extend: {
      fontFamily: {
        sans: ["'Segoe UI'", "system-ui", "sans-serif"],
      },
      boxShadow: {
        platform: "0 12px 32px -16px rgb(15 23 42 / 0.35)",
      },
    },
  },
  plugins: [],
};

export default config;
