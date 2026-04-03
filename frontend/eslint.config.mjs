import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";

import { FlatCompat } from "@eslint/eslintrc";

const require = createRequire(import.meta.url);
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const compat = new FlatCompat({
  baseDirectory: __dirname,
});

const boundariesPlugin = require("eslint-plugin-boundaries");
const importPlugin = require("eslint-plugin-import");

export default [
  ...compat.extends("next/core-web-vitals", "next/typescript", "prettier"),
  {
    files: ["src/**/*.{js,jsx,ts,tsx}"],
    plugins: {
      boundaries: boundariesPlugin,
      import: importPlugin,
    },
    settings: {
      "import/resolver": {
        typescript: {
          project: "./tsconfig.json",
        },
        node: {
          extensions: [".js", ".jsx", ".ts", ".tsx"],
        },
      },
      "boundaries/elements": [
        {
          type: "feature",
          pattern: "src/features/*",
          capture: ["featureName"],
        },
        {
          type: "shared",
          pattern: "src/shared",
          mode: "folder",
        },
        {
          type: "app",
          pattern: "src/app",
          mode: "folder",
        },
        {
          type: "config",
          pattern: "src/config",
          mode: "folder",
        },
        {
          type: "lib",
          pattern: "src/lib",
          mode: "folder",
        },
      ],
    },
    rules: {
      "boundaries/element-types": [
        "error",
        {
          default: "allow",
          rules: [
            {
              from: "feature",
              disallow: ["feature"],
              message: "Features nao podem importar outras features diretamente.",
            },
            {
              from: [["feature", { featureName: "*" }]],
              allow: [["feature", { featureName: "${from.featureName}" }]],
            },
            {
              from: "shared",
              disallow: ["feature"],
              message: "Shared nao pode importar features.",
            },
          ],
        },
      ],
      "import/no-unresolved": [
        "error",
        {
          commonjs: true,
          caseSensitive: true,
        },
      ],
    },
  },
  {
    files: ["src/**/services/**/*.{ts,tsx}"],
    rules: {
      "no-restricted-imports": [
        "error",
        {
          patterns: [
            {
              group: ["**/components/**", "@/shared/components/**", "@/features/*/components/**"],
              message: "Services nao podem importar components.",
            },
          ],
        },
      ],
    },
  },
  {
    files: ["src/**/stores/**/*.{ts,tsx}"],
    rules: {
      "no-restricted-imports": [
        "error",
        {
          patterns: [
            {
              group: ["**/components/**", "@/shared/components/**", "@/features/*/components/**"],
              message: "Stores nao podem importar components.",
            },
          ],
        },
      ],
    },
  },
];
