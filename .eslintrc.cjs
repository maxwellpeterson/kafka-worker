module.exports = {
  extends: [
    "plugin:@typescript-eslint/recommended",
    "plugin:@typescript-eslint/recommended-requiring-type-checking",
    "plugin:@typescript-eslint/strict",
    "plugin:import/recommended",
    "plugin:import/typescript",
    "prettier",
  ],
  parser: "@typescript-eslint/parser",
  parserOptions: {
    tsconfigRootDir: __dirname,
    project: ["./tsconfig.json", "./test/tsconfig.json"],
  },
  plugins: ["@typescript-eslint", "import"],
  root: true,
  ignorePatterns: [".eslintrc.js", "jest.config.js", "build.js"],
  settings: {
    "import/resolver": {
      typescript: true,
      node: true,
    },
  },
  rules: {
    // Sort import statements by module name
    "import/order": [
      "warn",
      {
        groups: ["external", "internal"],
        alphabetize: {
          order: "asc",
        },
      },
    ],
    // Sort imported members within the same import statement by name
    "sort-imports": [
      "warn",
      {
        ignoreDeclarationSort: true,
      },
    ],
  },
};
