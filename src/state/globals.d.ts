import { Env } from "src/common";

// TODO: This needs to get scoped to test files only (same with Jest)!
// Also, need to figure out how to force:
// import { jest } from "@jest/globals";
// Also, can we ban importing from test files into non-test files with tsconfig?
declare global {
  function getMiniflareBindings(): Env;
}
