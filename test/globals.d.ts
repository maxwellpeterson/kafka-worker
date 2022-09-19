import { Env } from "src/common";

declare global {
  function getMiniflareBindings(): Env;
}
