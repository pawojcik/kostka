// Every engine there is. The places that make a Database (src/node.ts on Node, the
// workbench's Web Worker in the browser) give it these; the shared layers never
// import an engine.

import { Engines } from "../execution/registry.ts";
import { kostka } from "./kostka/engine.ts";
import { spark } from "./spark/engine.ts";

export const ENGINES = new Engines([kostka, spark]);
