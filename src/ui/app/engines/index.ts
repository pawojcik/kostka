// The views of every engine, by name: the query tab picks them by `state.engine`.

import type { EngineViews } from "./views.ts";
import { KOSTKA_VIEWS } from "./kostka/views.tsx";
import { SPARK_VIEWS } from "./spark/views.tsx";

const VIEWS: Record<string, EngineViews> = { kostka: KOSTKA_VIEWS, spark: SPARK_VIEWS };

export function viewsOf(engine: string): EngineViews {
  const v = VIEWS[engine];
  if (!v) throw new Error(`the workbench has no views for the engine ${JSON.stringify(engine)}`);
  return v;
}
