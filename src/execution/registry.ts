// The engines a Database can run queries on, by name. The set is given when the
// Database is made (src/node.ts and the workbench's worker give every engine there
// is), so nothing in the shared layers imports an engine.

import { UnknownEngineError } from "../core/errors.ts";
import type { Engine } from "./engine.ts";

export class Engines {
  private readonly byName = new Map<string, Engine>();
  /** The engine a query runs on when it names none. */
  readonly defaultEngine: Engine;

  /** `list` must not be empty; its first engine is the default. */
  constructor(list: readonly Engine[]) {
    const first = list[0];
    if (!first) throw new RangeError("a Database needs at least one engine");
    for (const e of list) {
      if (this.byName.has(e.name)) throw new RangeError(`two engines are named ${e.name}: give each its own name`);
      this.byName.set(e.name, e);
    }
    this.defaultEngine = first;
  }

  /** The engine called `name` (default: the default engine). */
  get(name?: string): Engine {
    if (name === undefined) return this.defaultEngine;
    const e = this.byName.get(name);
    if (!e) throw new UnknownEngineError(`there is no engine called ${JSON.stringify(name)}: use one of ${this.names().join(", ")}`);
    return e;
  }

  names(): string[] {
    return [...this.byName.keys()];
  }

  all(): Engine[] {
    return [...this.byName.values()];
  }
}
