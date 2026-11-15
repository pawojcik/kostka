// The SQL editor: CodeMirror 6 with kostka's keywords, completion of tables and
// columns from the catalog (and functions), and squiggles under parse and analysis
// errors, which the worker finds by analyzing the text as it is typed.

import { autocompletion, closeBrackets, completionKeymap, type Completion, type CompletionContext } from "@codemirror/autocomplete";
import { defaultKeymap, history, historyKeymap, indentWithTab } from "@codemirror/commands";
import { sql, StandardSQL, type SQLNamespace } from "@codemirror/lang-sql";
import { bracketMatching, syntaxHighlighting } from "@codemirror/language";
import { classHighlighter } from "@lezer/highlight";
import { linter, lintGutter, type Diagnostic } from "@codemirror/lint";
import { Compartment, EditorState } from "@codemirror/state";
import { EditorView, keymap, lineNumbers, placeholder as ph } from "@codemirror/view";

export interface Catalog {
  tables: Record<string, { name: string; type: string }[]>;
  functions: { name: string; signature: string; doc: string; kind: string }[];
}

export interface EditorOptions {
  doc: string;
  catalog: Catalog;
  onChange?: (text: string) => void;
  onRun?: () => void;
  check?: (text: string) => Promise<{ from: number; to: number; message: string }[]>;
  placeholder?: string;
  readOnly?: boolean;
}

function namespace(c: Catalog): SQLNamespace {
  return Object.fromEntries(Object.entries(c.tables).map(([t, cols]) => [t, cols.map((col): Completion => ({ label: col.name, type: "property", detail: col.type }))]));
}

function functionSource(c: Catalog) {
  const options: Completion[] = c.functions.map((f) => ({ label: f.name, type: f.kind === "aggregate" ? "method" : "function", detail: f.signature, info: f.doc, apply: `${f.name}(`, boost: -1 }));
  return (ctx: CompletionContext) => {
    const word = ctx.matchBefore(/\w+/);
    if (!word || (word.from === word.to && !ctx.explicit)) return null;
    if (ctx.state.sliceDoc(word.from - 1, word.from) === ".") return null;
    return { from: word.from, options, validFor: /^\w*$/ };
  };
}

const theme = EditorView.theme({
  "&": { fontSize: "12.5px", border: "1px solid var(--line-strong)", borderRadius: "6px", background: "var(--surface)" },
  "&.cm-focused": { outline: "2px solid var(--accent)", outlineOffset: "1px" },
  ".cm-content": { fontFamily: "var(--mono)", minHeight: "120px", caretColor: "var(--ink)" },
  ".cm-gutters": { background: "var(--surface-2)", color: "var(--faint)", border: "0", borderRadius: "6px 0 0 6px" },
  ".cm-tooltip": { background: "var(--surface)", color: "var(--ink)", border: "1px solid var(--line-strong)" },
  ".cm-tooltip-autocomplete ul li[aria-selected]": { background: "var(--accent)", color: "#fff" },
  ".cm-completionDetail": { opacity: ".7", fontStyle: "normal", marginLeft: "8px" },
});

export class SqlEditor {
  readonly view: EditorView;
  private readonly lang = new Compartment();
  private readonly ro = new Compartment();
  private catalog: Catalog;

  constructor(parent: HTMLElement, o: EditorOptions) {
    this.catalog = o.catalog;
    const lint = o.check
      ? [linter(async (view) => (await o.check!(view.state.doc.toString())).map((d): Diagnostic => ({ from: Math.min(d.from, view.state.doc.length), to: Math.min(d.to, view.state.doc.length), severity: "error", message: d.message })), { delay: 400 }), lintGutter()]
      : [];
    this.view = new EditorView({
      parent,
      state: EditorState.create({
        doc: o.doc,
        extensions: [
          lineNumbers(), history(), bracketMatching(), closeBrackets(), syntaxHighlighting(classHighlighter),
          this.lang.of(this.language()),
          autocompletion({ activateOnTyping: true }),
          ...lint,
          keymap.of([{ key: "Mod-Enter", run: () => (o.onRun?.(), true) }, ...completionKeymap, ...defaultKeymap, ...historyKeymap, indentWithTab]),
          EditorView.updateListener.of((u) => u.docChanged && o.onChange?.(u.state.doc.toString())),
          EditorView.lineWrapping, theme, ph(o.placeholder ?? ""),
          this.ro.of(EditorState.readOnly.of(!!o.readOnly)),
        ],
      }),
    });
  }

  private language() {
    const lang = sql({ dialect: StandardSQL, schema: namespace(this.catalog), upperCaseKeywords: false });
    return [lang, lang.language.data.of({ autocomplete: functionSource(this.catalog) })];
  }

  setCatalog(c: Catalog): void {
    this.catalog = c;
    this.view.dispatch({ effects: this.lang.reconfigure(this.language()) });
  }

  setReadOnly(on: boolean): void {
    this.view.dispatch({ effects: this.ro.reconfigure(EditorState.readOnly.of(on)) });
  }

  get text(): string {
    return this.view.state.doc.toString();
  }

  set text(t: string) {
    if (t !== this.text) this.view.dispatch({ changes: { from: 0, to: this.view.state.doc.length, insert: t } });
  }

  destroy(): void {
    this.view.destroy();
  }
}
