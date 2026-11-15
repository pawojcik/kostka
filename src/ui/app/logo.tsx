// The logo: a scrambled Rubik's cube (kostka, "a small cube") in isometric view.
// Each visible face is a 3×3 grid of stickers, drawn as parallelograms on a dark
// body; the side faces are shaded a little so the cube reads as 3D.
//
// With `solving`, it is the loading animation: the stickers turn to their face's
// color in the order a beginner solves a cube (the top center, its edges, its
// corners, then the side faces layer by layer), stay solved for a moment, and
// scramble again. Every wait in the workbench shows it (see Spinner).

import type { ComponentChildren } from "preact";

type P = [number, number];

const W = "#f4f4f4", Y = "#ffd500", R = "#c41e3a", O = "#ff6a13", B = "#0051ba", G = "#009e60";

// Stickers row by row from the face's origin corner, mixed up as after a scramble,
// and the face's color when solved.
const FACES: { origin: P; u: P; v: P; shade: number; colors: string[]; solved: string }[] = [
  // top: from the left corner, u toward the back corner, v toward the front corner
  { origin: [2.5, 6.5], u: [9.5, -5.5], v: [9.5, 5.5], shade: 1, colors: [W, R, B, G, W, Y, O, B, W], solved: W },
  // left: u along the top edge to the front, v down
  { origin: [2.5, 6.5], u: [9.5, 5.5], v: [0, 11], shade: 0.86, colors: [G, O, R, B, G, W, Y, R, G], solved: G },
  // right: from the front corner, u along the top edge to the back, v down
  { origin: [12, 12], u: [9.5, -5.5], v: [0, 11], shade: 0.72, colors: [R, Y, Y, O, R, B, W, G, O], solved: R },
];

/** When each sticker gets solved, 0 first: the top face center, edges, corners; then
 * the side faces' rows from the top, alternating left and right. */
function rank(face: number, k: number): number {
  if (face === 0) return [5, 1, 6, 2, 0, 3, 7, 4, 8][k]!;
  const row = Math.floor(k / 3), col = k % 3;
  return 9 + row * 6 + col * 2 + (face - 1);
}

const GAP = 0.1; // of a sticker's size, on each side
const STEP_S = 0.06; // seconds between two stickers getting solved

function sticker(o: P, u: P, v: P, i: number, j: number): string {
  const at = (a: number, b: number) => `${(o[0] + (u[0] * a + v[0] * b) / 3).toFixed(2)},${(o[1] + (u[1] * a + v[1] * b) / 3).toFixed(2)}`;
  const a0 = i + GAP, a1 = i + 1 - GAP, b0 = j + GAP, b1 = j + 1 - GAP;
  return `${at(a0, b0)} ${at(a1, b0)} ${at(a1, b1)} ${at(a0, b1)}`;
}

export function Logo({ size = 22, solving = false, label }: { size?: number; solving?: boolean; label?: string }) {
  return (
    <svg class={`logo ${solving ? "solving" : ""}`} width={size} height={size} viewBox="0 0 24 24"
      role={label ? "img" : undefined} aria-label={label} aria-hidden={label ? undefined : "true"}>
      <polygon points="12,1 21.5,6.5 21.5,17.5 12,23 2.5,17.5 2.5,6.5" fill="#1b1d21" stroke="#1b1d21" stroke-width="1.2" stroke-linejoin="round" />
      {FACES.map((f, fi) => (
        <g style={f.shade < 1 ? `filter:brightness(${f.shade})` : undefined}>
          {f.colors.map((c, k) => (
            <polygon points={sticker(f.origin, f.u, f.v, k % 3, Math.floor(k / 3))} fill={c}
              style={solving ? `--from:${c};--to:${f.solved};animation-delay:${(rank(fi, k) * STEP_S).toFixed(2)}s` : undefined} />
          ))}
        </g>
      ))}
    </svg>
  );
}

/** A wait: the cube solving itself, with what is being waited for. */
export function Spinner({ size = 18, children, class: cls = "" }: { size?: number; children?: ComponentChildren; class?: string }) {
  return (
    <span class={`spinner ${cls}`} role="status">
      <Logo size={size} solving />
      {children && <span>{children}</span>}
    </span>
  );
}
