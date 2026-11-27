// The settings of the simulated cluster a query runs on, and the IO profiles it
// prices IO with. Every engine runs on the same cluster:
//
// - Nodes: `nodes` simulated machines, each with `cores` cores (node.ts). An engine
//   places its own workers on them.
// - CPU time is measured (the time the engine's code actually ran, with
//   performance.now()) or, with cpu: "model", computed from the cost model (core/cost.ts),
//   which makes every run take the same simulated time.
// - IO time is computed from bytes and an IOProfile: shared table storage (read by
//   scans), each node's local disk (spilling) and the network (between nodes, and to
//   the scheduler).
// - Memory and disk: each node has a capacity of each (memory.ts decides what
//   spills, and fails the unit when something doesn't fit).

export interface IOProfile {
  readonly name: string;
  readonly label: string;
  readonly storageMBs: number;
  readonly storageLatencyMs: number;
  readonly diskMBs: number;
  readonly diskLatencyMs: number;
  readonly networkMBs: number;
  readonly networkLatencyMs: number;
}

export type Device = "storage" | "disk" | "network";

/** Simulated ns to move `bytes` on a device in one request. */
export function ioCost(p: IOProfile, device: Device, bytes: number): number {
  const mbs = p[`${device}MBs`], latency = p[`${device}LatencyMs`];
  return Math.round(latency * 1e6 + (mbs ? (bytes / (mbs * 1e6)) * 1e9 : 0));
}

export const IO_PROFILES: Record<string, IOProfile> = Object.fromEntries([
  { name: "cloud", label: "Cloud: object storage 200 MB/s, SSD 500 MB/s, 1 Gbit/s network", storageMBs: 200, storageLatencyMs: 5, diskMBs: 500, diskLatencyMs: 0.1, networkMBs: 125, networkLatencyMs: 0.5 },
  { name: "fast", label: "Fast: local NVMe storage and disks, 10 Gbit/s network", storageMBs: 2000, storageLatencyMs: 0.2, diskMBs: 3000, diskLatencyMs: 0.02, networkMBs: 1250, networkLatencyMs: 0.05 },
  { name: "slow", label: "Slow: remote storage 50 MB/s, HDD 150 MB/s, 100 Mbit/s network", storageMBs: 50, storageLatencyMs: 20, diskMBs: 150, diskLatencyMs: 5, networkMBs: 12.5, networkLatencyMs: 1 },
  { name: "none", label: "No IO cost: CPU time only", storageMBs: 0, storageLatencyMs: 0, diskMBs: 0, diskLatencyMs: 0, networkMBs: 0, networkLatencyMs: 0 },
].map((p) => [p.name, p]));

export interface ClusterConfig {
  /** Simulated machines. */
  readonly nodes: number;
  /** Cores per node: each runs one unit of work at a time, on its own clock. */
  readonly cores: number;
  /** Each node's memory. */
  readonly memoryMB: number;
  /** Each node's local disk, where state waiting for its readers spills. */
  readonly diskMB: number;
  readonly io: string;
  readonly batchRows: number;
  /** A partition with more rows is cut into several splits (where allowed). */
  readonly splitRows: number;
  /** Where CPU time comes from: measured as the operators run, or the cost model. */
  readonly cpu: CpuMode;
}

export type CpuMode = "measured" | "model";
export const CPU_MODES: readonly { name: CpuMode; label: string; doc: string }[] = [
  { name: "measured", label: "Measured", doc: "CPU time is the time each operator's code actually took in this browser (performance.now()). It varies between runs, and so may the schedule." },
  { name: "model", label: "Cost model", doc: "CPU time is computed from rows and a per-row cost of each kind of work, so every run of a query takes the same simulated time and has the same schedule." },
];

export const NODE_COUNTS = [1, 2, 4, 8] as const;
export const MEMORY_OPTIONS_MB = [0.25, 0.5, 1, 2, 4, 8, 16, 32, 64, 128, 256] as const;
export const DISK_OPTIONS_MB = [1, 2, 4, 8, 16, 32, 64, 128, 256, 1024] as const;
export const BATCH_SIZES = [1024, 4096, 8192, 16384, 65536] as const;
export const DEFAULT_CLUSTER: ClusterConfig = { nodes: 1, cores: 1, memoryMB: 2, diskMB: 16, io: "cloud", batchRows: 4096, splitRows: 65536, cpu: "measured" };

/** Cluster settings as a query gives them: any of them, and `workers`, the name
 * `nodes` had before nodes had cores (kostka runs one worker per node). */
export type ClusterInput = Partial<ClusterConfig> & { readonly workers?: number };

/** The cluster a query runs on: the defaults, overridden by `input`, checked. */
export function clusterConfig(input: ClusterInput = {}): ClusterConfig {
  const { workers, ...rest } = input;
  if (workers !== undefined && rest.nodes !== undefined && workers !== rest.nodes) {
    throw new RangeError(`workers (${workers}) and nodes (${rest.nodes}) disagree: workers is the old name of nodes, so give only nodes`);
  }
  return checkCluster({ ...DEFAULT_CLUSTER, ...(workers === undefined ? {} : { nodes: workers }), ...rest });
}

export function checkCluster(c: ClusterConfig): ClusterConfig {
  const fail = (m: string) => {
    throw new RangeError(m);
  };
  if (!Number.isInteger(c.nodes) || c.nodes < 1 || c.nodes > 64) fail(`nodes must be 1 to 64, got ${c.nodes}`);
  if (!Number.isInteger(c.cores) || c.cores < 1 || c.cores > 64) fail(`cores must be 1 to 64, got ${c.cores}`);
  if (!(c.memoryMB > 0)) fail(`memory must be positive, got ${c.memoryMB}`);
  if (!(c.diskMB > 0)) fail(`disk must be positive, got ${c.diskMB}`);
  if (!IO_PROFILES[c.io]) fail(`io must be one of ${Object.keys(IO_PROFILES).join(", ")}, got ${c.io}`);
  if (!Number.isInteger(c.batchRows) || c.batchRows < 1) fail(`batch rows must be a positive int, got ${c.batchRows}`);
  if (!Number.isInteger(c.splitRows) || c.splitRows < 1) fail(`split rows must be a positive int, got ${c.splitRows}`);
  if (!CPU_MODES.some((m) => m.name === c.cpu)) fail(`cpu must be one of ${CPU_MODES.map((m) => m.name).join(", ")}, got ${c.cpu}`);
  return c;
}
