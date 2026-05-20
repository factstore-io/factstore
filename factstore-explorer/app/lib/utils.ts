export function fmtDate(iso: string): string {
  return iso.replace("T", " ").slice(0, 19);
}

export function fmtN(n: number): string {
  return n.toLocaleString();
}

export function trunc(uuid: string, len = 8): string {
  return uuid.slice(0, len) + "…";
}

const typeColorMap = new Map<string, number>();
let colorCounter = 0;

export function getTypeClass(type: string): string {
  if (!typeColorMap.has(type)) {
    typeColorMap.set(type, colorCounter++ % 4);
  }
  return "t" + typeColorMap.get(type);
}
