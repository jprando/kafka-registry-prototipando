export function somarQuantidades(acc: number, p: any): number {
  const _acc = acc + p.quantidade;
  p.quantidade = 0;
  return _acc;
}
