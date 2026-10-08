const CODES: Record<string, string> = {
  yellow: '\x1b[33m', green: '\x1b[32m', cyan: '\x1b[36m', magenta: '\x1b[35m',
  red: '\x1b[31m', gray: '\x1b[90m', bold: '\x1b[1m', reset: '\x1b[0m',
};

export const paint = (color: string, s: string) => `${CODES[color] ?? ''}${s}${CODES.reset}`;

export function makeLogger(name: string, color = 'green') {
  const prefix = paint(color, `[${name}]`.padEnd(11));
  return {
    info: (msg: string) => console.log(`${prefix} ${msg}`),
    warn: (msg: string) => console.log(`${prefix} ${paint('yellow', msg)}`),
    error: (msg: string) => console.log(`${prefix} ${paint('red', msg)}`),
  };
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export const money = (cents: number) =>
  (cents < 0 ? '-' : '') + '$' + (Math.abs(cents) / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
