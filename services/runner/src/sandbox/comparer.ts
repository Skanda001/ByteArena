/**
 * Output comparison rule per AGENTS.md:
 * Compare judge output to expected output after:
 * 1. normalising \r\n to \n
 * 2. trimming trailing whitespace on every line
 * 3. ignoring trailing blank lines
 * Everything else must match exactly.
 */
export function normalizeOutput(output: string): string {
  const unix = output.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  const lines = unix.split("\n").map((line) => line.trimEnd());
  while (lines.length > 0 && lines[lines.length - 1] === "") {
    lines.pop();
  }
  return lines.join("\n");
}

export function compareOutput(actual: string, expected: string): boolean {
  return normalizeOutput(actual) === normalizeOutput(expected);
}
