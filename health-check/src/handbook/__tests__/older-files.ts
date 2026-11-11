// For the tests: handbook.json as engines before decision 482 wrote it.

/** A file's flows and validation rules as a version 4 or older file has them: no last change on any, and no word of them on the part. */
export function olderComponents<T extends Record<string, unknown>>(file: T): T {
  const older = (part: { status: string; items?: Array<Record<string, unknown>>; lastChanges?: string }) => {
    if (part.status !== "ok") return part;
    const { lastChanges: _said, ...rest } = part;
    return { ...rest, items: part.items!.map(({ lastChanged: _gone, ...item }) => item) };
  };
  return { ...file, flows: older(file.flows as never), validationRules: older(file.validationRules as never) };
}
