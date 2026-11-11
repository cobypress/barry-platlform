// For the tests: handbook.json as engines before decisions 482 and 484 wrote it.

/** A file's flows and validation rules as a version 4 or older file has them: no last change on any, and no word of them on the part. Nor, as before version 6, which reading made it (decision 484). */
export function olderComponents<T extends Record<string, unknown>>(file: T): T {
  const older = (part: { status: string; items?: Array<Record<string, unknown>>; lastChanges?: string }) => {
    if (part.status !== "ok") return part;
    const { lastChanges: _said, ...rest } = part;
    return { ...rest, items: part.items!.map(({ lastChanged: _gone, ...item }) => item) };
  };
  return { ...withoutReadings(file), flows: older(file.flows as never), validationRules: older(file.validationRules as never) };
}

/** A file as one from before version 6 has it: no word of which reading made it (decision 484). */
export function withoutReadings<T extends Record<string, unknown>>(file: T): T {
  const { readBy: _none, ...rest } = file;
  return rest as T;
}
