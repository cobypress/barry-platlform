// Repeated values held once (DECISIONS.md 346). JSON.parse makes a new string
// for every value it reads, so a big org's 500,000 stage changes held their
// stage names, close dates and opportunity ids half a million times over. A
// step's output is walked once when it's done, and each short or id-like
// value is swapped for the first copy seen. The values are the same, so
// nothing that reads them can tell; only the memory changes.

const SALESFORCE_ID = /^[A-Za-z0-9]{15}(?:[A-Za-z0-9]{3})?$/;

/**
 * Worth sharing: an id that refers to another record, or a short value such
 * as a stage, a status or a date. A record's own Id is never repeated, so
 * holding it in the table would cost room and save none.
 */
const worth = (key: string, value: string): boolean => key !== "Id" && (value.length <= 24 || SALESFORCE_ID.test(value));

/** Swaps each repeated value in `value` for one copy, in place, and returns it. The table goes when this returns. */
export function shareRepeatedValues<T>(value: T): T {
  const seen = new Map<string, string>();
  const share = (v: unknown, key: string): unknown => {
    if (typeof v === "string") {
      if (!worth(key, v)) return v;
      const first = seen.get(v);
      if (first !== undefined) return first;
      seen.set(v, v);
      return v;
    }
    if (Array.isArray(v)) {
      for (let i = 0; i < v.length; i++) v[i] = share(v[i], key);
    } else if (v !== null && typeof v === "object" && !Object.isFrozen(v)) {
      for (const k of Object.keys(v)) (v as Record<string, unknown>)[k] = share((v as Record<string, unknown>)[k], k);
    }
    return v;
  };
  return share(value, "") as T;
}
