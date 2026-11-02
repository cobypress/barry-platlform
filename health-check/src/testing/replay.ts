import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import type { Transport, TransportRequest, TransportResponse } from "../salesforce/transport.ts";
import type { ConnectionProvider, OrgConnection } from "../salesforce/connection.ts";
import { Calibration, ManualAnswer } from "../schemas/index.ts";
import { ORG_ID } from "./synthetic-org.ts";
import { z } from "zod";

// Tests run from recordings, so no org is needed. A request with no recording
// fails loudly, which means a changed query can't quietly test against old data.

export type RecordedExchange = { key: string; status: number; limitInfo: string | null; text: string };
export type Recording = { note: string; exchanges: RecordedExchange[] };

export const FIXTURE_DIR = fileURLToPath(new URL("../../fixtures/synthetic-org/", import.meta.url));

/** Identifies a request without the access token, which is in SOAP bodies. */
export function requestKey(request: TransportRequest): string {
  const url = new URL(request.url);
  const path = `${url.pathname}${url.search}`;
  if (path.includes("/Soap/m/")) return `SOAP ${request.headers.SOAPAction?.replace(/"/g, "")}`;
  if (request.method === "POST") return `POST ${path} ${createHash("sha256").update(request.body ?? "").digest("hex").slice(0, 16)}`;
  return `GET ${path}`;
}

export class RecordingTransport implements Transport {
  readonly exchanges: RecordedExchange[] = [];
  readonly #inner: Transport;

  constructor(inner: Transport) {
    this.#inner = inner;
  }

  async send(request: TransportRequest): Promise<TransportResponse> {
    const response = await this.#inner.send(request);
    this.exchanges.push({ key: requestKey(request), status: response.status, limitInfo: response.header("sforce-limit-info"), text: response.text });
    return response;
  }
}

export class ReplayTransport implements Transport {
  readonly #queues = new Map<string, RecordedExchange[]>();
  readonly requests: TransportRequest[] = [];

  constructor(recording: Recording) {
    for (const exchange of recording.exchanges) this.#queues.set(exchange.key, [...(this.#queues.get(exchange.key) ?? []), exchange]);
  }

  async send(request: TransportRequest): Promise<TransportResponse> {
    this.requests.push(request);
    const key = requestKey(request);
    const queue = this.#queues.get(key);
    if (!queue) throw new Error(`No recording for: ${key.slice(0, 200)}. Run npm run fixtures if the query changed on purpose.`);
    // Repeated calls step through the recording, then keep the last answer.
    const exchange = queue.length > 1 ? queue.shift()! : queue[0]!;
    return { status: exchange.status, text: exchange.text, header: (name) => (name.toLowerCase() === "sforce-limit-info" ? exchange.limitInfo : null) };
  }
}

const SYNTHETIC_CONNECTION: OrgConnection = { instanceUrl: "https://synthetic.example.com", accessToken: "synthetic-token-not-real", orgId: ORG_ID };

export const syntheticProvider: ConnectionProvider = {
  kind: "sf-cli",
  connect: async () => SYNTHETIC_CONNECTION,
  refresh: async () => SYNTHETIC_CONNECTION,
};

export const loadRecording = (): Recording => JSON.parse(readFileSync(`${FIXTURE_DIR}recording.json`, "utf8")) as Recording;
export const loadCalibration = (): Calibration => Calibration.parse(JSON.parse(readFileSync(`${FIXTURE_DIR}calibration.json`, "utf8")));
export const loadAnswers = (): ManualAnswer[] => z.array(ManualAnswer).parse(JSON.parse(readFileSync(`${FIXTURE_DIR}answers.json`, "utf8")));
