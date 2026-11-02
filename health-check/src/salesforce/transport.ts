import { SF_DEFAULTS } from "../config.ts";

export type TransportRequest = {
  method: string;
  url: string;
  headers: Record<string, string>;
  body?: string;
};

export type TransportResponse = {
  status: number;
  header(name: string): string | null;
  text: string;
};

export interface Transport {
  send(request: TransportRequest): Promise<TransportResponse>;
}

// The only place in the service that touches the network. A source scan test
// holds that line. Redirects are errors so the bearer token can't follow one.
export const fetchTransport: Transport = {
  async send(request) {
    const res = await fetch(request.url, {
      method: request.method,
      headers: request.headers,
      body: request.body,
      redirect: "error",
      signal: AbortSignal.timeout(SF_DEFAULTS.requestTimeoutMs),
    });
    return { status: res.status, header: (name) => res.headers.get(name), text: await res.text() };
  },
};
