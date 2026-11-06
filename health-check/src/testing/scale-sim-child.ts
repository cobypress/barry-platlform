// The scale test's simulator, in a process of its own (docs/overnight/night-3/
// scale.md), so the engine's memory can be measured without the generated
// org's. The parent sends its options once, then each request; this answers
// each as the simulator would.

import type { TransportRequest } from "../salesforce/transport.ts";
import { buildScaleOrg, FIVE_HUNDRED_USERS, scaled } from "./scale-org.ts";
import { Simulator } from "./simulator.ts";

export type ChildStart = { kind: "start"; factor: number; pageSize: number; dailyLimit: number; used: number };
export type ChildRequest = { kind: "request"; id: number; request: TransportRequest };
export type ChildReply = { kind: "ready"; counts: Record<string, number>; generateMs: number; rssMb: number } | { kind: "reply"; id: number; status: number; text: string; limitInfo: string | null } | { kind: "memory"; rssMb: number };

let simulator: Simulator | null = null;
const send = (message: ChildReply): void => void process.send!(message);

process.on("message", async (message: ChildStart | ChildRequest | { kind: "memory" }) => {
  if (message.kind === "start") {
    const started = performance.now();
    const records = buildScaleOrg(scaled(FIVE_HUNDRED_USERS, message.factor));
    simulator = new Simulator({ records, pageSize: message.pageSize, dailyLimit: message.dailyLimit, used: message.used });
    const counts = {
      users: records.users.length,
      opportunities: records.pipeline.opportunities.length,
      opportunityHistory: records.pipeline.history.length,
      leads: records.leadFunnel.leads.length,
      tasks: records.activity.tasks.length,
      events: records.activity.events.length,
      cases: records.supportCases.cases.length,
      quotes: records.cpqQuotes.length,
      forecastFacts: records.forecastFacts.length,
      logins: records.loginHistory.length,
    };
    send({ kind: "ready", counts, generateMs: performance.now() - started, rssMb: Math.round(process.memoryUsage().rss / 1048576) });
  } else if (message.kind === "request") {
    const res = await simulator!.send(message.request);
    send({ kind: "reply", id: message.id, status: res.status, text: res.text, limitInfo: res.header("sforce-limit-info") });
  } else {
    send({ kind: "memory", rssMb: Math.round(process.memoryUsage().rss / 1048576) });
  }
});
