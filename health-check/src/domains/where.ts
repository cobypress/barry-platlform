// Where a rule's subject is in Setup, for the free scan's findings shown in
// full (DECISIONS.md 623). Each is the Lightning Setup menu's own labels, in
// order, without the three group headings (Administration, Platform Tools,
// Settings), which only fold the menu. A rule gets one only when its subject is
// in that one place in every org; anything spread over several pages, or kept
// outside Setup (reports, dashboards, CPQ's rules, articles), gets none, rather
// than a guess (rule 5). Labels only: never a URL, an id or a name the org chose.

export const WHERE = {
  workflowRules: ["Setup", "Process Automation", "Workflow Rules"],
  processBuilder: ["Setup", "Process Automation", "Process Builder"],
  flows: ["Setup", "Process Automation", "Flows"],
  apexClasses: ["Setup", "Custom Code", "Apex Classes"],
  apexTriggers: ["Setup", "Custom Code", "Apex Triggers"],
  storage: ["Setup", "Data", "Storage Usage"],
  companyInformation: ["Setup", "Company Settings", "Company Information"],
  installedPackages: ["Setup", "Apps", "Packaging", "Installed Packages"],
  users: ["Setup", "Users", "Users"],
  queues: ["Setup", "Users", "Queues"],
  securityHealthCheck: ["Setup", "Security", "Health Check"],
  sharingSettings: ["Setup", "Security", "Sharing Settings"],
  remoteSites: ["Setup", "Security", "Remote Site Settings"],
  setupAuditTrail: ["Setup", "Security", "View Setup Audit Trail"],
  connectedApps: ["Setup", "Apps", "Connected Apps", "Manage Connected Apps"],
  apexJobs: ["Setup", "Environments", "Jobs", "Apex Jobs"],
  scheduledJobs: ["Setup", "Environments", "Jobs", "Scheduled Jobs"],
  objectManager: ["Setup", "Object Manager"],
  escalationRules: ["Setup", "Feature Settings", "Service", "Escalation Rules"],
  campaignInfluence: ["Setup", "Feature Settings", "Marketing", "Campaign Influence"],
} as const satisfies Record<string, readonly string[]>;
