import { createSign } from "node:crypto";

// Google Drive, for the documents of a released run. A service account signs
// in (a JWT bearer grant, as barry-platlform worker/funnels.ts does for
// Calendar) and works only in the folders it has been given. A service account
// has no storage of its own, so the top folder must be in a shared drive, or
// HC_DRIVE_IMPERSONATE names a user to act as (domain-wide delegation).
//
// Plain fetch, no googleapis, like the rest of the engine. Uploads are
// resumable, which has no 5 MB limit, and a file uploaded again under the
// same name keeps its id, so a link someone already has keeps working.

export type DriveConfig = {
  clientEmail: string;
  privateKey: string;
  /** The top "Health checks" folder. */
  rootFolderId: string;
  /** A user to act as, when the folder isn't in a shared drive. */
  subject: string | null;
};

export type DriveItem = { id: string; name: string; url: string; /** When Drive says it was created, if it said. */ createdAt?: string };

/** What the documents job needs from Drive, so tests can stand in for it. */
export interface DocumentDrive {
  readonly rootFolderId: string;
  /** A folder by id, or null if it's gone or in the bin. */
  folder(id: string): Promise<DriveItem | null>;
  folders(parentId: string): Promise<DriveItem[]>;
  files(parentId: string): Promise<DriveItem[]>;
  createFolder(parentId: string, name: string): Promise<DriveItem>;
  /** Uploads a file, or new contents for `replaceId`. */
  putFile(parentId: string, name: string, mimeType: string, body: Buffer, replaceId?: string): Promise<DriveItem>;
  /** Moves a file to another folder. Its id, and so any link to it, stays the same. */
  moveFile(id: string, fromParentId: string, toParentId: string): Promise<void>;
}

export class DriveError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "DriveError";
  }
}

const API = "https://www.googleapis.com/drive/v3/files";
const UPLOAD = "https://www.googleapis.com/upload/drive/v3/files";
const TOKEN_URL = "https://oauth2.googleapis.com/token";
const FOLDER = "application/vnd.google-apps.folder";
const FIELDS = "id,name,webViewLink,createdTime";
// Shared drives need these on every call; they do nothing elsewhere.
const ALL_DRIVES = "supportsAllDrives=true";
// Drive ids are letters, digits, - and _. Checked before one goes into a query.
const DRIVE_ID = /^[A-Za-z0-9_-]{10,200}$/;

type DriveFile = { id: string; name: string; webViewLink: string; createdTime?: string };
const item = (f: DriveFile): DriveItem => ({ id: f.id, name: f.name, url: f.webViewLink, ...(f.createdTime ? { createdAt: f.createdTime } : {}) });

const base64url = (input: string | Buffer): string => Buffer.from(input).toString("base64url");

const GOOGLE_HOSTS: ReadonlySet<string> = new Set(["www.googleapis.com", "oauth2.googleapis.com"]);

type Fetch = typeof fetch;

export class GoogleDrive implements DocumentDrive {
  readonly rootFolderId: string;
  readonly #config: DriveConfig;
  readonly #fetch: Fetch;
  #token: { value: string; expiresAt: number } | null = null;

  constructor(config: DriveConfig, fetchImpl: Fetch = fetch) {
    if (!DRIVE_ID.test(config.rootFolderId)) throw new DriveError("HC_DRIVE_FOLDER_ID isn't a Drive folder id");
    this.rootFolderId = config.rootFolderId;
    this.#config = config;
    this.#fetch = fetchImpl;
  }

  async #accessToken(): Promise<string> {
    if (this.#token && this.#token.expiresAt > Date.now() + 60_000) return this.#token.value;
    const now = Math.floor(Date.now() / 1000);
    const claims: Record<string, unknown> = { iss: this.#config.clientEmail, scope: "https://www.googleapis.com/auth/drive", aud: TOKEN_URL, iat: now, exp: now + 3600 };
    if (this.#config.subject) claims.sub = this.#config.subject;
    const unsigned = `${base64url(JSON.stringify({ alg: "RS256", typ: "JWT" }))}.${base64url(JSON.stringify(claims))}`;
    let signature: Buffer;
    try {
      signature = createSign("RSA-SHA256").update(unsigned).sign(this.#config.privateKey);
    } catch {
      throw new DriveError("HC_DRIVE_SA_KEY isn't a private key the worker can sign with");
    }
    const res = await this.#send(TOKEN_URL, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({ grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer", assertion: `${unsigned}.${base64url(signature)}` }),
    });
    const body = (await res.json().catch(() => ({}))) as { access_token?: string; expires_in?: number; error?: string; error_description?: string };
    if (!res.ok || !body.access_token) throw new DriveError(`Google refused the service account's sign-in (${res.status}): ${body.error_description || body.error || "no reason given"}`);
    this.#token = { value: body.access_token, expiresAt: Date.now() + (body.expires_in ?? 3600) * 1000 };
    return body.access_token;
  }

  /**
   * The only way out. The engine's rule is that only the Salesforce transport
   * touches the network (src/salesforce/__tests__/client.test.ts, "source
   * scan"); this is the one exception, and it reaches Google's hosts only, so
   * it can never be a way to write to a customer's org (DECISIONS 75).
   */
  #send(url: string, init: RequestInit): Promise<Response> {
    if (!GOOGLE_HOSTS.has(new URL(url).host) || !url.startsWith("https://")) throw new DriveError("The Drive client only talks to Google");
    return this.#fetch(url, init);
  }

  async #call(url: string, init: RequestInit = {}): Promise<Response> {
    const res = await this.#send(url, { ...init, headers: { ...(init.headers as Record<string, string>), Authorization: `Bearer ${await this.#accessToken()}` } });
    if (!res.ok && res.status !== 404) {
      const body = (await res.json().catch(() => ({}))) as { error?: { message?: string } };
      throw new DriveError(`Google Drive refused a request (${res.status}): ${body.error?.message ?? "no reason given"}`);
    }
    return res;
  }

  async #json<T>(url: string, init?: RequestInit, parentId?: string): Promise<T> {
    const res = await this.#call(url, init);
    if (res.status === 404) {
      // Google answers "not found" for a folder that exists but wasn't shared, so say how to share it.
      if (parentId === this.rootFolderId) {
        throw new DriveError(`The service account can't see the Health checks folder (HC_DRIVE_FOLDER_ID). Add ${this.#config.clientEmail} to its shared drive as a Content manager`);
      }
      throw new DriveError(`Google Drive couldn't find a folder the worker needed. Check ${this.#config.clientEmail} can see it`);
    }
    return (await res.json()) as T;
  }

  async folder(id: string): Promise<DriveItem | null> {
    if (!DRIVE_ID.test(id)) return null;
    const res = await this.#call(`${API}/${id}?${ALL_DRIVES}&fields=${FIELDS},trashed,mimeType`);
    if (res.status === 404) return null;
    const f = (await res.json()) as { id: string; name: string; webViewLink: string; trashed?: boolean; mimeType?: string };
    return f.trashed || f.mimeType !== FOLDER ? null : { id: f.id, name: f.name, url: f.webViewLink };
  }

  async #children(parentId: string, folders: boolean): Promise<DriveItem[]> {
    if (!DRIVE_ID.test(parentId)) throw new DriveError("That isn't a Drive folder id");
    const items: DriveItem[] = [];
    let page: string | undefined;
    do {
      const params = new URLSearchParams({
        q: `'${parentId}' in parents and trashed = false and mimeType ${folders ? "=" : "!="} '${FOLDER}'`,
        fields: `nextPageToken,files(${FIELDS})`,
        pageSize: "1000",
        corpora: "allDrives",
        includeItemsFromAllDrives: "true",
        supportsAllDrives: "true",
        ...(page ? { pageToken: page } : {}),
      });
      const body = await this.#json<{ files: DriveFile[]; nextPageToken?: string }>(`${API}?${params}`, undefined, parentId);
      items.push(...body.files.map(item));
      page = body.nextPageToken;
    } while (page);
    return items;
  }

  folders(parentId: string): Promise<DriveItem[]> {
    return this.#children(parentId, true);
  }

  files(parentId: string): Promise<DriveItem[]> {
    return this.#children(parentId, false);
  }

  async createFolder(parentId: string, name: string): Promise<DriveItem> {
    const f = await this.#json<DriveFile>(`${API}?${ALL_DRIVES}&fields=${FIELDS}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name, mimeType: FOLDER, parents: [parentId] }),
    });
    return item(f);
  }

  async putFile(parentId: string, name: string, mimeType: string, body: Buffer, replaceId?: string): Promise<DriveItem> {
    if (!DRIVE_ID.test(parentId) || (replaceId !== undefined && !DRIVE_ID.test(replaceId))) throw new DriveError("That isn't a Drive file id");
    // A resumable session: the metadata first, then the bytes to the address Google gives back.
    // The fields asked for here come back with the last part of the upload.
    const query = `uploadType=resumable&${ALL_DRIVES}&fields=${FIELDS}`;
    const start = await this.#call(replaceId ? `${UPLOAD}/${replaceId}?${query}` : `${UPLOAD}?${query}`, {
      method: replaceId ? "PATCH" : "POST",
      headers: { "Content-Type": "application/json; charset=UTF-8", "X-Upload-Content-Type": mimeType, "X-Upload-Content-Length": String(body.length) },
      body: JSON.stringify(replaceId ? { name } : { name, parents: [parentId] }),
    });
    const session = start.headers.get("location");
    if (start.status === 404 || !session) throw new DriveError(`Google Drive didn't start the upload of ${name}`);
    // The access token goes with the bytes, so only to Google's upload host.
    if (!session.startsWith(`${UPLOAD}?`) && !session.startsWith(`${UPLOAD}/`)) throw new DriveError(`Google Drive gave an upload address the worker doesn't trust, for ${name}`);
    const done = await this.#call(session, { method: "PUT", headers: { "Content-Type": mimeType }, body: new Uint8Array(body) });
    if (done.status === 404) throw new DriveError(`Google Drive lost the upload of ${name}`);
    return item((await done.json()) as DriveFile);
  }

  async moveFile(id: string, fromParentId: string, toParentId: string): Promise<void> {
    if (![id, fromParentId, toParentId].every((x) => DRIVE_ID.test(x))) throw new DriveError("That isn't a Drive file id");
    await this.#json(`${API}/${id}?${ALL_DRIVES}&addParents=${toParentId}&removeParents=${fromParentId}&fields=id`, { method: "PATCH", headers: { "Content-Type": "application/json" }, body: "{}" });
  }
}
