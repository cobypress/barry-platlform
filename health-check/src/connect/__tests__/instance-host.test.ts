import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { isMyDomainHost, MY_DOMAIN_HOST, myDomainHost } from "../instance-host.ts";

// The org's own address, kept on a used connect link for the portal's links
// into Setup (DECISIONS.md 626). Only a My Domain host, bare and in lower case.

describe("the org's My Domain host (decision 626)", () => {
  test("is the console's check constraint word for word, so the constraint never refuses a host we write", () => {
    // blackcloud-next migrations/20261128_health_check_run_score_and_host.sql, as the portal design's 3.2 gives it.
    assert.equal(MY_DOMAIN_HOST.source, String.raw`^[a-z0-9-]+(\.[a-z0-9-]+)*\.my\.salesforce\.com$`);
    assert.equal(MY_DOMAIN_HOST.flags, "");
  });

  for (const [url, host] of [
    ["https://acme.my.salesforce.com", "acme.my.salesforce.com"],
    ["https://ACME-Group.My.Salesforce.com", "acme-group.my.salesforce.com"],
    ["https://acme.my.salesforce.com/", "acme.my.salesforce.com"],
    ["https://acme.my.salesforce.com:443", "acme.my.salesforce.com"],
    ["https://acme--uat.sandbox.my.salesforce.com", "acme--uat.sandbox.my.salesforce.com"],
    ["https://acme-dev-ed.develop.my.salesforce.com", "acme-dev-ed.develop.my.salesforce.com"],
    ["https://power-ruby-1234-dev-ed.scratch.my.salesforce.com", "power-ruby-1234-dev-ed.scratch.my.salesforce.com"],
  ] as const) {
    test(`${url} gives the bare host ${host}`, () => {
      assert.equal(myDomainHost(url), host);
      assert.ok(isMyDomainHost(host));
    });
  }

  for (const url of [
    // Salesforce, but not the org's own My Domain.
    "https://login.salesforce.com",
    "https://test.salesforce.com",
    "https://na1.salesforce.com",
    "https://eu45.salesforce.com",
    "https://acme.lightning.force.com",
    "https://acme.my.site.com",
    "https://acme.my.salesforce.mil",
    "https://my.salesforce.com",
    // Not Salesforce at all, or dressed as it.
    "https://evil.example",
    "https://acme.my.salesforce.com.evil.example",
    "https://acme.my.salesforce.com@evil.example",
    "https://evil.example/acme.my.salesforce.com",
    // A My Domain with more than a host: a port, path, query, fragment, credentials or another scheme.
    "https://acme.my.salesforce.com:8443",
    "https://acme.my.salesforce.com/lightning/setup",
    "https://acme.my.salesforce.com/?x=1",
    "https://acme.my.salesforce.com/#x",
    "https://user:pass@acme.my.salesforce.com",
    "http://acme.my.salesforce.com",
    "https://acme.my.salesforce.com.",
    "https://acme_co.my.salesforce.com",
    "acme.my.salesforce.com",
    "",
    "not a url",
  ]) {
    test(`${JSON.stringify(url)} gives no host`, () => assert.equal(myDomainHost(url), null));
  }

  test("anything but a string gives no host, and nothing throws", () => {
    for (const value of [undefined, null, 1, {}, ["https://acme.my.salesforce.com"]]) assert.equal(myDomainHost(value), null);
  });

  test("the check refuses what isn't a bare lower-case My Domain host", () => {
    for (const host of ["ACME.my.salesforce.com", "https://acme.my.salesforce.com", "acme.my.salesforce.com:443", "acme.my.salesforce.com/", " acme.my.salesforce.com", `${"a".repeat(250)}.my.salesforce.com`, null, undefined, 7]) {
      assert.equal(isMyDomainHost(host), false, String(host));
    }
  });
});
