// A single-currency org's currency, worked out from its default locale
// (DECISIONS.md 700). Salesforce's object reference documents no field a query
// can read for a single-currency org's currency: its "Currency Locale" is a
// setting of its own in Company Information, and the default locale is a
// separate one (docs/QUESTIONS.md). So the code worked out here is only ever
// an assumption, said on every money figure that uses it, and never confirmed.
//
// Country to ISO 4217 code, for the countries whose everyday currency is one
// code beyond doubt. A country left out gives no currency: two currencies in
// everyday use (Cambodia, Lebanon, Panama, Liberia), a changeover close to now
// (Bulgaria's euro, Sierra Leone's and Curaçao's new codes), or a currency too
// unsettled to label amounts with (Venezuela, Zimbabwe, Cuba). The euro
// countries include those that use it without being members (Montenegro,
// Kosovo, Andorra, Monaco, San Marino, the Vatican).

const EURO = [
  "AD", "AT", "AX", "BE", "BL", "CY", "DE", "EE", "ES", "FI", "FR", "GF", "GP", "GR", "HR", "IE", "IT", "LT", "LU", "LV", "MC", "ME", "MF", "MQ", "MT", "NL",
  "PM", "PT", "RE", "SI", "SK", "SM", "VA", "XK", "YT",
] as const;

const OTHERS: Record<string, string> = {
  AE: "AED", AF: "AFN", AG: "XCD", AL: "ALL", AM: "AMD", AO: "AOA", AR: "ARS", AS: "USD", AU: "AUD", AW: "AWG", AZ: "AZN",
  BA: "BAM", BB: "BBD", BD: "BDT", BF: "XOF", BH: "BHD", BI: "BIF", BJ: "XOF", BM: "BMD", BN: "BND", BO: "BOB", BR: "BRL", BS: "BSD", BW: "BWP", BY: "BYN", BZ: "BZD",
  CA: "CAD", CD: "CDF", CF: "XAF", CG: "XAF", CH: "CHF", CI: "XOF", CK: "NZD", CL: "CLP", CM: "XAF", CN: "CNY", CO: "COP", CR: "CRC", CV: "CVE", CZ: "CZK",
  DJ: "DJF", DK: "DKK", DM: "XCD", DO: "DOP", DZ: "DZD",
  EC: "USD", EG: "EGP", ER: "ERN", ET: "ETB",
  FJ: "FJD", FM: "USD", FO: "DKK",
  GA: "XAF", GB: "GBP", GD: "XCD", GE: "GEL", GG: "GBP", GH: "GHS", GI: "GIP", GL: "DKK", GM: "GMD", GN: "GNF", GQ: "XAF", GT: "GTQ", GU: "USD", GW: "XOF", GY: "GYD",
  HK: "HKD", HN: "HNL", HT: "HTG", HU: "HUF",
  ID: "IDR", IL: "ILS", IM: "GBP", IN: "INR", IQ: "IQD", IR: "IRR", IS: "ISK",
  JE: "GBP", JM: "JMD", JO: "JOD", JP: "JPY",
  KE: "KES", KG: "KGS", KI: "AUD", KM: "KMF", KN: "XCD", KP: "KPW", KR: "KRW", KW: "KWD", KY: "KYD", KZ: "KZT",
  LA: "LAK", LC: "XCD", LI: "CHF", LK: "LKR", LY: "LYD",
  MA: "MAD", MD: "MDL", MG: "MGA", MH: "USD", MK: "MKD", ML: "XOF", MM: "MMK", MN: "MNT", MO: "MOP", MR: "MRU", MU: "MUR", MV: "MVR", MW: "MWK", MX: "MXN", MY: "MYR", MZ: "MZN",
  NA: "NAD", NC: "XPF", NE: "XOF", NG: "NGN", NI: "NIO", NO: "NOK", NP: "NPR", NR: "AUD", NU: "NZD", NZ: "NZD",
  OM: "OMR",
  PE: "PEN", PF: "XPF", PG: "PGK", PH: "PHP", PK: "PKR", PL: "PLN", PR: "USD", PW: "USD", PY: "PYG",
  QA: "QAR",
  RO: "RON", RS: "RSD", RU: "RUB", RW: "RWF",
  SA: "SAR", SB: "SBD", SC: "SCR", SD: "SDG", SE: "SEK", SG: "SGD", SN: "XOF", SR: "SRD", ST: "STN", SV: "USD", SY: "SYP",
  TD: "XAF", TG: "XOF", TH: "THB", TJ: "TJS", TL: "USD", TM: "TMT", TN: "TND", TO: "TOP", TR: "TRY", TT: "TTD", TV: "AUD", TW: "TWD", TZ: "TZS",
  UA: "UAH", UG: "UGX", US: "USD", UY: "UYU", UZ: "UZS",
  VC: "XCD", VI: "USD", VN: "VND", VU: "VUV",
  WF: "XPF", WS: "WST",
  YE: "YER",
  ZA: "ZAR", ZM: "ZMW",
};

/** Every country we map, to its code. Frozen: a test reads it whole. */
export const COUNTRY_CURRENCY: Readonly<Record<string, string>> = Object.freeze({ ...OTHERS, ...Object.fromEntries(EURO.map((c) => [c, "EUR"])) });

/**
 * The country a Salesforce locale names, such as GB in en_GB or DE in
 * de_DE_EURO, or null when it names none (en). The language comes first; a
 * script of four letters (sr_Latn_RS) may come before the country; anything
 * after it is a variant.
 */
export function localeCountry(locale: string): string | null {
  const parts = locale.trim().split(/[_-]/);
  if (!/^[a-z]{2,3}$/.test(parts[0] ?? "")) return null;
  const rest = /^[A-Z][a-z]{3}$/.test(parts[1] ?? "") ? parts.slice(2) : parts.slice(1);
  return /^[A-Z]{2}$/.test(rest[0] ?? "") ? rest[0]! : null;
}

/** The ISO 4217 code of the locale's country, or null: no country, or one we can't map with certainty. */
export function currencyForLocale(locale: string): string | null {
  const country = localeCountry(locale);
  return country ? (COUNTRY_CURRENCY[country] ?? null) : null;
}
