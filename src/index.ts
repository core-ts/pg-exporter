import { Pool } from 'pg';
import QueryStream from 'pg-query-stream';

export type DataType = 'ObjectId' | 'date' | 'datetime' | 'time'
  | 'boolean' | 'number' | 'integer' | 'string' | 'text'
  | 'object' | 'array' | 'binary'
  | 'primitives' | 'booleans' | 'numbers' | 'integers' | 'strings' | 'dates' | 'datetimes' | 'times';
export type FormatType = 'currency' | 'percentage' | 'email' | 'url' | 'phone' | 'fax' | 'ipv4' | 'ipv6';
export type MatchType = 'equal' | 'prefix' | 'contain' | 'max' | 'min'; // contain: default for string, min: default for Date, number

export interface Attribute {
  name?: string;
  field?: string;
  column?: string;
  type?: DataType;
  format?: FormatType;
  required?: boolean;
  match?: MatchType;
  default?: string|number|Date|boolean;
  key?: boolean;
  unique?: boolean;
  enum?: string[] | number[];
  q?: boolean;
  noinsert?: boolean;
  noupdate?: boolean;
  nopatch?: boolean;
  version?: boolean;
  length?: number;
  min?: number;
  max?: number;
  gt?: number;
  lt?: number;
  precision?: number;
  scale?: number;
  exp?: RegExp | string;
  code?: string;
  noformat?: boolean;
  ignored?: boolean;
  jsonField?: string;
  link?: string;
  typeof?: Attributes;
  true?: string|number;
  false?: string|number;
}
export interface Attributes {
  [key: string]: Attribute;
}

export interface StringMap {
  [key: string]: string;
}
export interface Statement {
  query: string;
  params?: any[];
}
export interface QueryBuilder {
  buildQuery(ctx?: any): Promise<Statement>;
}
export interface Formatter<T> {
  format: (row: T) => string;
}
export interface FileWriter {
  write(chunk: string): boolean;
  flush?(cb?: () => void): void;
  end?(cb?: () => void): void;
}
export class Exporter<T> {
  constructor(
    public pool: Pool,
    public buildQuery: (ctx?: any) => Promise<Statement>,
    public format: (row: T) => string,
    public write: (chunk: string) => boolean,
    public end: (cb?: () => void) => void,
    public attributes?: Attributes
  ) {
    if (attributes) {
      this.map = buildMap(attributes);
    }
    this.export = this.export.bind(this);
  }
  map?: StringMap;
  async export(ctx?: any): Promise<number> {
    let i = 0;
    const stmt = await this.buildQuery(ctx);
    const query = new QueryStream(stmt.query, stmt.params);
    await this.pool.connect();
    this.pool.query(query);
    if (this.map) {
      for await (const data of query) {
        i++;
        const obj = mapOne<T>(data, this.map);
        const str = this.format(obj);
        this.write(str);
      }
    } else {
      for await (const data of query) {
        i++;
        const str = this.format(data);
        this.write(str);
      }
    }
    this.pool.end();
    this.end();
    return i;
  }
}
// tslint:disable-next-line:max-classes-per-file
export class ExportService<T> {
  constructor(
    public pool: Pool,
    public queryBuilder: QueryBuilder,
    // tslint:disable-next-line:ban-types
    public formatter: Formatter<T>,
    public writer: FileWriter,
    public attributes?: Attributes
  ) {
    if (attributes) {
      this.map = buildMap(attributes);
    }
    this.export = this.export.bind(this);
  }
  map?: StringMap;
  async export(ctx?: any): Promise<number> {
    let i = 0;
    const stmt = await this.queryBuilder.buildQuery(ctx);
    const query = new QueryStream(stmt.query, stmt.params);
    await this.pool.connect();
    this.pool.query(query);
    if (this.map) {
      for await (const data of query) {
        i++;
        const obj = mapOne<T>(data, this.map);
        const str = this.formatter.format(obj);
        this.writer.write(str);
      }
    } else {
      for await (const data of query) {
        i++;
        const str = this.formatter.format(data);
        this.writer.write(str);
      }
    }
    this.pool.end();
    if (this.writer.end) {
      this.writer.end();
    } else if (this.writer.flush) {
      this.writer.flush();
    }
    return i;
  }
}
// tslint:disable-next-line:ban-types
export function mapOne<T>(results: Object, m?: StringMap): T {
  const obj: any = results;
  if (!m) {
    return obj;
  }
  const mkeys = Object.keys(m as any);
  if (mkeys.length === 0) {
    return obj;
  }
  const obj2: any = {};
  const keys = Object.keys(obj);
  for (const key of keys) {
    let k0 = m[key];
    if (!k0) {
      k0 = key;
    }
    obj2[k0] = (obj)[key];
  }
  return obj2;
}
export function buildMap(attrs: Attributes): StringMap|undefined {
  const mp: StringMap = {};
  const ks = Object.keys(attrs);
  let isMap = false;
  for (const k of ks) {
    const attr = attrs[k];
    attr.name = k;
    const field = (attr.column ? attr.column : k);
    const s = field.toLowerCase();
    if (s !== k) {
      mp[s] = k;
      isMap = true;
    }
  }
  if (isMap) {
    return mp;
  }
  return undefined;
}
export function param(i: number): string {
  return '$' + i;
}
export function select(table: string, attrs: Attributes): string {
  const cols: string[] = [];
  const ks = Object.keys(attrs);
  for (const k of ks) {
    const attr = attrs[k];
    attr.name = k;
    const field = (attr.column ? attr.column : k);
    const s = field.toLowerCase();
    cols.push(s);
  }
  return `select ${cols.join(',')} from ${table}`;
}
