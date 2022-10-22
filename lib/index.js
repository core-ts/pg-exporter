"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
  function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
  return new (P || (P = Promise))(function (resolve, reject) {
    function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
    function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
    function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
    step((generator = generator.apply(thisArg, _arguments || [])).next());
  });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
  var _ = { label: 0, sent: function () { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
  return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function () { return this; }), g;
  function verb(n) { return function (v) { return step([n, v]); }; }
  function step(op) {
    if (f) throw new TypeError("Generator is already executing.");
    while (_) try {
      if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
      if (y = 0, t) op = [op[0] & 2, t.value];
      switch (op[0]) {
        case 0: case 1: t = op; break;
        case 4: _.label++; return { value: op[1], done: false };
        case 5: _.label++; y = op[1]; op = [0]; continue;
        case 7: op = _.ops.pop(); _.trys.pop(); continue;
        default:
          if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
          if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
          if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
          if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
          if (t[2]) _.ops.pop();
          _.trys.pop(); continue;
      }
      op = body.call(thisArg, _);
    } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
    if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
  }
};
var __asyncValues = (this && this.__asyncValues) || function (o) {
  if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
  var m = o[Symbol.asyncIterator], i;
  return m ? m.call(o) : (o = typeof __values === "function" ? __values(o) : o[Symbol.iterator](), i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i);
  function verb(n) { i[n] = o[n] && function (v) { return new Promise(function (resolve, reject) { v = o[n](v), settle(resolve, reject, v.done, v.value); }); }; }
  function settle(resolve, reject, d, v) { Promise.resolve(v).then(function (v) { resolve({ value: v, done: d }); }, reject); }
};
var __importDefault = (this && this.__importDefault) || function (mod) {
  return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
var pg_query_stream_1 = __importDefault(require("pg-query-stream"));
var Exporter = (function () {
  function Exporter(pool, buildQuery, format, write, end, attributes) {
    this.pool = pool;
    this.buildQuery = buildQuery;
    this.format = format;
    this.write = write;
    this.end = end;
    this.attributes = attributes;
    if (attributes) {
      this.map = buildMap(attributes);
    }
    this.export = this.export.bind(this);
  }
  Exporter.prototype.export = function (ctx) {
    var e_1, _a, e_2, _b;
    return __awaiter(this, void 0, void 0, function () {
      var i, stmt, query, query_1, query_1_1, data, obj, str, e_1_1, query_2, query_2_1, data, str, e_2_1;
      return __generator(this, function (_c) {
        switch (_c.label) {
          case 0:
            i = 0;
            return [4, this.buildQuery(ctx)];
          case 1:
            stmt = _c.sent();
            query = new pg_query_stream_1.default(stmt.query, stmt.params);
            return [4, this.pool.connect()];
          case 2:
            _c.sent();
            this.pool.query(query);
            if (!this.map) return [3, 15];
            _c.label = 3;
          case 3:
            _c.trys.push([3, 8, 9, 14]);
            query_1 = __asyncValues(query);
            _c.label = 4;
          case 4: return [4, query_1.next()];
          case 5:
            if (!(query_1_1 = _c.sent(), !query_1_1.done)) return [3, 7];
            data = query_1_1.value;
            i++;
            obj = mapOne(data, this.map);
            str = this.format(obj);
            this.write(str);
            _c.label = 6;
          case 6: return [3, 4];
          case 7: return [3, 14];
          case 8:
            e_1_1 = _c.sent();
            e_1 = { error: e_1_1 };
            return [3, 14];
          case 9:
            _c.trys.push([9, , 12, 13]);
            if (!(query_1_1 && !query_1_1.done && (_a = query_1.return))) return [3, 11];
            return [4, _a.call(query_1)];
          case 10:
            _c.sent();
            _c.label = 11;
          case 11: return [3, 13];
          case 12:
            if (e_1) throw e_1.error;
            return [7];
          case 13: return [7];
          case 14: return [3, 26];
          case 15:
            _c.trys.push([15, 20, 21, 26]);
            query_2 = __asyncValues(query);
            _c.label = 16;
          case 16: return [4, query_2.next()];
          case 17:
            if (!(query_2_1 = _c.sent(), !query_2_1.done)) return [3, 19];
            data = query_2_1.value;
            i++;
            str = this.format(data);
            this.write(str);
            _c.label = 18;
          case 18: return [3, 16];
          case 19: return [3, 26];
          case 20:
            e_2_1 = _c.sent();
            e_2 = { error: e_2_1 };
            return [3, 26];
          case 21:
            _c.trys.push([21, , 24, 25]);
            if (!(query_2_1 && !query_2_1.done && (_b = query_2.return))) return [3, 23];
            return [4, _b.call(query_2)];
          case 22:
            _c.sent();
            _c.label = 23;
          case 23: return [3, 25];
          case 24:
            if (e_2) throw e_2.error;
            return [7];
          case 25: return [7];
          case 26:
            this.pool.end();
            this.end();
            return [2, i];
        }
      });
    });
  };
  return Exporter;
}());
exports.Exporter = Exporter;
var ExportService = (function () {
  function ExportService(pool, queryBuilder, formatter, writer, attributes) {
    this.pool = pool;
    this.queryBuilder = queryBuilder;
    this.formatter = formatter;
    this.writer = writer;
    this.attributes = attributes;
    if (attributes) {
      this.map = buildMap(attributes);
    }
    this.export = this.export.bind(this);
  }
  ExportService.prototype.export = function (ctx) {
    var e_3, _a, e_4, _b;
    return __awaiter(this, void 0, void 0, function () {
      var i, stmt, query, query_3, query_3_1, data, obj, str, e_3_1, query_4, query_4_1, data, str, e_4_1;
      return __generator(this, function (_c) {
        switch (_c.label) {
          case 0:
            i = 0;
            return [4, this.queryBuilder.buildQuery(ctx)];
          case 1:
            stmt = _c.sent();
            query = new pg_query_stream_1.default(stmt.query, stmt.params);
            return [4, this.pool.connect()];
          case 2:
            _c.sent();
            this.pool.query(query);
            if (!this.map) return [3, 15];
            _c.label = 3;
          case 3:
            _c.trys.push([3, 8, 9, 14]);
            query_3 = __asyncValues(query);
            _c.label = 4;
          case 4: return [4, query_3.next()];
          case 5:
            if (!(query_3_1 = _c.sent(), !query_3_1.done)) return [3, 7];
            data = query_3_1.value;
            i++;
            obj = mapOne(data, this.map);
            str = this.formatter.format(obj);
            this.writer.write(str);
            _c.label = 6;
          case 6: return [3, 4];
          case 7: return [3, 14];
          case 8:
            e_3_1 = _c.sent();
            e_3 = { error: e_3_1 };
            return [3, 14];
          case 9:
            _c.trys.push([9, , 12, 13]);
            if (!(query_3_1 && !query_3_1.done && (_a = query_3.return))) return [3, 11];
            return [4, _a.call(query_3)];
          case 10:
            _c.sent();
            _c.label = 11;
          case 11: return [3, 13];
          case 12:
            if (e_3) throw e_3.error;
            return [7];
          case 13: return [7];
          case 14: return [3, 26];
          case 15:
            _c.trys.push([15, 20, 21, 26]);
            query_4 = __asyncValues(query);
            _c.label = 16;
          case 16: return [4, query_4.next()];
          case 17:
            if (!(query_4_1 = _c.sent(), !query_4_1.done)) return [3, 19];
            data = query_4_1.value;
            i++;
            str = this.formatter.format(data);
            this.writer.write(str);
            _c.label = 18;
          case 18: return [3, 16];
          case 19: return [3, 26];
          case 20:
            e_4_1 = _c.sent();
            e_4 = { error: e_4_1 };
            return [3, 26];
          case 21:
            _c.trys.push([21, , 24, 25]);
            if (!(query_4_1 && !query_4_1.done && (_b = query_4.return))) return [3, 23];
            return [4, _b.call(query_4)];
          case 22:
            _c.sent();
            _c.label = 23;
          case 23: return [3, 25];
          case 24:
            if (e_4) throw e_4.error;
            return [7];
          case 25: return [7];
          case 26:
            this.pool.end();
            if (this.writer.end) {
              this.writer.end();
            }
            else if (this.writer.flush) {
              this.writer.flush();
            }
            return [2, i];
        }
      });
    });
  };
  return ExportService;
}());
exports.ExportService = ExportService;
function mapOne(results, m) {
  var obj = results;
  if (!m) {
    return obj;
  }
  var mkeys = Object.keys(m);
  if (mkeys.length === 0) {
    return obj;
  }
  var obj2 = {};
  var keys = Object.keys(obj);
  for (var _i = 0, keys_1 = keys; _i < keys_1.length; _i++) {
    var key = keys_1[_i];
    var k0 = m[key];
    if (!k0) {
      k0 = key;
    }
    obj2[k0] = (obj)[key];
  }
  return obj2;
}
exports.mapOne = mapOne;
function buildMap(attrs) {
  var mp = {};
  var ks = Object.keys(attrs);
  var isMap = false;
  for (var _i = 0, ks_1 = ks; _i < ks_1.length; _i++) {
    var k = ks_1[_i];
    var attr = attrs[k];
    attr.name = k;
    var field = (attr.column ? attr.column : k);
    var s = field.toLowerCase();
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
exports.buildMap = buildMap;
function param(i) {
  return '$' + i;
}
exports.param = param;
function select(table, attrs) {
  var cols = [];
  var ks = Object.keys(attrs);
  for (var _i = 0, ks_2 = ks; _i < ks_2.length; _i++) {
    var k = ks_2[_i];
    var attr = attrs[k];
    attr.name = k;
    var field = (attr.column ? attr.column : k);
    var s = field.toLowerCase();
    cols.push(s);
  }
  return "select " + cols.join(',') + " from " + table;
}
exports.select = select;
