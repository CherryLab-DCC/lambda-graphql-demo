import Client from "pg/lib/native/client.js";
import Pool from "pg-pool";

function interpolate(jsonpath, vars) {
  if (vars === undefined) {
    return jsonpath;
  }
  if (typeof vars !== "object" || vars === null) {
    vars = [vars];
  }
  return jsonpath.replace(/\$[_A-Za-z][_0-9A-Za-z]*/, (match) =>
    // @ts-ignore: Element implicitly has an 'any' type
    JSON.stringify(vars[match.slice(1)] ?? null)
  );
}

const errconsole = new console.Console({
  stdout: process.stderr,
  stderr: process.stderr,
});

class DB {
  constructor(client) {
    this.client = client;
    this.counter = 0;
    this.cached_getByUUID = new Map();
    this.cached_queryJsonPath = new Map();
    this.pending_getByUUID = new Set();
    this.pending_queryJsonPath = new Set();
    this.promise = null;
    this.lastbatch = -1;
  }
  async _fetchBatch() {
    //await null; // may not wait long enough
    //setimmediate because it is after the promise queue
    await new Promise((resolve) => setImmediate(resolve));
    // Also works since nextTick
    //await new Promise(resolve => process.nextTick(resolve));
    const batch = this.counter++;
    const pending_getByUUID = Array.from(this.pending_getByUUID);
    const pending_queryJsonPath = Array.from(this.pending_queryJsonPath);
    this.pending_getByUUID = new Set();
    this.pending_queryJsonPath = new Set();
    const values = [JSON.stringify(pending_getByUUID), JSON.stringify(pending_queryJsonPath)];
    const name = "fetchbatch";
    const text = `\
SELECT null AS index, id, (SELECT object FROM items WHERE items.id = ids.id::uuid) AS object
FROM jsonb_array_elements_text($1::jsonb) AS ids(id)
UNION ALL
SELECT index - 1, null AS id, COALESCE((SELECT jsonb_agg(id) FROM items WHERE object @@ query::jsonpath), '[]'::jsonb) AS object
FROM jsonb_array_elements_text($2::jsonb) WITH ORDINALITY queries(query, index)
;`;
    const result = await this.client.query({ name, text, values });
    for (const { index, id, object } of result.rows) {
      if (id !== null) {
        this.cached_getByUUID.set(id, object);
      } else {
        this.cached_queryJsonPath.set(pending_queryJsonPath[index], object);
      }
    }
    this.promise = null;
    this.lastbatch = batch;
  }
  async _pending() {
    const batch = this.counter;
    while (this.lastbatch !== batch) {
      if (this.promise === null) {
        this.promise = this._fetchBatch();
      }
      await this.promise;
    }
  }
  async getByUUID(itemid) {
    if (!this.cached_getByUUID.has(itemid)) {
      this.pending_getByUUID.add(itemid);
      await this._pending();
    }
    return this.cached_getByUUID.get(itemid);
  }
  async queryJsonPath(jsonpath, vars=null) {
    const jsonwhere = interpolate(jsonpath, vars);
    if (!this.cached_queryJsonPath.has(jsonwhere)) {
      this.pending_queryJsonPath.add(jsonwhere);
      await this._pending();
    }
    return this.cached_queryJsonPath.get(jsonwhere);
  }
}

function dbMiddlware(config) {
  const pool = new Pool(config);

  return function dbMiddlewareInner(req, res, next) {
    async function transactionalNext() {
      let released = false;
      let client;
      try {
        client = await pool.connect();
      } catch (err) {
        next(err);
        return;
      }
      try {
        try {
          req.db = new DB(client);
          await client.query("BEGIN");
        } catch (err) {
          client.release(true);
          released = true;
          next(err);
          return;
        }
        try {
          await new Promise((resolve) => {
            res.on("finish", resolve);
            next();
          });
        } finally {
          await client.query("ROLLBACK");
        }
      } finally {
        if (!released) {
          client.release();
        }
      }
    }
    transactionalNext();
  };
}

import {
  GraphQLSchema,
  GraphQLObjectType,
  GraphQLUnionType,
  GraphQLString,
  GraphQLFloat,
  GraphQLInt,
  GraphQLBoolean,
  GraphQLList,
  GraphQLNonNull,
  printSchema,
} from "graphql";
import { GraphQLJSON, GraphQLJSONObject } from "graphql-type-json";
import { readFileSync } from "fs";
const {
  _subtypes: subtypes,
  ["@type"]: _,
  ...profiles
} = JSON.parse(readFileSync(new URL("./profiles.json", import.meta.url)));
profiles.Page.properties.parent.type = "string"; // ['string', 'null']

const types = {};
// only concrete subtypes
const supertypes = {};
const abstract = [];
for (const [child, childst] of Object.entries(subtypes)) {
  if (!profiles[child]) {
    abstract.push(child);
  }
  const parents = [];
  for (const [k, v] of Object.entries(subtypes)) {
    if (childst.every((t) => v.includes(t))) {
      parents.push(k);
    }
  }
  parents.sort((a, b) => {
    const asubb = subtypes[a].every((t) => subtypes[b].includes(t));
    const bsuba = subtypes[b].every((t) => subtypes[a].includes(t));
    if (asubb && !bsuba) {
      return -1;
    }
    if (bsuba && !asubb) {
      return 1;
    }
    return 0;
  });
  supertypes[child] = parents;
}
for (const name of abstract) {
  const st = subtypes[name];
  types[name] = new GraphQLUnionType({
    name,
    types: () => st.map((t) => types[t]),
    resolveType(value) {
      return value.__typename;
    },
  });
}

const scalarTypeMap = {
  string: GraphQLString,
  float: GraphQLFloat,
  number: GraphQLFloat,
  integer: GraphQLInt,
  boolean: GraphQLBoolean,
  object: GraphQLJSONObject,
};

function resolveRef(obj, _, { db }, { fieldName }) {
  const uuid = obj[fieldName];
  if (!uuid) {
    return undefined;
  }
  return db.getByUUID(uuid);
}

function resolveRefList(obj, _, { db }, { fieldName }) {
  const uuids = obj[fieldName];
  if (!uuids) {
    return undefined;
  }
  return uuids.map((uuid) => db.getByUUID(uuid));
}

function makeResolveJsonPath(jsonPath) {
  return (obj, _, { db }) =>
    db
      .queryJsonPath(jsonPath, obj)
      .then((ids) => ids.map((id) => db.getByUUID(id)));
}

function normalizeLinkTo(types, linkTo) {
  const st = Array.from(
    new Set(
      (typeof linkTo === "string" ? [linkTo] : linkTo).flatMap(
        (t) => subtypes[t]
      )
    )
  ).sort();
  if (st.length === 1) {
    return types[st[0]];
  }
  const name = st.join("__");
  if (!types[name]) {
    types[name] = new GraphQLUnionType({
      name,
      types: st.map((t) => types[t]),
      resolveType(value) {
        return value.__typename;
      },
    });
  }
  return types[name];
}

function jsonpathIdent(s) {
  return /^[_A-Za-z][_0-9A-Za-z]*$/.test(s) ? s : JSON.stringify(s);
}

function defaultLinkFromJsonPath(fromType, fromProperty) {
  return `$."@type"[*] == ${JSON.stringify(fromType)} && $.${jsonpathIdent(
    fromProperty
  )} == $uuid && $.status != "deleted" && $.status != "replaced"`;
}

function makeField(path, schema, types, isRequired, isRenamed) {
  const { description, items } = schema;
  const originalName = path[path.length - 1];

  if (schema.linkTo) {
    const inner = normalizeLinkTo(types, schema.linkTo);
    const type = isRequired ? new GraphQLNonNull(inner) : inner;
    return { type, description, resolve: resolveRef };
  }

  if (items?.linkTo) {
    const inner = normalizeLinkTo(types, items.linkTo);
    const list = new GraphQLList(new GraphQLNonNull(inner));
    const type = isRequired ? new GraphQLNonNull(list) : list;
    return { type, description, resolve: resolveRefList };
  }

  if (items?.linkFrom) {
    const [fromType, fromProperty] = items.linkFrom.split(".");
    const inner = types[fromType];
    const jsonPath =
      items.linkFromJsonPath ?? defaultLinkFromJsonPath(fromType, fromProperty);
    const type = new GraphQLNonNull(new GraphQLList(new GraphQLNonNull(inner)));
    return { type, description, resolve: makeResolveJsonPath(jsonPath) };
  }

  let type;
  if (items) {
    const inner = makeField(path, items, types, true, false);
    if (inner) {
      type = new GraphQLList(inner.type);
    }
  } else if (schema.properties && !schema.additionalProperties) {
    type = makeObject(path, schema, types);
  } else if (Array.isArray(schema.type)) {
    // all type: ['number', 'string'], mostly also pattern: '^Infinity$',
    console.warn({ reason: "type isArray", path, schema });
    type = GraphQLJSON;
  } else {
    type = scalarTypeMap[schema.type];
  }
  if (type) {
    if (isRequired) {
      type = new GraphQLNonNull(type);
    }
    const resolve = isRenamed ? (obj) => obj[originalName] : undefined;
    return { type, description, resolve };
  }
  console.error({ reason: "no type", path, schema });
}

function normalizeName(name) {
  let fieldName = name.replace("%", "pct").replace(/[^_a-zA-Z0-9]/g, "_");
  if (!fieldName.match(/^[_a-zA-Z]/)) {
    fieldName = "_" + fieldName;
  }
  return fieldName;
}

function makeObject(path, schema, types) {
  const { description, properties, required } = schema;
  const name = path.map((name) => normalizeName(name)).join("__");
  //const interfaces = supertypes[name].slice(1);
  const type = new GraphQLObjectType({
    name,
    description,
    //interfaces: () => interfaces.map(t => types[t]),
    fields: () =>
      Object.fromEntries(
        Object.entries(properties)
          .map(([k, subschema]) => {
            const fieldName = normalizeName(k);
            const isRequired = required?.includes(k);
            const isRenamed = fieldName !== k;
            return [
              fieldName,
              makeField([...path, k], subschema, types, isRequired, isRenamed),
            ];
          })
          .filter(([k, v]) => v)
      ),
  });
  types[name] = type;
  return type;
}

for (const [name, schema] of Object.entries(profiles)) {
  makeObject([name], schema, types);
}

const query = new GraphQLObjectType({
  name: "Query",
  fields: () => ({
    getByUUID: {
      type: types.Item,
      args: {
        uuid: { type: new GraphQLNonNull(GraphQLString) },
      },
      resolve: (_, { uuid }, { db }) => {
        return db.getByUUID(uuid);
      },
    },
    queryJsonPath: {
      type: new GraphQLNonNull(new GraphQLList(new GraphQLNonNull(types.Item))),
      args: {
        path: { type: new GraphQLNonNull(GraphQLString) },
      },
      resolve: (_, { path }, { db }) => {
        return db
          .queryJsonPath(path)
          .then((ids) => ids.map((id) => db.getByUUID(id)));
      },
    },
  }),
});
const schema = new GraphQLSchema({ query });

import express from "express";
import { graphqlHTTP } from "express-graphql";

const extensions = ({
  document,
  variables,
  operationName,
  result,
  context,
}) => {
  return {
    time_ms: Date.now() - context.startTime,
  };
};

const app = express();
//app.use("/schema.graphql", (req, res, next) => {
//  res.contentType('text/plain');
//  res.send(printSchema(schema));
//});
app.use(
//  "/graphql",
  dbMiddlware({
    Client,
    log: console.log,
    user: 'postgres',
    password: 'postgres',
    host: "lrowe-graphql-demo-rds.cfkkcfbabiei.us-west-2.rds.amazonaws.com",
    database: "graphql",
    max: 1,
    min: 0,
    idleTimeoutMillis: 120000,
    connectionTimeoutMillis: 10000
  }),
  graphqlHTTP((request) => {
    request.startTime = Date.now();
    return {
      schema,
      graphiql: true,
      customFormatErrorFn: (error) => {
        console.error(error);
        return {
          message: error.message,
          locations: error.locations,
          stack: error.stack ? error.stack.split("\n") : [],
          path: error.path,
        };
      },
      extensions,
    };
  })
);
//app.listen(4000);

export default app;
