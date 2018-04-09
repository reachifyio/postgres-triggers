'use strict';

var pg = require('pg');
var assert = require('assert');

function buildQuery(triggers) {
  var opts = arguments.length <= 1 || arguments[1] === undefined ? {} : arguments[1];

  opts.channel = typeof opts.channel === 'string' ? opts.channel : 'table_update';
  assert.equal(typeof triggers, 'string');

  return '\n    CREATE OR REPLACE FUNCTION table_update_notify() RETURNS trigger AS $$\n    DECLARE\n      id TEXT;\n      row RECORD;\n    BEGIN\n      IF TG_OP = \'INSERT\' OR TG_OP = \'UPDATE\' THEN\n        row = NEW;\n      ELSE\n        row = OLD;\n      END IF;\n\n      EXECUTE \'SELECT ($1).\' || TG_ARGV[0] INTO id USING row;\n\n      IF TG_OP = \'UPDATE\' THEN\n        PERFORM pg_notify(\'' + opts.channel + '\', json_build_object(\'table\', TG_TABLE_NAME, \'id\', id, \'type\', lower(TG_OP), \'row\', hstore_to_json(hstore(NEW) - hstore(OLD)))::text);\n        RETURN NEW;\n      END IF;\n\n      IF TG_OP = \'INSERT\' THEN\n        PERFORM pg_notify(\'' + opts.channel + '\', json_build_object(\'table\', TG_TABLE_NAME, \'id\', id, \'type\', lower(TG_OP), \'row\', row_to_json(NEW))::text);\n        RETURN NEW;\n      END IF;\n\n      IF TG_OP = \'DELETE\' THEN\n        PERFORM pg_notify(\'' + opts.channel + '\', json_build_object(\'table\', TG_TABLE_NAME, \'id\', id, \'type\', lower(TG_OP), \'row\', row_to_json(OLD))::text);\n        RETURN OLD;\n      END IF;\n\n    END;\n    $$ LANGUAGE plpgsql;\n\n    ' + triggers + '\n  ';
}

function parseTables(tables) {
  // tableName:idColumn -> { name: 'tableName', id: 'idColumn' }
  return tables.map(function (table) {
    if (typeof table === 'string') {
      var split = table.split(':');
      return { name: split[0], id: split[1] || 'id' };
    } else {
      return table;
    }
  });
}

function buildTriggers(tables) {
  return parseTables(tables).map(function (table) {
    return '\n      DROP TRIGGER IF EXISTS ' + table.name + '_notify_update ON ' + table.name + ';\n      CREATE TRIGGER ' + table.name + '_notify_update AFTER UPDATE ON ' + table.name + ' FOR EACH ROW EXECUTE PROCEDURE table_update_notify(\'' + table.id + '\');\n\n      DROP TRIGGER IF EXISTS ' + table.name + '_notify_insert ON ' + table.name + ';\n      CREATE TRIGGER ' + table.name + '_notify_insert AFTER INSERT ON ' + table.name + ' FOR EACH ROW EXECUTE PROCEDURE table_update_notify(\'' + table.id + '\');\n\n      DROP TRIGGER IF EXISTS ' + table.name + '_notify_delete ON ' + table.name + ';\n      CREATE TRIGGER ' + table.name + '_notify_delete AFTER DELETE ON ' + table.name + ' FOR EACH ROW EXECUTE PROCEDURE table_update_notify(\'' + table.id + '\');\n    ';
  }).join('');
}

module.exports = function (opts, cb) {
  assert(Array.isArray(opts.tables), 'opts.tables should be an array.');
  assert.ok(opts.db, 'need db connection string');

  // nothing to do
  if (!opts.tables.length) return cb(null, { message: 'nothing to do' });

  pg.connect(opts.db, function (err, client, done) {
    if (err) return cb(err);

    var triggers = buildTriggers(opts.tables);
    var query = client.query(buildQuery(triggers, opts));

    // query.on('row', function(row) {
    //   console.log(row)
    // })

    query.on('error', function (queryErr) {
      done(queryErr);
      cb(queryErr);
    });

    query.on('end', function () {
      client.end();
      cb(null, { message: 'done' });
    });
  });
};

module.exports.buildQuery = buildQuery;
module.exports.buildTriggers = buildTriggers;