#!/usr/bin/env node

'use strict';

var program = require('commander');
var triggers = require('./');

program.usage('[options] connection-string <table ...>').version(require('./package.json').version).parse(process.argv);

var db = program.args[0];
var tables = program.args.slice(1);

triggers({ db: db, tables: tables }, function (err, res) {
  if (err) throw err;
  console.log(res.message);
});