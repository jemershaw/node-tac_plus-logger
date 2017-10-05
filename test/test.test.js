var test = require('tape');
var fs = require('fs');
var path = require('path');
var tmpPath = path.join(__dirname,'../tmp/');

var config = {
    PID_FILE: tmpPath+'testing.pid',
    logDir: __dirname,
    TP_TEMPDIR: tmpPath,
    LOG_MANTA_PATH: '~~/stor/node-tplogger-testing/',
    TP_FILE_RE: /.*examples.*/
}

test('test: testing first pass', function(t) {
  t.plan(1);
  var tacPlus = require('../tac_plus.js');
  tacPlus.config(config);
  tacPlus.startPipeline(function(err, results) {
    t.equals(err,null);
    t.end();
  });
});

test('test: single pid', function(t) {
  t.plan(2);
  var tacPlus = tacPlus2 = require('../tac_plus.js');
  tacPlus.config(config);
  tacPlus.start(function(err) {
    t.equals(err,null);
  });
  setTimeout(function() {
    tacPlus2.start(function(err) {
      t.equals(typeof err,'object');
    });
  }, 50);
});


test('test: cleanup', function(t) {
  t.plan(1);
  var tacPlus = require('../tac_plus.js');
  tacPlus.config(config);
  tacPlus.cleanUp(function(err) {
    t.equals(err,null);
  });
});
