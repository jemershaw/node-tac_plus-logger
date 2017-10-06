var fs = require('fs');
var path = require('path');
var bunyan = require('bunyan');
var logDir = '/var/log'
var vasync = require('vasync');
var os = require('os');

var assert = require('assert');
var fs = require('fs');
var manta = require('manta');
var PID_FILE= "/tmp/node-tac_plus-logger.pid"
var TP_TEMPDIR = os.tmpdir();
var LOG_MANTA_PATH = '~~/stor/logs/tac_plus/'
var TP_FILE_RE = /tac_plus\.acct\..*/
var HOSTNAME = os.hostname().match(/([^\.]*).*/)[1];

function config(config) {
  if(config && config.PID_FILE) PID_FILE = config.PID_FILE;
  if(config && config.TP_TEMPDIR) TP_TEMPDIR = config.TP_TEMPDIR;
  if(config && config.LOG_MANTA_PATH) LOG_MANTA_PATH = config.LOG_MANTA_PATH;
  if(config && config.logDir) logDir = config.logDir;
  if(config && config.TP_FILE_RE) TP_FILE_RE = config.TP_FILE_RE;
}

function checkAlreadyRunning(cb) {
  fs.stat(PID_FILE, function (err, stat) {
    function recordPid() {
      fs.writeFileSync(PID_FILE, process.pid, 'utf8');
      cb(null);
      return;
    }
    if (err && err.code === 'ENOENT') {
      recordPid();
      return;
    }
    if( err ) {
      cb(err);
      return;
    }
    if (!stat.isFile()) {
      recordPid();
      return;
    }
    cb(new Error("Pid exists"));
  });
}
function cleanupPidFile(cb) {
  fs.unlinkSync(PID_FILE);
  cb();
};

function start(cb) {
  cb = (cb && typeof cb == 'function') ? cb : null
  checkAlreadyRunning(function(err) {
    if(err) {
      return cb(err);
    }
    startPipeline(function(err,results) {
      cleanupPidFile(function() {
        cb(err,results)
      });
    });
  });
}

function startPipeline(cb) {
  vasync.pipeline({arg: { }, funcs: [
    function createClient(args,cb) {
      var client = manta.createClient({
        sign: manta.privateKeySigner({
          key: fs.readFileSync(process.env.HOME + '/.ssh/id_rsa', 'utf8'),
          keyId: process.env.MANTA_KEY_ID,
          user: process.env.MANTA_USER
        }),
        user: process.env.MANTA_USER,
        url: process.env.MANTA_URL
      });
      args.client = client;
      cb();
    },
    function f1 (args,cb) {
      getFiles(function(files) {
        args.files = files
        cb(null);
      })
    },
    function f2 (args,cb) {
      parse(args.files,function(logs) {
        args.logs = logs
        cb(null);
      });
    },
    function f3 (args,cb) {
      loopLines(args.files,args.logs, function() {
        cb();
      });
    },
    function f4 (args, cb) {
      createMantaDir(args.client, args.files, function() {
        cb();
      });
    },
    function f5 (args, cb) {
      putLogToManta(args.client, args.logs, function() {
        cb();
      });
    }
  ]
  },cb);
}

// 1. Find files that match the tac_plus acct log, and create an array
function getFiles(cb) {
  fs.readdir(logDir, 'utf8', function (err, files) {
    if(err) return;
    var results = []
    files.map(function(file) {
      if(file = file.match(TP_FILE_RE)) {
        results.push(file.input);
      }
    });
    cb(results);
  });
}

// 2. create temporary bunyan log file locaiton
function parse(files, cb) {
  var logs = [];
  files.map(function(file) {
    var logFile = path.join(TP_TEMPDIR,file+'.bunyan');
    logs.push(bunyan.createLogger({ name: file,
      streams: [
        {
          path: logFile
        }
      ]
    }));
  });
  cb(logs);
}

// 3. read each file and create the bunyan file
function loopLines(files,logs,cb) {
  if(files.length ==  0 || logs.length == 0) return cb();
  files.forEach(function(filename, index) {
    var log = logs[index];
    var filePath = path.join(logDir,filename);
    
    writeLog(filePath,log,function() {
      if(index == (files.length -1)) {
        cb()
      }
    });
  });
}

// 4. read each file line by line and slit on tab then convert lines into
//    bunyan logs
function writeLog(file, log, cb) {
  var dateRegex = /(^[A-Z][a-z]{2})\s+(\d{1,2})\s+(\d{1,2}:\d{1,2}:\d{1,2})$/;
  var ipRegex = /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/;
  var cmd_RE = /^cmd=(.*)$/;
  var service_RE = /^service=(.*)$/;
  var timezone_RE = /^timezone=(.*)$/;
  var process_RE = /^process\*mgd\[(.*)\]$/;
  var elapsed_time_RE = /^elapsed_time=(.*)$/;
  var discCause_RE = /^disc-cause=(.*)$/;
  var preSessionTime_RE = /^pre-session-time=(.*)$/;
  var privLvl_RE= /^priv-lvl=(.*)$/;
  var TZ = new Date().toString().match(/.*\((.*)\)/)[1];

  fs.readFile(file,'utf8', function (err, data) {
    var lines = data.split('\n');
    lines.forEach(function(line,lineIndex) {
      if(!line.length) {
        if(finished(lineIndex,lines.length)) cb();
        return
      }
      var lineData = {};
      lineData.misc = [];
      var fields = line.split('\t');
      fields.forEach(function(field, i) {
        if(i == 0 && field.match(dateRegex)) {
          var match = field.match(dateRegex);
          lineData.time = new Date(Date.parse(match[2]+" "+match[1]+" "+new Date().getFullYear()+" "+match[3]+" "+TZ)).toISOString();
        } else if(i == 1 && ipRegex.test(field)) {
          lineData.device_ip = field;
        } else if(i == 2) {
          lineData.user = field;
        } else if(i == 3) {
          lineData.tty = field;
        } else if(i == 4) {
          lineData.client_ip = field;
        } else if(i == 5) {
          lineData.event = field;
        } else if(i == 6) {
          lineData.task_id = field;
        } else if((i == 7 || i == 8) && service_RE.test(field)) {
          lineData.service = field.match(service_RE)[1];
        } else if(cmd_RE.test(field)) {
          lineData.cmd = field.match(cmd_RE)[1];
        } else if(timezone_RE.test(field)) {
          lineData.timeZone = field.match(timezone_RE)[1];
        } else if(process_RE.test(field)) {
          lineData.process= field.match(process_RE)[1];
        } else if(elapsed_time_RE.test(field)) {
          lineData.elapsedTime= field.match(elapsed_time_RE)[1];
        } else if(discCause_RE.test(field)) {
          // Would be nice to add the description of this code
          // http://manpages.ubuntu.com/manpages/trusty/man5/tac_plus.conf.5.html
          lineData.discCause= field.match(discCause_RE)[1];
        } else if(preSessionTime_RE.test(field)) {
          lineData.preSessionTime= field.match(preSessionTime_RE)[1];
        } else if(privLvl_RE.test(field)) {
          lineData.preLvl = field.match(privLvl_RE)[1];
        } else {
          lineData.misc.push(field);
        }
      });
      log.info(lineData);
      if(finished(lineIndex,lines.length)) {
        cb();
      }
    });
  });
}

// create manta folders /$currentuser/stor/logs/tac_plus/$year/$month/$day/$hour/
function createMantaDir(client, files, cb) {
  var directory_RE = /.*\..*\.(\d{4})(\d{2})(\d{2})(\d{2})(.*)/
  vasync.forEachParallel({
    'func': function(file, cb) {
      var matches = file.match(directory_RE);
      var year = matches[1];
      var month = matches[2];
      var day = matches[3];
      var hour = matches[4];
      var directory = LOG_MANTA_PATH+year+'/'+month+'/'+day+'/'+hour;
      client.mkdirp(directory, function (err) {
        assert.ifError(err);
        cb();
      });
    },
    'inputs': files
  }, function(err, results) {
    cb();
  });
}

function putLogToManta(client, logs, cb) {
  vasync.forEachParallel({
    'func': function(log,cb) {
      var logFile = log.streams[0].path;
      var directory_RE = /.*\..*\.(\d{4})(\d{2})(\d{2})(\d{2})(.*)\..*/
      var matches = logFile.match(directory_RE);
      var year = matches[1];
      var month = matches[2];
      var day = matches[3];
      var hour = matches[4];
      var filename = HOSTNAME+'_'+matches[5];
      var mantaDir = LOG_MANTA_PATH+year+'/'+month+'/'+day+'/'+hour+'/'+filename+'.log';
      var file = fs.createReadStream(logFile,{encoding: 'utf8'})
      client.put(mantaDir, file, { type: 'text/plain' }, function (err) {
        assert.ifError(err);
        fs.unlink(logFile, function(err) {
          assert.ifError(err);
          cb();
        });
      });
    },
    'inputs': logs
  }, function(err, results) {
    cb();
  });
}

function finished(index, max) {
  return (index == (max -1));
}

function cleanUp(cb) {
  vasync.pipeline({arg: { }, funcs: [
    function createClient(args,cb) {
      var client = manta.createClient({
        sign: manta.privateKeySigner({
          key: fs.readFileSync(process.env.HOME + '/.ssh/id_rsa', 'utf8'),
          keyId: process.env.MANTA_KEY_ID,
          user: process.env.MANTA_USER
        }),
        user: process.env.MANTA_USER,
        url: process.env.MANTA_URL
      });
      args.client = client;
      cb();
    },
    function findAllFiles(args, cb) {
      args.files = [];
      args.client.rmr(LOG_MANTA_PATH,function(err) {
        if(err) cb(err);
        cb();
      });
    }
  ]}, function (err, results) {
    if(err) return cb(err);
    cb(null);
  });
}


module.exports = {
  start: start,
  startPipeline: startPipeline,
  config: config,
  cleanUp: cleanUp
};
