var fs = require('fs');
var path = require('path');
var bunyan = require('bunyan');
var logDir = '/var/log'
var vasync = require('vasync');

// Create the manata clients
var assert = require('assert');
var fs = require('fs');
var manta = require('manta');

var client = manta.createClient({
  sign: manta.privateKeySigner({
    key: fs.readFileSync(process.env.HOME + '/.ssh/id_rsa', 'utf8'),
    keyId: process.env.MANTA_KEY_ID,
    user: process.env.MANTA_USER
  }),
  user: process.env.MANTA_USER,
  url: process.env.MANTA_URL
});
// End of manta client

vasync.pipeline({arg: { }, funcs: [
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
    createMantaDir(args.files, function() {
      cb();
    });
  },
  function f5 (args, cb) {
    putLogToManta(args.logs, function() {
      cb();
    });
  }
], function(err, results) {
  assert.ifError(err);
}});


// 1. Find files that match the tac_plus acct log, and create an array
function getFiles(cb) {
  var tacPlusRegex = /tac_plus\.acct\..*/
  fs.readdir(logDir, 'utf8', function (err, files) {
    if(err) return;
    var results = []
    files.map(function(file) {
      if(file = file.match(tacPlusRegex)) {
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
    logs.push(bunyan.createLogger({ name: file,
      streams: [
        {
          path: path.join(process.env.TMPDIR,file+'.bunyan')
        }
      ]
    }));
  });
  cb(logs);
}

// 3. read each file and create the bunyan file
function loopLines(files,logs,cb) {
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

    fs.readFile(file,'utf8', function (err, data) {
      var lines = data.split('\n');
      lines.forEach(function(lines,lineIndex) {
        if(!lines.length) return
        var lineData = {};
        lineData.misc = [];
        var fields = lines.split('\t');
        fields.forEach(function(field, i) {
          if(i == 0 && field.match(dateRegex)) {
            var match = field.match(dateRegex);
            lineData.time = new Date(Date.parse(match[2]+" "+match[1]+" "+new Date().getFullYear()+" "+match[3])).toISOString();
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
        if(lineIndex == (file.length - 1)){
          cb()
        }
      });
    });
}

// create folders $user/tac_plus/$year/$month/$day/$hour.log(bunyan format)
function createMantaDir(files, cb) {
  var directory_RE = /.*\..*\.(\d{4})(\d{2})(\d{2})(\d{2})(.*)/
  files.forEach(function(file, i) {
    var matches = file.match(directory_RE);
    var directory = '/'+process.env.MANTA_USER+'/stor/'+matches[1]+'/'+matches[2]+'/'+matches[3]+'/'+matches[4];
    client.mkdirp(directory, function (err) {
      assert.ifError(err);
      if(i == (files.length -1 )) {
        cb();
      }
    });
  });
}

function putLogToManta(logs, cb) {
  logs.forEach(function(log, i) {
    var logFile = log.streams[0].path;
    var directory_RE = /.*\..*\.(\d{4})(\d{2})(\d{2})(\d{2})(.*)\..*/
    var matches = logFile.match(directory_RE);
    var year = matches[1];
    var month = matches[2];
    var day = matches[3];
    var hour = matches[4];
    var filename = matches[5]; // should add hostname infront of this
    var mantaDir = '/'+process.env.MANTA_USER+'/stor/'+year+'/'+month+'/'+day+'/'+hour+'/'+filename+'.log';
    var file = fs.createReadStream(logFile,{encoding: 'utf8'})
    client.put(mantaDir, file, { type: 'text/plain' }, function (err) {
      assert.ifError(err);
      fs.unlink(logFile, function(err) {
              
      });
    });
  });
}
