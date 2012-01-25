var dgram  = require('dgram')
  , sys    = require('sys')
  , net    = require('net')
  , config = require('./config')

var counters = {};
var timers = {};
var numHits = 0;
var debugInt, flushInt, server, mgmtServer;
var startup_time = Math.round(new Date().getTime() / 1000);

var stats = {
  graphite: {
    last_flush: startup_time,
    last_exception: startup_time
  },
  messages: {
    last_msg_seen: startup_time,
    bad_lines_seen: 0,
  }
};

config.configFile(process.argv[2], function (config, oldConfig) {
  if (! config.debug && debugInt) {
    clearInterval(debugInt); 
    debugInt = false;
  }

  if (config.debug) {
    if (debugInt !== undefined) { clearInterval(debugInt); }
    debugInt = setInterval(function () { 
      sys.log("Counters:\n" + sys.inspect(counters) + "\nTimers:\n" + sys.inspect(timers));
    }, config.debugInterval || 10000);
  }

  if (server === undefined) {
    server = dgram.createSocket('udp4', function (msg, rinfo) {
      if (config.dumpMessages) { sys.log(msg.toString()); }
      var bits = msg.toString().split(':');
      var key = bits.shift()
                    .replace(/\s+/g, '_')
                    .replace(/\//g, '-')
                    .replace(/[^a-zA-Z_\-0-9\.]/g, '');

      if (bits.length == 0) {
        bits.push("1");
      }

      for (var i = 0; i < bits.length; i++) {
        var sampleRate = 1;
        var fields = bits[i].split("|");
        if (fields[1] === undefined) {
            sys.log('Bad line: ' + fields);
            stats['messages']['bad_lines_seen']++;
            continue;
        }
        if (fields[1].trim() == "ms") {
          if (! timers[key]) {
            timers[key] = [];
          }
          timers[key].push(Number(fields[0] || 0));
        } else {
          if (fields[2] && fields[2].match(/^@([\d\.]+)/)) {
            sampleRate = Number(fields[2].match(/^@([\d\.]+)/)[1]);
          }
          if (! counters[key]) {
            counters[key] = 0;
          }
          counters[key] += Number(fields[0] || 1) * (1 / sampleRate);
        }
        numHits += 1;
      }

      stats['messages']['last_msg_seen'] = Math.round(new Date().getTime() / 1000);
    });

    mgmtServer = net.createServer(function(stream) {
      stream.setEncoding('ascii');

      stream.on('data', function(data) {
        var cmd = data.trim();

        switch(cmd) {
          case "help":
            stream.write("Commands: stats, counters, timers, quit\n\n");
            break;

          case "stats":
            var now    = Math.round(new Date().getTime() / 1000);
            var uptime = now - startup_time;

            stream.write("uptime: " + uptime + "\n");

            for (group in stats) {
              for (metric in stats[group]) {
                var val;

                if (metric.match("^last_")) {
                  val = now - stats[group][metric];
                }
                else {
                  val = stats[group][metric];
                }

                stream.write(group + "." + metric + ": " + val + "\n");
              }
            }
            stream.write("END\n\n");
            break;

          case "counters":
            stream.write(sys.inspect(counters) + "\n");
            stream.write("END\n\n");
            break;

          case "timers":
            stream.write(sys.inspect(timers) + "\n");
            stream.write("END\n\n");
            break;

          case "quit":
            stream.end();
            break;

          default:
            stream.write("ERROR\n");
            break;
        }

      });
    });

    server.bind(config.port || 8125);
    mgmtServer.listen(config.mgmt_port || 8126);

    sys.log("server is up");

    var flushInterval = Number(config.flushInterval || 10000);

    flushInt = setInterval(function () {
      var statString = '';
      var ts = Math.round(new Date().getTime() / 1000);
      var numStats = 0;
      var key;

      for (key in counters) {
        if (counters[key] != null) {
          var value = counters[key] / (flushInterval / 1000);
          var message = 'stats.' + key + ' ' + value + ' ' + ts + "\n";
          message += 'stats_counts.' + key + ' ' + counters[key] + ' ' + ts + "\n";
          statString += message;
          counters[key] = null;
          numStats += 1;
        }
      }

      for (key in timers) {
        if (timers[key].length > 0) {
          var pctThreshold = config.percentThreshold || 90;
          var values = timers[key].sort(function (a,b) { return a-b; });

          var count = values.length;
          var min = values[0];
          var mean = min;
          var max = values[count - 1];
          var sum = min;

          var numInThreshold = 1;
          var meanInThreshold = min;
          var maxAtThreshold = max;
          var sumInThreshold = min;

          if (count > 1) {
            var thresholdIndex = Math.round(((100 - pctThreshold) / 100) * count);
            numInThreshold = count - thresholdIndex;
            maxAtThreshold = values[numInThreshold - 1];

            // average the remaining timings
            sumInThreshold = 0;
            for (var i = 0; i < numInThreshold; i++) {
              sumInThreshold += values[i];
            }

            sum = sumInThreshold;
            for (var i = numInThreshold; i < count; i++) {
              sum += values[i];
            }

            mean = sum / count;
            meanInThreshold = sumInThreshold / numInThreshold;
          }

          timers[key] = [];

          var message = "";

          message += 'stats.timers.' + key + '.count ' + count + ' ' + ts + "\n";
          message += 'stats.timers.' + key + '.lower ' + min + ' ' + ts + "\n";
          message += 'stats.timers.' + key + '.mean '  + mean + ' ' + ts + "\n";
          message += 'stats.timers.' + key + '.upper ' + max + ' ' + ts + "\n";
          message += 'stats.timers.' + key + '.sum '   + sum + ' ' + ts + "\n";

          message += 'stats.timers.' + key + '.count_' + pctThreshold + ' ' + numInThreshold + ' ' + ts + "\n";
          message += 'stats.timers.' + key + '.mean_'  + pctThreshold + ' ' + meanInThreshold + ' ' + ts + "\n";
          message += 'stats.timers.' + key + '.upper_' + pctThreshold + ' ' + maxAtThreshold + ' ' + ts + "\n";
          message += 'stats.timers.' + key + '.sum_'   + pctThreshold + ' ' + sumInThreshold + ' ' + ts + "\n";

          statString += message;

          numStats += 1;
        }
      }

      statString += 'statsd.numStats ' + numStats + ' ' + ts + "\n";
      statString += 'statsd.numHits ' + numHits + ' ' + ts + "\n";
      numHits = 0;

      //sys.log(statString);
      
      if (config.graphiteHost) {
        try {
          var graphite = net.createConnection(config.graphitePort, config.graphiteHost);
          graphite.addListener('error', function(connectionException){
            if (config.debug) {
              sys.log(connectionException);
            }
          });
          graphite.on('connect', function() {
            this.write(statString);
            this.end();
            stats['graphite']['last_flush'] = Math.round(new Date().getTime() / 1000);
          });
        } catch(e){
          if (config.debug) {
            sys.log(e);
          }
          stats['graphite']['last_exception'] = Math.round(new Date().getTime() / 1000);
        }
      }

    }, flushInterval);
  }

});

