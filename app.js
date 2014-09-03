var fs = require('fs');

var influx = require('influx');
var mysql = require('mysql');
var config = require('./secrets').config;
var async = require('async');
var format = require('string-template')

var icon = influx(config.influx);
var gcon = mysql.createConnection(config.gratia.url);
/*
icon.createDatabase('123', function(err) {
    if(err) throw err;
});
*/

/*
//http://stackoverflow.com/questions/2280104/convert-javascript-to-date-object-to-mysql-date-format-yyyy-mm-dd
Date.prototype.toYMD = Date_toYMD;
function Date_toYMD() {
    var year, month, day;
    year = String(this.getFullYear());
    month = String(this.getMonth() + 1);
    if (month.length == 1) {
        month = "0" + month;
    }
    day = String(this.getDate());
    if (day.length == 1) {
        day = "0" + day;
    }
    return year + "-" + month + "-" + day;
}
*/

//http://stackoverflow.com/questions/5129624/convert-js-date-time-to-mysql-datetime
function twoDigits(d) {
    if(0 <= d && d < 10) return "0" + d.toString();
    if(-10 < d && d < 0) return "-0" + (-1*d).toString();
    return d.toString();
}
Date.prototype.toMysqlFormat = function() {
    return this.getUTCFullYear() + "-" + twoDigits(1 + this.getUTCMonth()) + "-" + twoDigits(this.getUTCDate()) + " " + twoDigits(this.getUTCHours()) + ":" + twoDigits(this.getUTCMinutes()) + ":" + twoDigits(this.getUTCSeconds());
};

var now = new Date();
console.log("running etl at "+now.toString());

function getyesterday() {
    var time = new Date();
    time.setDate(time.getDate() - 1); 
    time.setHours(0,0,0,0);
    return time; 
}

//time to query data for.. summary data are posted 1 per day.
var start_time, end_time;
if(config.start_time) {
    start_time = new Date(Date.parse(config.start_time));
} else {
    start_time = getyesterday();
}
if(config.end_time) {
    end_time = new Date(Date.parse(config.end_time));
} else {
    end_time = getyesterday();
}
//console.log("start:"+start_time.toString());
//console.log("end:"+end_time.toString());

//console.log("Doing ETL for following date/time");
//console.dir(times);

function create_times(start_time, end_time, duration) {
    //create date lists
    //var duration = 60*60*1000; //1 hour window
    var times = [];
    while(start_time <= end_time) {
        times.push(new Date(start_time));
        start_time = new Date(start_time.getTime() + duration);
    }
    return times;
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// main execution block here
//
gcon.connect(function(err) {
    if(err) throw err;

    async.series([/*do_24h,*/ do_1h], function(err) {
        if(err) throw err;
        console.log("all done");
        gcon.end();
    });
});

function do_24h(cb) {
    console.log("running 24 hour queries");
    var duration = 24*60*60*1000;
    times = create_times(start_time, end_time, duration);

    async.eachSeries(times,
    function(start_time, next_date) {
        var end_time = new Date(start_time.getTime() + duration);
        async.series([
            function(next) {
                etl(start_time, end_time, "vo_summary.sql", "vo_summary", next);
            },
            function(next) {
                etl(start_time, end_time, "site_summary.sql", "site_summary", next);
            },
            function(next) {
                etl(start_time, end_time, "project_summary.sql", "project_summary", next);
            },
            /*
            function(next) {
                etl(start_time, end_time, "vo.sql", "vo", next);
            },
            function(next) {
                etl(start_time, end_time, "site.sql", "site", next);
            },
            function(next) {
                etl(start_time, end_time, "project.sql", "project", next);
            },
            function(next) {
                etl(start_time, end_time, "user.sql", "user", next);
            },
            */
        ], function(err) {
            if(err) throw err;
            next_date();
        });
    }, cb);
}

function do_1h(cb) {
    console.log("running 1 hour queries");
    var duration = 60*60*1000;
    times = create_times(start_time, end_time, duration);

    async.eachSeries(times,
    function(start_time, next_date) {
        var end_time = new Date(start_time.getTime() + duration);
        async.series([
            /*
            function(next) {
                etl(start_time, end_time, "vo.sql", "vo", next);
            },
            function(next) {
                etl(start_time, end_time, "site.sql", "site", next);
            },
            function(next) {
                etl(start_time, end_time, "project.sql", "project", next);
            },
            */
            function(next) {
                etl(start_time, end_time, "user.sql", "user", next);
            },
        ], function(err) {
            if(err) throw err;
            next_date();
        });
    }, cb);
}

function getquery(template_name, vars, cb) {
    fs.readFile(__dirname+'/template/'+template_name, function(err, template) {
        if(err) return cb(err);
        var query = format(template.toString(), vars);
        cb(null, query);
    });
}

//group small groups into "other" group
function group(points) {
    var good = [];
    var other = null;
    points.forEach(function(point) {
        if(point.jobs < 1000) {
            if(other == null) { 
                other = point;
                other.name = "others";
            } else {
                other.jobs += point.jobs;
                other.wall_sec += point.wall_sec;
                other.user_sec += point.user_sec;
                other.system_sec += point.system_sec;
            }
        } else {
            good.push(point);
        }
    });
    if(other != null) {
        good.push(other);
    }
    //console.dir(good);
    return good;
}

function etl(start_time, end_time, template, series, done) {
    console.log("Processing "+series+" between "+start_time.toString() + " and " + end_time.toString());
    
    //DO E in ETL
    getquery(template, {starttime: start_time.toMysqlFormat(), endtime: end_time.toMysqlFormat()}, function(err, query) {
        console.log(start_time.toMysqlFormat());
        console.log(query);
        gcon.query(query, function(err, rows) {
            if(err) return done(err);
            var points = [];
            rows.forEach(function(row) {
                //DO T in ETL
                points.push({
                    time: start_time, //let's use start_time of window for "end time"
                    name: row.name,
                    //gratia stores sec in double.. convert to int for clearner data.
                    wall_sec: parseInt(row.wall_sec),
                    user_sec: parseInt(row.user_sec),
                    system_sec: parseInt(row.system_sec),
                    jobs: row.jobs
                });
            });

            //this is just a hack until we can figure out a way to do this via influxDB/grafana
            points = group(points);
            console.dir(points);

            //DO L in ETL
            icon.writePoints(series, points, function(err) {
                if(err) return done(err);
                console.log("posted data for "+series);
                done();
            });
        });
    });
}
/*
function randompoint(vo) {
    return {
        vo: vo,
        wall_hour: Math.random()*3600,
        cpu_user_hour: Math.random()*1800,
        cpu_system_hour: Math.random()*1800,
        jobs: Math.random()*100,
        time: new Date()
    };
}

icon.writePoint('vo', randompoint('cms'), function(err) {
    if(err) throw err;
});
icon.writePoint('vo', randompoint('osg'), function(err) {
    if(err) throw err;
});
icon.writePoint('vo', randompoint('atlas'), function(err) {
    if(err) throw err;
});
*/
