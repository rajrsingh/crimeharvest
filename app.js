var Socrata = require('node-socrata');
var Cloudant = require('cloudant');
var CronJob = require('cron').CronJob;

//-- Cloudant settings
var password = process.env.CLOUDANTPASSWORD || process.argv[4];
var dbuser = process.env.CLOUDANTUSER || process.argv[3]; //'rajsingh'; // Set this to your own account
var dbname = process.env.CLOUDANTDB || process.argv[2]; //'crimes';
//-- end Cloudant

//-- Repeating job to send messages
var thejob = new CronJob('00 22 * * * *', harvestCrimes); // every day at about 3am GMT
thejob.start();

function harvestCrimes() {
	console.log('Harvesting crimes...');

	harvestSFCrimes();
	harvestBatonRougeCrimes(); 
	harvestChicagoCrimes();
}

/**
 * Queries Chicago open data for yesterday's crimes and writes them to Cloudant
 */
function harvestChicagoCrimes() {
	var city = 'Chicago';
	console.log('Harvesting ' + city + ' crimes...');
	var msg = '';
	var config = {
		hostDomain: 'https://data.cityofchicago.org/', 
		resource: '6zsd-86xi.json' 
	};
	var soda = new Socrata(config);

	// Configure the date query
	var d = new Date();
	var yest = new Date();
	yest.setDate(d.getDate() - 8);
	dstring = d.toISOString().slice(0, 10); d.getDate();
	yeststring = yest.toISOString().slice(0, 10);

	var where = "date>='" + yeststring + "' AND date<'" + dstring + "'";

	Cloudant({account:dbuser, password:password}, function(er, cloudant) {
		if (er) return console.log('Error connecting to Cloudant account %s: %s', me, er.message);

		var thedb = cloudant.db.use(dbname); // specify the database we are going to use
		var newcrimes = new Array();
		var params = { 
			$select: ['id', 'location', 'date', 'primary_type', 'fbi_code', 'iucr', 'description'], 
			$where: where, 
			$limit: 50000
		};

		soda.get(params, function(connecterr, response, data) {
			if ( connecterr ) {
				logMessage(city, connecterr);
				console.log(connecterr);
				return;
			}

			// Even if we get data back, let's run through some sanity checks
			var mincrimes = 15;
			if ( data.length < mincrimes) logMessage(city, "POSSIBLE ERROR: less than "+mincrimes+" found for day "+where);

			//-- Process each record received
			for (var i = 0; i < data.length; i++) {
				var thecrime = data[i];
				var crimedate = new Date(thecrime.date);

				// create the document to insert
				var rec = {
					_id: thecrime.id, 
					type: "Feature", 
					properties: {
						compnos: thecrime.id, 
						source: city, 
						type: thecrime.fbi_code,
						iucr: thecrime.iucr, 
						desc: thecrime.primary_type+'>'+thecrime.description,  
						timestamp: crimedate.getTime()
					}
				};

				// location
				if (thecrime.location && thecrime.location.coordinates) {
					rec.geometry = { type: "Point", coordinates: thecrime.location.coordinates};
				}

				newcrimes.push(rec);
			} // end for each crime

			thedb.bulk({'docs':newcrimes}, null, function(err4, body) {
				if ( err4 ) logMessage(city, "Error writing new crimes: "+err4);
			});

			logMessage(city, "SUCCESSFULLY inserted " + newcrimes.length + " new crimes");
		}); // end soda.get
	}); // end Cloudant connect
} // end harvest Chicago

/**
 * Queries BatonRouge open data for yesterday's crimes and writes them to Cloudant
 */
function harvestBatonRougeCrimes() {
	var city = 'BatonRouge';

	console.log('Harvesting ' + city + ' crimes...');
	var msg = '';
	var config = {
		hostDomain: 'https://data.brla.gov/', 
		resource: '5rji-ddnu.json' 
	};
	var soda = new Socrata(config);

	// Configure the date query
	var d = new Date();
	var yest = new Date();
	yest.setDate(d.getDate() - 1);
	dstring = d.toISOString().slice(0, 10); d.getDate();
	yeststring = yest.toISOString().slice(0, 10);

	var where = "offense_date>='" + yeststring + "' AND offense_date<'" + dstring + "'";

	Cloudant({account:dbuser, password:password}, function(er, cloudant) {
		if (er) return console.log('Error connecting to Cloudant account %s: %s', me, er.message);

		var thedb = cloudant.db.use(dbname); // specify the database we are going to use
		var newcrimes = new Array();
		var params = { 
			$select: ['file_number', 'geolocation', 'offense_date', 'offense_time', 'crime', 'offense'],
			$where: where, 
			$limit: 50000
		};

		soda.get(params, function(connecterr, response, data) {
			if ( connecterr ) {
				logMessage(city, connecterr);
				console.log(connecterr);
				return;
			}

			// Even if we get data back, let's run through some sanity checks
			var mincrimes = 3;
			if ( data.length < mincrimes) logMessage(city, "POSSIBLE ERROR: less than "+mincrimes+" found for day "+where);

			//-- Process each record received
			for (var i = 0; i < data.length; i++) {
				var thecrime = data[i];
				var crimedate = new Date(thecrime.offense_date);
				if (thecrime.offense_time && thecrime.offense_time.length > 3) {
					var h = thecrime.offense_time.substring(0, 2);
					var m = thecrime.offense_time.substring(2);
					crimedate.setHours(parseInt(h), parseInt(m));
				}

				// create the document to insert
				var rec = {
					_id: thecrime.file_number, 
					type: "Feature", 
					properties: {
						compnos: thecrime.file_number, 
						source: city, 
						type: thecrime.crime,
						desc: thecrime.offense_desc,  
						timestamp: crimedate.getTime()
					}
				};

				// location
				if (thecrime.geolocation && thecrime.geolocation.coordinates) {
					rec.geometry = { type: "Point", coordinates: thecrime.geolocation.coordinates};
				}

				newcrimes.push(rec);
			} // end for each crime

			thedb.bulk({'docs':newcrimes}, null, function(err4, body) {
				if ( err4 ) logMessage(city, "Error writing new crimes: "+err4);
			});

			logMessage(city, "SUCCESSFULLY inserted " + newcrimes.length + " new crimes");
		}); // end soda.get
	}); // end Cloudant connect
} // end harvest BatonRouge

/**
 * Queries SF open data for yesterday's crimes and writes them to Cloudant
 */
function harvestSFCrimes(options) {
	var city = 'SF';
	console.log('Harvesting ' + city + ' crimes...');
	var msg = '';
	var config = {
		hostDomain: 'https://data.sfgov.org', 
		resource: 'tmnf-yvry.json' 
	};
	var soda = new Socrata(config);

	// Configure the date query
	var d = new Date();
	var yest = new Date();
	yest.setDate(d.getDate() - 14);
	dstring = d.toISOString().slice(0, 10); d.getDate();
	yeststring = yest.toISOString().slice(0, 10);

	var where = "date>='" + yeststring + "' AND date<'" + dstring + "'";

	Cloudant({account:dbuser, password:password}, function(er, cloudant) {
		if (er) return console.log('Error connecting to Cloudant account %s: %s', me, er.message);

		var thedb = cloudant.db.use(dbname); // specify the database we are going to use
		var newcrimes = new Array();
		var params = { 
			$select: ['incidntnum', 'category', 'date', 'time', 'x', 'y'], 
			$where: where, 
			$limit: 50000
		};

		soda.get(params, function(err, response, data) {
			if ( err ) logMessage(city, err);

			// Even if we get data back, let's run through some sanity checks
			var mincrimes = 35;
			if ( data.length < mincrimes) logMessage(city, "POSSIBLE ERROR: less than "+mincrimes+" found for day "+where);

			//-- Process each record received
			for (var i = 0; i < data.length; i++) {
				var thecrime = data[i];
				var crimedate = new Date(thecrime.date);
				var hm = thecrime.time.split(':');
				crimedate.setHours(parseInt(hm[0]), parseInt(hm[1]));

				// create the document to insert
				var rec = {
					_id: thecrime.incidntnum, 
					type: "Feature", 
					properties: {
						compnos: thecrime.incidntnum, 
						source: city, 
						type: thecrime.category,
						desc: thecrime.descript,  
						timestamp: crimedate.getTime()
					}
				};

				// location
				var lon = thecrime.x;
				var lat = thecrime.y;
				if ( lon && lat ) {
					lon = parseFloat(lon);
					lat = parseFloat(lat);
					if ( lon >= -180 && lon <= 180 && lat >= -90 && lat <= 90 )
						rec.geometry = { type: "Point", coordinates: [ lon, lat ]};
				} else {
					console.error("LOCS: "+locs.toString());
				}

				newcrimes.push(rec);
			} // end for each crime

			thedb.bulk({'docs':newcrimes}, null, function(err4, body) {
				if ( err4 ) logMessage(city, "Error writing new crimes: "+err4);
			});
			msg = "inserted " + newcrimes.length + " new crimes";
			logMessage(city, msg);
		}); // end soda.get
	}); // end Cloudant connect
} // end harvest SF

function logMessage(cityname, messageinfo) {
	var msg = {
		time: new Date().getTime(),
		msg: cityname + ': ' + messageinfo,
		app: 'crimeharvest'
	};
	console.log(JSON.stringify(msg));
	Cloudant({account:dbuser, password:password}, function(er, cloudant) {
		var logdb = cloudant.db.use(dbname+'_log');
		logdb.insert(msg, function (err2, body) {
			if (err2) console.log('Error writing error to ' + dbname+'_log: ' + err2);
		});
	});
}

/**
 * Queries Boston open data for yesterday's crimes and writes them to Cloudant
 */
function harvestBostonCrimes() {
	console.log('Harvesting Boston crimes...');
	var config = {
		hostDomain: 'http://data.cityofboston.gov', 
		resource: '7cdf-6fgx' 
		// XAppToken: process.env.SOCRATA_APP_TOKEN || 'registered-app-token'
	};
	var soda = new Socrata(config);

	// Configure the date query
	var d = new Date();
	var yest = new Date();
	yest.setDate(d.getDate() - 1);
	dstring = d.toISOString().slice(0, 10); d.getDate();
	yeststring = yest.toISOString().slice(0, 10);

	var where = "fromdate>='" + yeststring + "' AND fromdate<'" + dstring + "'";
	// var where = "fromdate>='2014-11-07' AND fromdate<'2014-11-08'";
	params = { 
		$select: ['compnos', 'naturecode', 'fromdate', 'main_crimecode', 'location', 'domestic', 'shooting', 'reptdistrict', 'weapontype', 'reportingarea', 'streetname', 'day_week'],
		$where: where
	};

	Cloudant({account:dbuser, password:password}, function(er, cloudant) {
		if (er) return console.log('Error connecting to Cloudant account %s: %s', me, er.message);
		// specify the database we are going to use
		var thedb = cloudant.db.use(dbname);
		var logdb = cloudant.db.use(dbname+'_log');

		var newcrimes = new Array();
		//-- get Boston
		soda.get(params, function(err, response, data) {
			if ( err ) {
				errmsg = {
					time: new Date().getTime(), 
					msg: "Query for boston data failed: "+where, 
					app: 'crimeharvest'
				};
				console.log("crimeharvest query for boston data failed: "+where);
				logdb.insert(errmsg, function(err2, body) {
					if ( err2 ) {
						console.log("Error logging error message: " + err2);
					}
				});
			}

			// Even if we get data back, let's run through some sanity checks
			var mincrimes = 35;
			var msg = '';
			if ( data.length < mincrimes) {
				var fewcrimes = {
					time: new Date().getTime(), 
					msg: "POSSIBLE ERROR: less than "+mincrimes+" found for "+where, 
					app: 'crimeharvest'
				};
				msg += JSON.stringify(fewcrimes);
				logdb.insert(fewcrimes, function(err, body) {
					if ( err ) {
						console.log(fewcrimes.msg);
					}
				});
			}
			// end sanity checks

			//-- Process each record received
			for (var i = 0; i < data.length; i++) {
				var thecrime = data[i];
				// var thetime = new Date(thecrime.fromdate);

				// Create the document to insert
				var rec = {
					_id: thecrime.compnos,
					type: "Feature",
					properties: {
						compnos: thecrime.compnos,
						source: "boston", 
						naturecode: thecrime.naturecode,
						main_crimecode: thecrime.main_crimecode,
						domestic: (thecrime.domestic=='No'?false:true),
						shooting: (thecrime.shooting=='No'?false:true),
						reptdistrict: thecrime.reptdistrict,
						reptarea: thecrime.reptarea, 
						streetname: thecrime.streetname, 
						day_week: thecrime.day_week, 
						fromdate: thecrime.fromdate
					}
				};

				// location
				var lon = thecrime.location.longitude;
				var lat = thecrime.location.latitude;
				if ( lon && lat ) {
					lon = parseFloat(lon);
					lat = parseFloat(lat);
					if ( lon >= -180 && lon <= 180 && lat >= -90 && lat <= 90 )
						rec.geometry = { type: "Point", coordinates: [ lon, lat ]};
				} else {
					console.error("LOCS: "+locs.toString());
				}

				newcrimes.push(rec);
			} // end for each crime
			
			thedb.bulk({'docs':newcrimes}, null, function(err4, body) {
				if ( err4 ) {
					logdb.insert({
						time: new Date().getTime(), 
						msg: "Error writing new crimes: ", 
						err: err4
					}, function(err5, body) {
						if ( err5 ) {
							console.error(err5);
						}
					});
				}
				msg = "inserted " + newcrimes.length + " new Boston crimes";
				console.log("Boston harvesting complete with messages: "+msg);
			});

		}); // end soda.get		
	}); // end Cloudant connect
	// if (res) res.send("Harvesting Boston crimes from Socrata...");
} // end harvest boston crimes

//-- Twilio SMS sending service settings
var accountSid = 'ACa784a1107f8b9c468baa5541b92a1b3a';
var authToken = '5ddd13ecb64fff0f5f0537ab2cc33c3f';
if ( process.env.VCAP_SERVICES && process.env.VCAP_SERVICES['user-provided'] ) {
  accountSid = process.env.VCAP_SERVICES['user-provided'][0].credentials.accountSID;
  authToken = process.env.VCAP_SERVICES['user-provided'][0].credentials.authToken;
  console.log("here with accountSid=%s", accountSid);
}
var twilio = require('twilio')(accountSid, authToken);
//-- end Twilio

/**
 * Sends a text message to Raj.
 * With a user management system, and a non-trial Twilio account, this could be expanded to text others
 */
function sendAlert(req, res) {
	var msg = "SafetyPulse warning: you are approaching a high crime area rated: " + req.query.rating;
	// twilio.messages.create({
	//     body: msg,
	//     to: "+16176429372",
	//     from: "+16179103437"
	// }, function(err, message) {
	// 	if (err) res.send(err);
	//     res.send({"sent_message": msg, "message_SID": message.sid});
	// });
}

function sendSMS(msg) {
	twilio.messages.create({
	    body: msg,
	    to: "+16176429372",
	    from: "+16179103437"
	}, function(err, message) {
		if (err) 
			console.log(err);
	});
}

// var port = (process.env.VCAP_APP_PORT || 8192);
// var host = (process.env.VCAP_APP_HOST || 'localhost');
// var http = require('http');
// http.createServer(function(req, res) {
//   res.writeHead(200, {'Content-Type' : 'text/plain'});
//   res.end('Hello World\n');
// }).listen(port, host);
