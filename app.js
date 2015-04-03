var Socrata = require('node-socrata');
var Cloudant = require('cloudant');
var CronJob = require('cron').CronJob;

//-- Cloudant settings
var password = process.env.CLOUDANTPASSWORD || process.argv[4];
var dbuser = process.env.CLOUDANTUSER || process.argv[3]; //'rajsingh'; // Set this to your own account
var dbname = process.env.CLOUDANTDB || process.argv[2]; //'crimes';
//-- end Cloudant

var express = require('express');
var app = express();
app.get('/hcb', harvestBostonCrimes);

var host = (process.env.VCAP_APP_HOST || 'localhost'); // The IP address of the Cloud Foundry DEA (Droplet
														// Execution Agent) that hosts this application:
var port = (process.env.VCAP_APP_PORT || 3000); // The port on the DEA for communication with the application:
app.listen(port, host); // Start server

//-- Repeating job to send messages
var thejob = new CronJob('05 * * * * *', harvestCrimes); // every minute
// var thejob = new CronJob('00 00 22 * * *', harvestCrimes); // every day at 8am GMT
thejob.start();

function harvestCrimes() {
	console.log('Harvesting crimes...');
}

/**
 * Queries Boston open data for yesterday's crimes and writes them to Cloudant
 */
function harvestSFCrimes() {
	var msg = '';
	var config = {
		hostDomain: 'https://data.sfgov.org', 
		resource: 'tmnf-yvry.json' 
	};
	var soda = new Socrata(config);

	// Configure the date query
	var d = new Date();
	var yest = new Date;
	yest.setDate(d.getDate() - 1);
	dstring = d.toISOString().slice(0, 10); d.getDate();
	yeststring = yest.toISOString().slice(0, 10);

	var where = "date>='" + yeststring + "' AND date<'" + dstring + "'";
	params = { 
		$select: ['incidntnum', 'category', 'date', 'time', 'x', 'y'],
		$where: where
	};

	Cloudant({account:dbuser, password:password}, function(er, cloudant) {
		if (er) return console.log('Error connecting to Cloudant account %s: %s', me, er.message);
		// specify the database we are going to use
		var thedb = cloudant.db.use(dbname);
		var logdb = cloudant.db.use(dbname+'_log');

		//-- get SF
		soda.get(params, function(err, response, data) {
			if ( err ) {
				errmsg = {time: new Date().getTime(), msg: "Query for SF data failed: "+where, app: 'crimedata.js->harvestSFCrimes()'};
				sendSMS(JSON.stringify(errmsg));
				logdb.insert(errmsg, function(err, body) {
					if ( err ) {
						console.log(err);
						console.log("Query for %s failed.", where);
					}
				});
			}

			// Even if we get data back, let's run through some sanity checks
			var mincrimes = 35;
			if ( data.length < mincrimes) {
				logdb.insert({
					time: new Date().getTime(), 
					msg: "POSSIBLE ERROR: less than "+mincrimes+" found for day "+where, 
					app: 'app.js'
				}, function(err, body) {
					if ( err ) {
						console.log("POSSIBLE ERROR: less than "+mincrimes+" found for day "+where);
					}
				});
			}
			// end sanity checks

			//-- Process each record received
			for (var i = 0; i < data.length; i++) {
				var thecrime = data[i];
				var thetime = new Date(thecrime.date);

				// create the document to insert
				var rec = {
					type: "Feature", 
					properties: {
						compnos: thecrime.incidntnum, 
						source: "sf", 
						main_crimecode: getCrimeCode(thecrime.category), 
						category: thecrime.category, 
						fromdate: thetime.getTime()
					},
					geometry: {
						type: "Point", 
						coordinates: [ parseFloat(thecrime.x), 
							parseFloat(thecrime.y)]
					}
				};

				thedb.insert(rec, function(err, body) {
					if ( err ) {
						logdb.insert({
							time: new Date().getTime(), 
							msg: "Error writing ID: " + rec.properties.compnos, 
							app: 'app.js'
						}, function(err, body) {
							if ( err ) {
								console.error("Error writing ID: " + rec.properties.compnos);
								console.error(err);
							}
						});
					}
				});
			} // end for each crime
			msg = "inserted " + data.length + "new SF crimes";
		}); // end soda.get
	}); // end Cloudant connect

	return("SF harvesting complete with message: "+msg);
} // end harvest SF

/**
 * Queries Boston open data for yesterday's crimes and writes them to Cloudant
 */
function harvestBostonCrimes(req, res) {
	var config = {
		hostDomain: 'http://data.cityofboston.gov', 
		resource: '7cdf-6fgx' 
		// XAppToken: process.env.SOCRATA_APP_TOKEN || 'registered-app-token'
	};
	var soda = new Socrata(config);

	// Configure the date query
	var d = new Date();
	var yest = new Date;
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
					app: 'app.js'
				};
				msg += JSON.stringify(errmsg);
				logdb.insert(errmsg, function(errmsg, body) {
					if ( err ) {
						console.log(errmsg);
						console.log("Error logging error message: " + JSON.stringify(errmsg));
					}
				});
			}

			// Even if we get data back, let's run through some sanity checks
			var mincrimes = 35;
			if ( data.length < mincrimes) {
				var fewcrimes = {
					time: new Date().getTime(), 
					msg: "POSSIBLE ERROR: less than "+mincrimes+" found for day "+where, 
					app: 'app.js'
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
			
			thedb.bulk({'docs':newcrimes}, null, function(err, body) {
				if ( err ) {
					logdb.insert({
						time: new Date().getTime(), 
						msg: "Error writing new crimes: "
					}, function(err, body) {
						if ( err ) {
							console.error("Error writing new crimes: ");
							console.error(err);
						}
					});
				}
				msg = "inserted " + newcrimes.length + " new Boston crimes";
				console.log("Boston harvesting complete with messages: "+msg);
			});

		}); // end soda.get		
	}); // end Cloudant connect
	if (res) res.send("Harvesting Boston crimes from Socrata...");
} // end harvest boston crimes
