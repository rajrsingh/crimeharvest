var Cloudant = require('cloudant');
var CronJob = require('cron').CronJob;
var Socrata = require('node-socrata');
var SOCRATA_MAX_RECORDS = 50000;

//-- Cloudant settings
var password = process.env.CLOUDANTPASSWORD || process.argv[4];
var dbuser = process.env.CLOUDANTUSER || process.argv[3]; //'rajsingh'; // Set this to your own account
var dbname = process.env.CLOUDANTDB || process.argv[2]; //'crimes';
//-- end Cloudant

var sites = require('./lib/sites.js');

//-- Repeating job to send messages
var thejob = new CronJob('00 00 22 * * *', harvestCrimes); // every day at about 3am GMT
thejob.start();
harvestCrimes();

function harvestCrimes() {
	if (sites && sites.length > 0) {
		console.log('Harvesting crimes from %d location(s)', sites.length);

		Cloudant({ account: dbuser, password: password }, function(err, cloudant) {
			if (err) {
				console.error('Error connecting to Cloudant account:', err.message);
			}
			else {
				console.log('Connecting with Cloudant db:', dbname);
				// specify the database we are going to use
				var thedb = cloudant.db.use(dbname); 

				sites.forEach(function(site, index) {
					harvestSite(site, thedb);
				});
			}
		}); //end Cloudant connect
	}
	else {
		console.warn('No sites found or available for harvesting');
	}
}

function harvestSite(site, thedb) {
	console.log('Harvesting %s crimes...', site.city);

	// Configure the where clause
	var where = site.where();

	var params = { 
		$select: site.select, 
		$where: where, 
		$limit: SOCRATA_MAX_RECORDS
	};

	var soda = new Socrata({
		hostDomain: site.host,
		resource: site.resource
	});

	soda.get(params, function(connecterr, response, data) {
		if (connecterr) {
			logMessage(site.city, connecterr);
			console.log(connecterr);
		}
		else {
			// Even if we get data back, let's run through some sanity checks
			var mincrimes = 15;
			if ( data.length < mincrimes) logMessage(site.city, 'POSSIBLE ERROR: less than ' + mincrimes + ' found for day ' + where);

			var newcrimes = site.process(site, data);

			thedb.bulk({ 'docs': newcrimes }, null, function(error, body) {
				if (error) logMessage(site.city, 'Error writing new crimes: ' + error);
			});

			logMessage(site.city, 'SUCCESSFULLY inserted ' + newcrimes.length + ' new crimes: ' + where);
		}
	}); //end soda get
}

function logMessage(cityname, messageinfo) {
	var msg = {
		time: new Date().getTime(),
		msg: cityname + ': ' + messageinfo,
		app: 'crimeharvest'
	};
	console.log(JSON.stringify(msg));
	// send Raj an SMS
	// sendSMS(JSON.stringify(msg));

	Cloudant({account:dbuser, password:password}, function(er, cloudant) {
		var logdb = cloudant.db.use(dbname+'_log');
		logdb.insert(msg, function (err2, body) {
			if (err2) console.log('Error writing error to ' + dbname+'_log: ' + err2);
		});
	});
}

//-- Twilio SMS sending service settings
var accountSid = '';
var authToken = '';
var TWILIO_CRIMEHARVEST_MESSAGINGSERVICESID = ''
if ( process.env.VCAP_SERVICES && process.env.VCAP_SERVICES['user-provided'] ) {
  accountSid = process.env.VCAP_SERVICES['user-provided'][0].credentials.accountSID;
  authToken = process.env.VCAP_SERVICES['user-provided'][0].credentials.authToken;
  TWILIO_CRIMEHARVEST_MESSAGINGSERVICESID = process.env.VCAP_SERVICES['user-provided'][0].credentials.messagingServiceSid;
//   console.log("here with accountSid=%s", accountSid);
}
var twilio = require('twilio')(accountSid, authToken);
//-- end Twilio

/**
 * Sends a text message to Raj.
 * With a user management system, and a non-trial Twilio account, this could be expanded to text others
 */
function sendSMS(msg) {
	twilio.sendMessage({
		messagingServiceSid: '',
	    to: "+1",
	    body: msg
	}, function(err, message) {
		if (err) 
			console.log(err);
	});
}

