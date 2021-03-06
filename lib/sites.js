/**
 * Crime sites to be harvested should be added to the exported sites array with
 * the appropriate fields included:
 * 
 *  city: string - name of the city, region, etc. represented in the data
 *  host: string - domain hosting the data
 *  resource: string
 *  select: array of strings - the fields from the data to harvest
 *  where: function() - returns the clause to filter the data
 *  process: function(site, data) - returns the transformed data to be stored
 */

var categorization = require('./categorization.js');

var philly = {
  city: 'Philly',
  host: 'https://data.phila.gov', 
  resource: 'sspu-uyfa.json',
  select: ['dc_key', 'shape', 'dispatch_date_time', 'ucr_general', 'text_general_code'],
  where: function() {
    var today = new Date();
    var yesterday = new Date();
    yesterday.setDate(today.getDate() - 1);
    var todayStr = today.toISOString().slice(0, 10);
    var yesterdayStr = yesterday.toISOString().slice(0, 10);
    return 'dispatch_date_time>="' + yesterdayStr + '" AND dispatch_date_time<"' + todayStr + '"';
  },
  process: function(site, data) {
    var newcrimes = [];
    for (var i = 0; i < data.length; i++) {
      var thecrime = data[i];
      var crimedate = new Date(thecrime.dispatch_date_time);
      var catcode = isNaN(thecrime.ucr_general) ? thecrime.ucr_general.toUpperCase() : thecrime.ucr_general;

      // create the document to insert
      var rec = {
        _id: site.city + thecrime.dc_key, 
        type: 'Feature', 
        properties: {
          compnos: thecrime.dc_key, 
          source: site.city, 
          type: thecrime.ucr_general,
          desc: thecrime.text_general_code,  
          timestamp: crimedate.getTime(), 
          updated: Date.now(),
          CDSNV: categorization.CDSNV.Philly.indexOf(catcode) > -1,
          CDSDV: categorization.CDSDV.Philly.indexOf(catcode) > -1,
          CDSSTREET: categorization.CDSSTREET.Philly.indexOf(catcode) > -1
        }
      };

      // location
      if (thecrime.shape && thecrime.shape.coordinates) {
        rec.geometry = {
          type: 'Point',
          coordinates: thecrime.shape.coordinates
        };
      }

      newcrimes.push(rec);
    }
    return newcrimes;
  }
};

var chicago = {
  city: 'Chicago',
  host: 'https://data.cityofchicago.org/',
  resource: '6zsd-86xi.json',
  select: ['id', 'location', 'date', 'primary_type', 'fbi_code', 'iucr', 'description'],
  where: function() {
    var today = new Date();
    var yesterday = new Date();
    yesterday.setDate(today.getDate() - 8);
    var todayStr = today.toISOString().slice(0, 10);
    var yesterdayStr = yesterday.toISOString().slice(0, 10);
    return 'date>="' + yesterdayStr + '" AND date<"' + todayStr + '"';
  },
  process: function(site, data) {
    var newcrimes = [];
    for (var i = 0; i < data.length; i++) {
      var thecrime = data[i];
      var crimedate = new Date(thecrime.date);
      var catcode = isNaN(thecrime.iucr) ? thecrime.iucr.toUpperCase() : thecrime.iucr;

      // create the document to insert
      var rec = {
        _id: site.city + thecrime.id, 
        type: 'Feature', 
        properties: {
          compnos: thecrime.id, 
          source: site.city, 
          type: thecrime.fbi_code,
          iucr: thecrime.iucr, 
          desc: thecrime.primary_type + '>' + thecrime.description,  
          timestamp: crimedate.getTime(), 
          updated: Date.now(),
          CDSNV: categorization.CDSNV.Chicago.indexOf(catcode) > -1,
          CDSDV: categorization.CDSDV.Chicago.indexOf(catcode) > -1,
          CDSSTREET: categorization.CDSSTREET.Chicago.indexOf(catcode) > -1
        }
      };

      // location
      if (thecrime.location && thecrime.location.coordinates) {
        rec.geometry = {
          type: 'Point',
          coordinates: thecrime.location.coordinates
        };
      }

      newcrimes.push(rec);
    }
    return newcrimes;
  }
};

var batonrouge = {
  city: 'BatonRouge',
  host: 'https://data.brla.gov/', 
  resource: '5rji-ddnu.json',
  select: ['file_number', 'geolocation', 'offense_date', 'offense_time', 'crime', 'offense'],
  where: function() {
    var today = new Date();
    var yesterday = new Date();
    yesterday.setDate(today.getDate() - 1);
    var todayStr = today.toISOString().slice(0, 10);
    var yesterdayStr = yesterday.toISOString().slice(0, 10);
    return 'offense_date>="' + yesterdayStr + '" AND offense_date<"' + todayStr + '"';
  },
  process: function(site, data) {
    var newcrimes = [];
    for (var i = 0; i < data.length; i++) {
      var thecrime = data[i];
      var crimedate = new Date(thecrime.offense_date);
      if (thecrime.offense_time && thecrime.offense_time.length > 3) {
        var h = thecrime.offense_time.substring(0, 2);
        var m = thecrime.offense_time.substring(2);
        crimedate.setHours(parseInt(h), parseInt(m));
      }
      var catcode = isNaN(thecrime.crime) ? thecrime.crime.toUpperCase() : thecrime.crime;

      // create the document to insert
      var rec = {
        _id: site.city + thecrime.file_number, 
        type: 'Feature', 
        properties: {
          compnos: thecrime.file_number, 
          source: site.city, 
          type: thecrime.crime,
          desc: thecrime.offense_desc,  
          timestamp: crimedate.getTime(), 
          updated: Date.now(),
          CDSNV: categorization.CDSNV.BatonRouge.indexOf(catcode) > -1,
          CDSDV: categorization.CDSDV.BatonRouge.indexOf(catcode) > -1,
          CDSSTREET: categorization.CDSSTREET.BatonRouge.indexOf(catcode) > -1
        }
      };

      // location
      if (thecrime.geolocation && thecrime.geolocation.coordinates) {
        rec.geometry = {
          type: 'Point',
          coordinates: thecrime.geolocation.coordinates
        };
      }

      newcrimes.push(rec);
    }
    return newcrimes;
  }
};

var sf = {
  city: 'SF',
  host: 'https://data.sfgov.org', 
  resource: 'tmnf-yvry.json',
  select: ['incidntnum', 'category', 'date', 'time', 'x', 'y'],
  where: function() {
    var today = new Date();
    var yesterday = new Date();
    yesterday.setDate(today.getDate() - 14);
    var todayStr = today.toISOString().slice(0, 10);
    var yesterdayStr = yesterday.toISOString().slice(0, 10);
    return 'date>="' + yesterdayStr + '" AND date<"' + todayStr + '"';
  },
  process: function(site, data) {
    var newcrimes = [];
    for (var i = 0; i < data.length; i++) {
      var thecrime = data[i];
      var crimedate = new Date(thecrime.date);
      var hm = thecrime.time.split(':');
      crimedate.setHours(parseInt(hm[0]), parseInt(hm[1]));
      var catcode = isNaN(thecrime.category) ? thecrime.category.toUpperCase() : thecrime.category;

      // create the document to insert
      var rec = {
        _id: site.city + thecrime.incidntnum, 
        type: 'Feature', 
        properties: {
          compnos: thecrime.incidntnum, 
          source: site.city, 
          type: thecrime.category,
          desc: thecrime.descript,
          timestamp: crimedate.getTime(), 
          updated: Date.now(),
          CDSNV: categorization.CDSNV.SF.indexOf(catcode) > -1,
          CDSDV: categorization.CDSDV.SF.indexOf(catcode) > -1,
          CDSSTREET: categorization.CDSSTREET.SF.indexOf(catcode) > -1
        }
      };

      // location
      var lon = thecrime.x;
      var lat = thecrime.y;
      if ( lon && lat ) {
        lon = parseFloat(lon);
        lat = parseFloat(lat);
        if ( lon >= -180 && lon <= 180 && lat >= -90 && lat <= 90 ) {
            rec.geometry = {
              type: 'Point',
              coordinates: [lon, lat]
            };
        }
      }
      else {
        console.error('LOCS: '+locs.toString());
      }

      newcrimes.push(rec);
    }
    return newcrimes;
  }
};


module.exports = [philly, chicago, batonrouge, sf];
