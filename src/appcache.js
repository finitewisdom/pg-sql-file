import { hasProperty } from "dot-prop";

let gReporterFn = null;
let gEnabled = false;

function debug( err, message ) {
    if ( typeof gReporterFn === "function" ) {
        gReporterFn( err, message );
    }
}

function init( options ) {

    gReporterFn = options.reporterFn;
    gEnabled = options.enabled;

    if ( !global.locals ) {
        global.locals = {};
    }
    global.locals.appcache = false;

    if ( gEnabled ) {
        global.locals.appcache = {
            "rowLimit": options.rowLimit,
            "map": new Map(),
            "size": 0
        };
    }

    debug( null, "appcache.js/init: enabled = " + gEnabled );
}

function inited() {
    return hasProperty( global, "locals.appcache" );
}

function enabled() {
    return gEnabled;
}

function clear() {
    if ( enabled() ) {
        global.locals.appcache.map = new Map();
        debug( null, "appcache.js/clear: cache cleared" );
    }
}

function set( key, value ) {
    if ( enabled() ) {
        global.locals.appcache.map.set( key, value );
    } else {
        debug( null, "appcache.js/set: appcache not enabled" );
    }
}

function get( key ) {

    let value;

    if ( enabled() ) {
        value = global.locals.appcache.map.get( key );
        return value;
    } else {
        debug( null, "appcache.js/get: appcache not enabled" );
        return null;
    }
}

function keys() {

    let arr;

    if ( enabled() ) {
        arr = global.locals.appcache.map.keys();
    } else {
        debug( null, "appcache.js/keys: appcache not enabled" );
        arr = [];
    }

    return arr;
}

const appcache = {
    init,
    inited,
    enabled,
    clear,
    set,
    get,
    keys
};

export default appcache;
