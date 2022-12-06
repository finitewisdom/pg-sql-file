import _fs from "fs";
import _path from "path";
import _process from "process";

import appcache from "./src/appcache.js";
import postgres from "./src/postgres.js";

const fileCache = {};

let gConnectionInfo;
let gReporterFn;
let gIsCacheableFn;
let gLog;
let gSqlDirectory;
let gEnableCache;

function debug( err, message ) {
    if ( typeof gReporterFn === "function" ) {
        gReporterFn( err, message );
    }
}

function abbreviate( s, length = 100 ) {
    if ( s.length > length ) {
        const suffix = "… (" + s.length + " chars)";
        return s.substring( 0, length - suffix.length ) + suffix;
    } else {
        return s;
    }
}

function isCacheable( name ) {
    if ( name === "begin transaction" || name === "rollback transaction" || name === "commit transaction" ) {
        return false;
    } else if ( typeof gIsCacheableFn === "function" ) {
        return gIsCacheableFn( name );
    } else {
        return true;
    }
}

async function init( options ) {

    gConnectionInfo = options.connectionInfo;
    gReporterFn = options.reporterFn;
    gIsCacheableFn = options.isCacheableFn;
    gLog = options.log || {};
    gSqlDirectory = options.sqlDirectory;
    gEnableCache = options.enableCache;

    await postgres.init( {
        "connectionInfo": gConnectionInfo,
        "reporterFn": gReporterFn
    } );

    appcache.init( {
        "reporterFn": gReporterFn,
        "enabled": gEnableCache
    } );
}

async function exit() {
    await postgres.exit();
}

function getKey( name, parameters ) {

    let key;
    let qs;

    if ( parameters && typeof parameters === "object" ) {
        qs = Object
            .keys( parameters )
            .sort()
            .filter( function( param ) {
                return ( param !== "_" && param !== "q" );
            } )
            .map( function( param ) {
                return encodeURIComponent( param ) + "=" + encodeURIComponent( parameters[ param ] );
            } )
            .join( "&" );
    }

    key = name;
    if ( qs ) {
        key += "?" + qs;
    }

    return key;
}

function getFromQueryCache( name, parameters ) {

    let key;
    let rows;

    debug( null, `query.js/getFromQueryCache: name = ${ name }, appcache enabled = ${ appcache.enabled() }` );

    if ( appcache.enabled() ) {

        key = getKey( name, parameters );
        debug( null, `query.js/getFromQueryCache: key = ${ abbreviate( JSON.stringify( key ) ) }` );

        rows = appcache.get( key );
        if ( rows ) {
            debug( null, `query.js/getFromQueryCache: name = ${ name }, row count = ${ rows.length }` );
            if ( rows.length > 0 ) {
                debug( null, `query.js/getFromQueryCache: rows[0] = ${ abbreviate( JSON.stringify( rows[ 0 ] ) ) }` );
            }
        }

        if ( !rows ) {
            debug( null, `query.js/getFromQueryCache: name = ${ name }, no rows returned` );
        } else if ( Array.isArray( rows ) ) {
            // debug( null, `query.js/getFromQueryCache: name = ${ name }, row count = ${ String( rows.length ) }` );
        } else if ( typeof rows === "object" ) {
            // debug( null, `query.js/getFromQueryCache: name = ${ name }, returned object = ${ JSON.stringify( rows ) }` );
        } else {
            debug( null, `query.js/getFromQueryCache: name = ${ name }, returned type = ${ typeof rows }` );
        }
    }

    return rows;
}

function setInQueryCache( name, parameters, rows ) {

    let key;

    debug( null, `query.js/setInQueryCache: name = ${ name }, appcache enabled = ${ appcache.enabled() }` );
    if ( appcache.enabled() ) {

        key = getKey( name, parameters );
        appcache.set( key, rows );
        debug( null, `query.js/setInQueryCache: key = ${ abbreviate( JSON.stringify( key ) ) }` );
    }
}

async function getFromQueryFile( name, parameters, throwError, client ) {

    const filespec = _path.join( gSqlDirectory, name + ".sql" );
    const substitutions = [];

    let index = 1;
    let q;
    let regex;
    let rows;
    let t0;

    function compareForDescSort( a, b ) {
        if ( a > b ) {
            return -1;
        } else if ( a < b ) {
            return 1;
        } else {
            return 0;
        }
    }

    try {

        //  get the raw query
        if ( fileCache[ name ] ) {
            q = fileCache[ name ];
        } else {
            q = _fs.readFileSync( filespec, { "encoding": "utf8" } );
            fileCache[ name ] = q;
        }

        //  substitute any parameters
        if ( parameters ) {
            Object.keys( parameters ).sort( compareForDescSort ).forEach( function( key ) {
                if ( q.includes( "$$" + key ) ) {
                    regex = new RegExp( "\\$\\$" + key, "gi" );
                    q = q.replace( regex, parameters[ key ] );
                } else if ( q.includes( "$" + key ) ) {
                    regex = new RegExp( "\\$" + key, "gi" );
                    q = q.replace( regex, "$" + index );
                    substitutions.push( parameters[ key ] );
                    index += 1;
                }
            } );
        }

        //  logging before query execution
        const qRegexMatches = ( !gLog.regex || ( new RegExp( gLog.regex ).test( name ) ) );

        if ( gLog.queries === true && qRegexMatches ) {
            debug( null, `query.js/getFromQueryFile: name = "${ name }", query = \n${ q }, substitutions = ${ substitutions.join( " | " ) }` );
            t0 = _process.hrtime.bigint();
        }

        //  query execution
        rows = await postgres.query( q, substitutions, throwError === true, client );

        //  logging after query execution
        if ( gLog.queries === true && qRegexMatches ) {
            const time = `${ Math.round( Number( _process.hrtime.bigint() - t0 ) / 1000 / 1000 ) }ms`;
            debug( null, `query.js/getFromQueryFile: name = "${ name }", ${ rows ? rows.length : "no" } rows returned (${ time })` );
            if ( gLog.results === true ) {
                debug( null, `query.js/getFromQueryFile: results = ${ JSON.stringify( rows ) }` );
            }
        }

        //  catch results, if so configured
        if ( isCacheable( name ) ) {
            setInQueryCache( name, parameters, rows );
        }

    } catch ( e ) {
        debug( e, `query.js/getFromQueryFile: name = "${ name }", query =\n${ q }, substitutions = ${ substitutions.join( " | " ) }` );
        rows = [];
        if ( throwError === true ) {
            throw e;
        }
    }

    return rows;
}

async function execute( name, parameters, throwOnError, client ) {

    const start = _process.hrtime.bigint();

    let rows;
    let time;

    if ( isCacheable( name ) ) {
        rows = getFromQueryCache( name, parameters );
        time = `${ Math.round( Number( _process.hrtime.bigint() - start ) / 1000 / 1000 ) }ms`;
        debug( null, `query.js/query: getFromQueryCache, name = "${ name }", elapsed time = ${ time }` );
        if ( gLog.results === true ) {
            debug( null, `query.js/getFromQueryFile: results = ${ JSON.stringify( rows ) }` );
        }
}

    if ( !rows ) {
        rows = await getFromQueryFile( name, parameters, throwOnError, client );
        time = `${ Math.round( Number( _process.hrtime.bigint() - start ) / 1000 / 1000 ) }ms`;
        debug( null, `query.js/query: getFromQueryFile, name = "${ name }", elapsed time = ${ time }` );
    }

    return rows;
}

//  ===================================================
//  transaction methods
//  ===================================================

async function beginTransaction( throwError = true ) {
    const client = await postgres.beginTransaction( throwError === true );
    return client;
}

async function rollbackTransaction( client, throwError = true ) {
    await postgres.rollbackTransaction( client, throwError === true );
}

async function commitTransaction( client, throwError = true ) {
    await postgres.commitTransaction( client, throwError === true );
}

const query = {
    init,
    exit,
    execute,
    beginTransaction,
    rollbackTransaction,
    commitTransaction
};

export default query;
