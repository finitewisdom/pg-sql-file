/* global describe, it */

import assert from "assert";
import _path from "path";
import query from "../query.js";
import { fileURLToPath } from "url";

let reporterFnCount = 0;
let isCacheableFnCount = 0;
let cacheHitCount = 0;
let logQueryNameCount = 0;
let logQueryResultCount = 0;

function reporterFn( err, message ) {
    if ( err ) {
        //  do nothing
    }
    // console.log( message );
    cacheHitCount += ( message.startsWith( "query.js/getFromQueryCache: found, name" ) ? 1 : 0 );
    logQueryNameCount += ( message.startsWith( "query.js/getFromQueryFile: name" ) ? 1 : 0 );
    logQueryResultCount += ( message.startsWith( "query.js/getFromQueryFile: results" ) ? 1 : 0 );
    reporterFnCount += 1;
}

function isCacheableFn( name ) {
    isCacheableFnCount += 1;
    if ( name === "get-uuid-count" || name === "insert-uuid" ) {
        return false;
    } else {
        return true;
    }
}

const filename = fileURLToPath( import.meta.url );
const initOptions = {
    "connectionInfo": {
        "ssl": {
            "rejectUnauthorized": false
        }
    },
    "reporterFn": reporterFn,
    "isCacheableFn": isCacheableFn,
    "log": {
        "queries": true,
        "results": true
    },
    "sqlDirectory": _path.join( _path.dirname( filename ), "..", "test" ),
    "enableCache": true
};

// eslint-disable no-undef

describe( "query.js", function() {

    describe( "init()", function() {

        //  "should not throw an error"
        it( "should not throw an error", async function() {
            try {
                await query.init( initOptions );
            } catch ( err ) {
                assert.equal( err, null );
            }
        } );
    } );

    describe( "row counts", function() {

        //  "'get-all-states': should return 51 rows"
        it( "'get-all-states': should return 51 rows", async function() {
            try {
                const rows = await query.execute( "get-all-states", {}, true );
                assert.equal( rows.length, 51 );
            } catch ( err ) {
                assert.equal( err, null );
            }
        } );

        //  "'get-one-state': should return 1 row"
        it( "'get-one-state': should return 1 row", async function() {
            try {
                const rows = await query.execute( "get-one-state", { "code": "AZ" }, true );
                assert.equal( rows.length, 1 );
            } catch ( err ) {
                assert.equal( err, null );
            }
        } );

        //  "'get-all-states-starting-with', { 'term': 'a' }: should only return names starting with 'A' or 'a'"
        it( "'get-all-states-starting-with', { 'term': 'a' }: should only return names starting with 'A' or 'a'", async function() {
            try {
                const rows = await query.execute( "get-all-states-starting-with", { "term": "a" }, true );
                rows.forEach( ( row ) => {
                    assert.equal( row.name[ 0 ], "A" );
                } );
            } catch ( err ) {
                assert.equal( err, null );
            }
        } );

        //  "'get-all-states-starting-with', { 'term': 'b' }: should return zero rows"
        it( "'get-all-states-starting-with', { 'term': 'b' }: should return zero rows", async function() {
            try {
                const rows = await query.execute( "get-all-states-starting-with", { "term": "b" }, true );
                assert.equal( rows.length, 0 );
            } catch ( err ) {
                assert.equal( err, null );
            }
        } );

        //  "'get-all-states-starting-with', { 'term': 'new' }: should return 4 rows"
        it( "'get-all-states-starting-with', { 'term': 'new' }: should return 4 rows", async function() {
            try {
                const rows = await query.execute( "get-all-states-starting-with", { "term": "new" }, true );
                assert.equal( rows.length, 4 );
            } catch ( err ) {
                assert.equal( err, null );
            }
        } );

        //  "'get-all-states-containing', { 'term': 'w' }: should return 12 rows"
        it( "'get-all-states-containing', { 'term': 'w' }: should return 12 rows", async function() {
            try {
                const rows = await query.execute( "get-all-states-containing", { "term": "w" }, true );
                assert.equal( rows.length, 12 );
            } catch ( err ) {
                assert.equal( err, null );
            }
        } );

        //  "'get-selected-states', { 'codes': [ 'AZ', 'CA', 'VA', 'YY' ] }: should return 3 rows"
        it( "'get-selected-states', { 'codes': [ 'AZ', 'CA', 'VA', 'YY' ] }: should return 3 rows", async function() {
            try {
                const rows = await query.execute( "get-selected-states", { "codes": [ "AZ", "CA", "VA", "YY" ] }, true );
                assert.equal( rows.length, 3 );
            } catch ( err ) {
                assert.equal( err, null );
            }
        } );
    } );

    describe( "transactions", function() {

        //  "query.beginTransaction(), query.commitTransaction(): should succeed"
        it( "query.beginTransaction(), query.commitTransaction(): should succeed", async function() {
            try {
                const client = await query.beginTransaction();
                await query.commitTransaction( client );
            } catch ( err ) {
                assert.equal( err, null );
            }
        } );

        //  "query.beginTransaction(), query.rollbackTransaction(): should succeed"
        it( "query.beginTransaction(), query.rollbackTransaction(): should succeed", async function() {
            try {
                const client = await query.beginTransaction();
                await query.rollbackTransaction( client );
            } catch ( err ) {
                assert.equal( err, null );
            }
        } );

        //  "query.commitTransaction(): should fail"
        it( "query.commitTransaction(): should fail", async function() {
            try {
                await query.commitTransaction();
            } catch ( err ) {
                assert.equal( Boolean( err ), true );
            }
        } );

        //  "query.rollbackTransaction(): should fail"
        it( "query.rollbackTransaction(): should fail", async function() {
            try {
                await query.rollbackTransaction();
            } catch ( err ) {
                assert.equal( Boolean( err ), true );
            }
        } );

        //  "query.beginTransaction(), query.execute('insert-uuid'), commitTransaction(): should persist uuid"
        it( "query.beginTransaction(), query.execute('insert-uuid'), commitTransaction(): should persist uuid", async function() {
            try {
                const client = await query.beginTransaction();
                const rows1 = await query.execute( "insert-uuid", null, true, client );
                assert.equal( rows1.length, 1 );
                await query.commitTransaction( client );
                const rows2 = await query.execute( "get-one-uuid", { "uuid": rows1[ 0 ].uuid }, true );
                assert.equal( rows2.length, 1 );
                assert.equal( rows1[ 0 ].uuid, rows2[ 0 ].uuid );
            } catch ( err ) {
                assert.equal( err, null );
            }
        } );

        //  "query.beginTransaction(), query.execute('insert-uuid'), rollbackTransaction(): should *not* persist uuid"
        it( "query.beginTransaction(), query.execute('insert-uuid'), rollbackTransaction(): should *not* persist uuid", async function() {
            try {
                const client = await query.beginTransaction();
                const rows1 = await query.execute( "insert-uuid", null, true, client );
                assert.equal( rows1.length, 1 );
                await query.rollbackTransaction( client );
                const rows2 = await query.execute( "get-one-uuid", { "uuid": rows1[ 0 ].uuid }, false );
                assert.equal( rows2.length, 0 );
            } catch ( err ) {
                assert.equal( err, null );
            }
        } );
    } );

    describe( "query.execute('invalid', null, bool)", function() {

        //  "bool = true: should throw an error"
        it( "bool = true: should throw an error", async function() {
            try {
                await query.execute( "invalid", null, true );
                assert.fail( "query.execute should have failed" );
            } catch ( err ) {
                assert.notEqual( err, null );
            }
        } );

        //  "bool = false: should not throw an error"
        it( "bool = false: should not throw an error", async function() {
            try {
                const rows = await query.execute( "invalid", null, false );
                assert.equal( rows.length, 0 );
            } catch ( err ) {
                assert.equal( err, null );
            }
        } );
    } );

    describe( "reporterFn", function() {

        //  "should increment call count"
        it( "should increment call count", async function() {
            try {
                const reporterFnCountBefore = reporterFnCount;
                await query.execute( "get-one-state", { "code": "AZ" }, true );
                assert( reporterFnCount > reporterFnCountBefore );
            } catch ( err ) {
                assert.equal( err, null );
            }
        } );
    } );

    describe( "isCacheableFn", function() {

        //  "should increment call count"
        it( "should increment call count", async function() {
            try {
                const isCacheableFnCountBefore = isCacheableFnCount;
                await query.execute( "get-one-state", { "code": "AZ" }, true );
                assert( isCacheableFnCount > isCacheableFnCountBefore );
            } catch ( err ) {
                assert.equal( err, null );
            }
        } );

        //  "get-uuid-count-cacheable should return cached value"
        it( "get-uuid-count-cacheable should return cached value", async function() {
            try {
                let rows;
                const client = await query.beginTransaction();
                rows = await query.execute( "get-uuid-count-cacheable", null, true, client );
                const countBefore = rows[ 0 ].count;
                rows = await query.execute( "insert-uuid", null, true, client );
                await query.commitTransaction( client );
                rows = await query.execute( "get-uuid-count-cacheable", null, true );
                const countAfter = rows[ 0 ].count;
                assert.equal( countBefore, countAfter );
            } catch ( err ) {
                assert.equal( err, null );
            }
        } );
    } );

    describe( "enableCache = false", function() {

        //  "should not cache get-uuid-count-cacheable"
        it( "should not cache get-uuid-count-cacheable", async function() {
            try {
                const newInitOptions = Object.assign( initOptions, { "enableCache": false } );
                await query.init( newInitOptions );
                let rows;
                const client = await query.beginTransaction();
                rows = await query.execute( "get-uuid-count-cacheable", null, true, client );
                const countBefore = rows[ 0 ].count;
                rows = await query.execute( "insert-uuid", null, true, client );
                await query.commitTransaction( client );
                rows = await query.execute( "get-uuid-count-cacheable", null, true );
                const countAfter = rows[ 0 ].count;
                assert.equal( countBefore + 1, countAfter );
            } catch ( err ) {
                assert.equal( err, null );
            }
            await query.init( initOptions );
        } );

    } );

    describe( "cache", function() {

        //  "cache('clear') should clear the cache"
        it( "cache('clear') should clear the cache", async function() {
            try {
                const rows = await query.execute( "get-uuid-count-cacheable", null, true );
                const cacheHitCountBefore = cacheHitCount;
                query.cache( "clear" );
                const cacheHitCountAfter = cacheHitCount;
                assert.equal( cacheHitCountBefore, cacheHitCountAfter );
            } catch ( err ) {
                assert.equal( err, null );
            }
        } );

        //  "cache('invalid') should throw an error"
        it( "cache('invalid') should throw an error", async function() {
            try {
                query.cache( "invalid" );
                assert.ok( false, "should have thrown an error" );
            } catch ( err ) {
                assert.notEqual( err, null );
            }
        } );

    } );

    describe( "log", function() {

        //  "log.queries = false: should not increment query name counter"
        it( "log.queries = false: should not increment query name counter", async function() {
            try {
                const newInitOptions = Object.assign( initOptions, { "log": { "queries": false, "results": false } } );
                await query.init( newInitOptions );
                const logQueryNameCountBefore = logQueryNameCount;
                await query.execute( "get-uuid-count", null, true );
                assert.equal( logQueryNameCountBefore, logQueryNameCount );
            } catch ( err ) {
                assert.equal( err, null );
            }
            await query.init( initOptions );
        } );

        //  "log.queries = true: should increment query name counter"
        it( "log.queries = true: should increment query name counter", async function() {
            try {
                const newInitOptions = Object.assign( initOptions, { "log": { "queries": true, "results": false } } );
                await query.init( newInitOptions );
                const logQueryNameCountBefore = logQueryNameCount;
                await query.execute( "get-uuid-count", null, true );
                assert.equal( logQueryNameCount > logQueryNameCountBefore, true );
            } catch ( err ) {
                assert.equal( err, null );
            }
            await query.init( initOptions );
        } );

        //  "log.results = false: should not increment query result counter"
        it( "log.results = false: should not increment query result counter", async function() {
            try {
                const newInitOptions = Object.assign( initOptions, { "log": { "queries": true, "results": false } } );
                await query.init( newInitOptions );
                const logQueryResultCountBefore = logQueryResultCount;
                await query.execute( "get-uuid-count", null, true );
                assert.equal( logQueryResultCountBefore, logQueryResultCount );
            } catch ( err ) {
                assert.equal( err, null );
            }
            await query.init( initOptions );
        } );

        //  "log.results = true: should increment query results counter"
        it( "log.results = true: should increment query results counter", async function() {
            try {
                const newInitOptions = Object.assign( initOptions, { "log": { "queries": true, "results": true } } );
                await query.init( newInitOptions );
                const logQueryResultCountBefore = logQueryResultCount;
                await query.execute( "get-uuid-count", null, true );
                assert.equal( logQueryResultCount > logQueryResultCountBefore, true );
            } catch ( err ) {
                assert.equal( err, null );
            }
            await query.init( initOptions );
        } );
    } );

    describe( "exit()", function() {

        //  "should not throw an error"
        it( "should not throw an error", async function() {
            try {
                await query.exit();
            } catch ( err ) {
                assert.equal( err, null );
            }
        } );
    } );
} );
