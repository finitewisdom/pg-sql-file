import pkg from "pg";
const { Pool, types } = pkg;

let gConnectionInfo;
let gPool;
let gReporterFn;

function debug( err, message ) {
    if ( typeof gReporterFn === "function" ) {
        gReporterFn( err, message );
    }
}

async function init( options ) {
    await exit();
    gConnectionInfo = options.connectionInfo;
    gReporterFn = options.reporterFn;
}

async function exit() {
    if ( gPool ) {
        await gPool.end();
        debug( null, "postgres.js/exit: pool has ended" );
        gPool = null;
    }
}

function getPool() {

    if ( !gPool ) {

        types.setTypeParser( /* INT8 */ 20, ( value ) => {
            return parseInt( value, 10 );
        } );

        types.setTypeParser( /* FLOAT8 */ 701, ( value ) => {
            return parseFloat( value );
        } );

        types.setTypeParser( /* NUMERIC */ 1700, ( value ) => {
            return parseFloat( value );
        } );

        gPool = new Pool( gConnectionInfo );
    }

    return gPool;
}

async function beginTransaction( throwErr ) {

    try {
        const pool = getPool();
        const client = await pool.connect();
        await client.query( "begin transaction" );
        return client;
    } catch ( err ) {
        //  log error
        debug( err, "postgres.js/beginTransaction: error" );
        //  rethrow error?
        if ( throwErr === true ) {
            throw err.message;
        } else {
            return null;
        }
    }
}

async function commitTransaction( client, throwErr ) {

    try {
        await client.query( "commit transaction" );
        client.release();
    } catch ( err ) {
        //  log error
        debug( err, "postgres.js/commitTransaction: error" );
        //  rethrow error?
        if ( throwErr === true ) {
            throw err.message;
        }
    }
}

async function rollbackTransaction( client, throwErr ) {

    try {
        await client.query( "rollback transaction" );
        client.release();
    } catch ( err ) {
        //  log error
        debug( err, "postgres.js/rollbackTransaction: error" );
        //  rethrow error?
        if ( throwErr === true ) {
            throw err.message;
        }
    }
}

async function query( q, values, throwErr, client ) {

    const myPool = getPool();
    const executor = client || myPool;
    const queryObj = {
        "text": q,
        "values": ( values ? values : [] )
    };

    let res;

    try {
        //  see https://node-postgres.com/features/queries#query-config-object
        res = await executor.query( queryObj );
    } catch ( err ) {
        //  log error
        debug( err, `postgres.js/query: executor.query error, queryObj = ${ JSON.stringify( queryObj ) }` );
        //  rethrow error?
        if ( throwErr === true ) {
            throw err.message;
        } else {
            res = { "rows": [] };
        }
    }

    return res.rows;
}

function encodeArray( inArray ) {

    let outArray = JSON.stringify( inArray );

    outArray = outArray.replace( /^\[/, "{" );
    outArray = outArray.replace( /\]$/, "}" );

    return outArray;
}

const postgres = {
    init,
    exit,
    query,
    beginTransaction,
    commitTransaction,
    rollbackTransaction,
    encodeArray
};

export default postgres;
