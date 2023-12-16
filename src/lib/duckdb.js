import * as duckdb from '@duckdb/duckdb-wasm';
import duckdb_wasm from '@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url';
import duckdb_worker from '@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?worker';

let db;

const initDB = async () => {
	if (!db) {
        // Instantiate worker
        const logger = new duckdb.ConsoleLogger();
        const worker = new duckdb_worker();

        // and asynchronous database
        db = new duckdb.AsyncDuckDB(logger, worker);
        await db.instantiate(duckdb_wasm);
	}

	return db;
};

export {
    initDB,
    duckdb,
};
