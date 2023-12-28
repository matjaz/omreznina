<script>
	import { onMount } from "svelte";
	import { initDB } from "$lib/duckdb";

	// https://github.com/duckdb-wasm-examples/sveltekit-typescript/blob/main/src/routes/%2Bpage.svelte
	// https://github.com/sekuel/duckdb-wasm-codemirror-tabulator-svelte/blob/main/src/routes/%2Bpage.svelte

	let db;
	let conn;

	let files;
	let tables;
	let currentTable;
	let tableColumns;

	let results;
	let status;

	let query;

	onMount(loadDB);

	async function loadDB() {
		try {
			status = 'Instantiating DuckDB...'
			db = await initDB();
			conn = await db.connect();
			if (conn) {
				status = 'DuckDB Instantiated'
				window.conn = conn;
			}
			return conn;
		} catch (error) {
			console.error('Failed to initialize database:', error);
			throw error;
		}
	}

	async function createTableFromFiles(file) {
		if (file.name.slice(-4) !== '.csv') {
			alert('Nepravilna datoteka');
			return;
		}
		try {
			const csv = await file.text();
			await db.registerFileText(file.name, csv);
			await conn.query(`CREATE OR REPLACE TABLE '${file.name}' AS FROM '${file.name}'`);
			tables = await getTables();
			window.tables = tables;
		} catch (error) {
		}
	}

	async function getTables() {
		const res = await conn.query('SHOW TABLES');
		return {
			rows: res.toArray().map((r) => r.toJSON()),
			columns: res.schema.fields
		};
	}

	async function getColumns(table) {
		const query = 'SELECT column_name AS name FROM duckdb_columns() WHERE table_name = ? ORDER BY column_index DESC';
		const stmt = await conn.prepare(query);
		const res = await stmt.query(table);
		return res.toArray();
	}

	async function execute(query) {
		status = 'Executing query...';
		try {
			let startTime = Date.now();
			const res = await conn.query(query);

			const {fields} = res.schema;
			const rows = res.toArray().map((row) => row.toArray());

			results = {
				fields,
				rows,
			};

			const executionTime = Date.now() - startTime
			status = `Query executed in ${executionTime} ms`;
		} catch (error) {
			status = 'Error'
		}
	}

	status = '';

	$: if (files) {
		for (const file of files) {
			createTableFromFiles(file);
		}
	}

	$: if (tables) {
		const tbl = tables.rows[0].name;
		if (tbl != currentTable) {
			query = `SELECT "Časovna značka",
LEAD("Časovna značka") OVER (ORDER BY "Časovna značka") AS naslednji,
strptime("Časovna značka", '%Y-%m-%dT%H:%M')::DATETIME AS time1,
strptime(naslednji, '%Y-%m-%dT%H:%M')::DATETIME AS time2,
date_diff('minutes', time1, time2) AS trajanje,
Blok,"P+ Prejeta delovna moč" FROM '${tbl}' ORDER BY "P+ Prejeta delovna moč" DESC LIMIT 5`;
			currentTable = tbl;
			(async () => {
				tableColumns = await getColumns(tbl);
			})();
			execute(query);
		}
	}
</script>

<svelte:head>
	<title>Omrežnina</title>
</svelte:head>

<section>
	<h1>Elektro omrežnina</h1>

	15 minutni mojelektro.si podatki
	<input
		bind:files
		type="file"
		accept=".csv"
		title="Mojelektro 15 minutni podatki"
	/>
	<!-- {#await tables then table}
	<p>Datoteke na voljo:</p>
	<ul>
		{#each table.rows as t}
			<li>'{t.name}'</li>
		{/each}
	</ul>
	{/await} -->

	<p>Status: {status}</p>

	{#if tableColumns}
	<p>Stolpci:</p>
	<ol>
		{#each tableColumns as column}
		<li>{column.name}</li>
		{/each}
	</ol>
	{/if}
	<textarea bind:value={query} rows="8" cols="100" on:keydown={e=>{e.keyCode===13&&e.metaKey&&execute(query)}}></textarea><br>
	<button on:click={()=>{execute(query)}}>Poizvedba</button>

	{#if results}
	<p>Rezultat:</p>
	<table border="1">
		<tr>
		{#each results.fields as col}
			<td>{col.name}</td>
		{/each}
		</tr>
		{#each results.rows as row}
			<tr>
				{#each row as val}
				<td>{val}</td>
				{/each}
			</tr>
		{/each}
	</table>
	{/if}
</section>

<style>
	/* table {
		border: 1px solid black;
	} */
</style>
