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
	let currentQuery = 'konice';

	let filterMocEnabled = false;
	let filterMoc = 5;
	let filterDatumEnabled = false;
	let filterDatumOd = `2020-01-01`;
	let filterDatumDo = '';
	let filterBlokEnabled = false;
	let filterBlok = '1';

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

	async function fillMeta() {
		const res = await conn.query(`SELECT Leto FROM '${currentTable}' LIMIT 1`);
		const year = res.toArray()[0]?.Leto.toString();
		filterDatumOd = `${year}-01-01`;
		filterDatumDo = `${year}-12-31`;
		tableColumns = await getColumns(currentTable);
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

	function getQuery(name, filterSQL = '') {
		if (filterSQL) {
			filterSQL = ` WHERE ${filterSQL}`
		}
		switch (name) {
			case 'count':
				return `SELECT count(*) c FROM '${currentTable}'${filterSQL}`;
			case 'konice':
				return `SELECT "Časovna značka",
LEAD("Časovna značka") OVER (ORDER BY "Časovna značka") AS naslednji,
strptime("Časovna značka", '%Y-%m-%dT%H:%M')::DATETIME AS time1,
strptime(naslednji, '%Y-%m-%dT%H:%M')::DATETIME AS time2,
date_diff('minutes', time1, time2) AS trajanje,
Blok,"P+ Prejeta delovna moč" FROM '${currentTable}'${filterSQL} ORDER BY "P+ Prejeta delovna moč" DESC LIMIT 5`;
			case 'luknje':
				return `SELECT * FROM (SELECT "Časovna značka",
LEAD("Časovna značka") OVER (ORDER BY "Časovna značka") AS naslednji,
strptime("Časovna značka", '%Y-%m-%dT%H:%M')::DATETIME AS time1,
strptime(naslednji, '%Y-%m-%dT%H:%M')::DATETIME AS time2,
date_diff('minutes', time1, time2) AS trajanje,
Blok,"P+ Prejeta delovna moč" FROM '${currentTable}'${filterSQL} ORDER BY "trajanje" DESC) WHERE trajanje > 15`;
		}
	}

	function executeNamed(name) {
		let filters = [];
		if (filterBlokEnabled) {
			filters.push(`Blok = '${filterBlok}'`);
		}
		if (filterMocEnabled) {
			filters.push(`"P+ Prejeta delovna moč" <= '${filterMoc}'`);
		}
		if (filterDatumEnabled) {
			if (filterDatumOd) {
				filters.push(`"Časovna značka" >= '${filterDatumOd}T00:15'`);
			}
			if (filterDatumDo) {
				filters.push(`"Časovna značka" <= '${filterDatumDo}T23:45'`);
			}
		}
		currentQuery = name;
		query = getQuery(name, filters.join(' AND '));
		execute(query);
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
			currentTable = tbl;
			executeNamed(currentQuery);
			fillMeta();
		}
	}

	// auto update on filter change
	$: {
		if (filterBlokEnabled || filterBlok || filterMocEnabled || filterMoc || filterDatumEnabled || filterDatumOd || filterDatumDo) {
			executeNamed(currentQuery);
		}
	};
</script>

<svelte:head>
	<title>Omrežnina</title>
</svelte:head>

<section>
	<h1>MojElektro.si 15min analiza</h1>

	mojelektro.si 15 minutni CSV datoteka
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
		<li on:click={(e)=>{getSelection().selectAllChildren(e.target);navigator.clipboard.writeText(`"${e.target.textContent}"`);}}>{column.name}</li>
		{/each}
	</ol>
	<p>Filter:</p>
		<label>
		<input bind:checked={filterBlokEnabled} type="checkbox">
		Filtriraj blok
	</label>
	<select bind:value={filterBlok}>
		<option value="1">1</option>
		<option value="2">2</option>
		<option value="3">3</option>
		<option value="4">4</option>
		<option value="5">5</option>
	</select>
	<label>
		<input bind:checked={filterMocEnabled} type="checkbox">
		Filtriraj moč večjo od
	</label>
	<input bind:value={filterMoc} type="number" step="0.1" class="filterMoc"><br>
	<label>
		<input bind:checked={filterDatumEnabled} type="checkbox">
		Filtriraj Datum
	</label>
	Od <input bind:value={filterDatumOd} type="date">
	Do <input bind:value={filterDatumDo} type="date">
	<br>
	<button on:click={()=>{executeNamed('count')}}>Število zapisov</button>
	<button on:click={()=>{executeNamed('konice')}}>Izpis konic</button>
	<button on:click={()=>{executeNamed('luknje')}}>Luknje v podatkih</button>
	<textarea bind:value={query} rows="8" cols="100" on:keydown={e=>{e.keyCode===13&&e.metaKey&&execute(query)}}></textarea><br>
	<button on:click={()=>{execute(query)}}>Poizvedba</button>
	{/if}

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
	.filterMoc {
		width: 50px;
	}
</style>
