<script>
	import { onMount } from "svelte";
	import { initDB } from "$lib/duckdb";

	// https://github.com/duckdb-wasm-examples/sveltekit-typescript/blob/main/src/routes/%2Bpage.svelte
	// https://github.com/sekuel/duckdb-wasm-codemirror-tabulator-svelte/blob/main/src/routes/%2Bpage.svelte

	let db;
	let conn;

	let files;
	let tables;
	let results;
	let status;

	onMount(async () => {
		loadDB();
	});

	async function loadDB() {
		try {
			status = 'Instantiating DuckDB...'
			db = await initDB();
			conn = await db.connect();
			if (conn) {
				status = 'DuckDB Instantiated'
				window.conn = conn;
			}
			tables = getTables();
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
			await conn.query(`CREATE OR REPLACE TABLE '${file.name}' AS (SELECT * FROM '${file.name}')`);
			tables = await getTables();
			window.tables = tables;
		} catch (error) {
			results = Promise.reject(error);
		}
	}

	async function getTables() {
		const res = await conn.query('SHOW TABLES');
		return {
			rows: res.toArray().map((r) => Object.fromEntries(r)),
			columns: res.schema.fields
		};
	}

	async function execute(query) {
		status = 'Executing query...';
		try {
			let startTime = Date.now();
			results = conn.query(query);
			const res = await results;

			const {fields} = res.schema;
			const rows = res.toArray().map((row) => row.toArray());

			results = Promise.resolve({
				fields,
				rows,
			});

			const executionTime = Date.now() - startTime
			status = `Query executed in ${executionTime} ms`;
		} catch (error) {
			results = Promise.reject(error);
			status = 'Error'
		}
		return results;
	}
	window.execute = execute;

	$: tables = Promise.resolve({});
	$: results = Promise.resolve({});

	status = '';

	$: if (files) {
		for (const file of files) {
			createTableFromFiles(file);
		}
	}

	$: if (tables.rows) {
		const tbl = tables.rows[0].name;
		const query = `select "Časovna značka",Blok,"P+ Prejeta delovna moč" from '${tbl}' ORDER BY "P+ Prejeta delovna moč" DESC LIMIT 5`;
		execute(query)
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

	{#await results}
	<p>Nalagam</p>
	{:then result}
	<table border="1">
		<tr>
		{#each result.fields as col}
			<td>{col.name}</td>
		{/each}
		</tr>
		{#each result.rows as row}
			<tr>
				{#each row as val}
				<td>{val}</td>
				{/each}
			</tr>
		{/each}
	</table>
	{/await}
</section>

<style>
	/* table {
		border: 1px solid black;
	} */
</style>
