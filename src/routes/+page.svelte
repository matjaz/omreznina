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
	let currentYear;
	let currentQuery = 'konice';

	let filterMocEnabled = false;
	let filterMoc = 5;
	let filterDatumEnabled = false;
	let filterDatumOd = `2020-01-01`;
	let filterDatumDo = '';
	let filterBlokEnabled = false;
	let filterBlok = '1';

	let SIPXLoaded = false;
	let SIPXAgg = 'montly';

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
		} catch (error) {
			status = 'Error loadDB'
			console.error('Failed to initialize database:', error);
		}
	}

	async function createTableFromBlob(name, blob) {
		try {
			const csv = await blob.text();
			await db.registerFileText(name, csv);
			await conn.query(`CREATE OR REPLACE TABLE '${name}' AS FROM '${name}'`);
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
		currentYear = res.toArray()[0]?.Leto.toString();
		filterDatumOd = `${currentYear}-01-01`;
		filterDatumDo = `${currentYear+1}-01-01`;
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
				return `SELECT count(*) "count", datediff('minutes', DATE '${currentYear}-01-01', DATE '${+currentYear+1}-01-01')/15 expected FROM '${currentTable}'${filterSQL}`;
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
			case 'sipx':
				let sql = `SELECT *,
energy_sum * SIPX_hr_price total_eur
FROM (
SELECT *,
(SELECT CASE
WHEN SIPX_hr_idx = 1 THEN SIPX1
WHEN SIPX_hr_idx = 2 THEN SIPX2
WHEN SIPX_hr_idx = 3 THEN SIPX3
WHEN SIPX_hr_idx = 4 THEN SIPX4
WHEN SIPX_hr_idx = 5 THEN SIPX5
WHEN SIPX_hr_idx = 6 THEN SIPX6
WHEN SIPX_hr_idx = 7 THEN SIPX7
WHEN SIPX_hr_idx = 8 THEN SIPX8
WHEN SIPX_hr_idx = 9 THEN SIPX9
WHEN SIPX_hr_idx = 10 THEN SIPX10
WHEN SIPX_hr_idx = 11 THEN SIPX11
WHEN SIPX_hr_idx = 12 THEN SIPX12
WHEN SIPX_hr_idx = 13 THEN SIPX13
WHEN SIPX_hr_idx = 14 THEN SIPX14
WHEN SIPX_hr_idx = 15 THEN SIPX15
WHEN SIPX_hr_idx = 16 THEN SIPX16
WHEN SIPX_hr_idx = 17 THEN SIPX17
WHEN SIPX_hr_idx = 18 THEN SIPX18
WHEN SIPX_hr_idx = 19 THEN SIPX19
WHEN SIPX_hr_idx = 20 THEN SIPX20
WHEN SIPX_hr_idx = 21 THEN SIPX21
WHEN SIPX_hr_idx = 22 THEN SIPX22
WHEN SIPX_hr_idx = 23 THEN SIPX23
WHEN SIPX_hr_idx = 24 THEN SIPX24
WHEN SIPX_hr_idx = 25 THEN SIPX25
WHEN SIPX_hr_idx = 34 THEN (SIPX3+SIPX4)/2
END FROM 'SIPX.csv' WHERE date_hr::date = "Datum dobave")::DOUBLE / 1000 SIPX_hr_price
FROM (
SELECT date_hr, energy_sum,
(
SELECT
CASE
WHEN SIPX25 IS NOT NULL THEN
 CASE WHEN hr = 2 THEN 34 WHEN hr > 2 THEN hr+2 ELSE hr+1 END
WHEN SIPX24 IS NULL THEN
 CASE WHEN hr > 2 THEN hr ELSE hr+1 END
ELSE hr+1
END
FROM 'SIPX.csv' WHERE date_hr::date = "Datum dobave") SIPX_hr_idx
FROM (
SELECT
LEFT(strftime(date_norm, '%c'), 13) date_hr,
extract('hour' FROM date_norm) hr,
SUM("Energija A+") energy_sum
FROM (
SELECT
(("Časovna značka" || ':00')::TIMESTAMP - interval 15 minutes) date_norm,
"Energija A+"
FROM '${currentTable}'
WHERE 1=1
${filterDatumEnabled && filterDatumOd ? `AND date_norm::DATE >= '${filterDatumOd}'` : ''}
${filterDatumEnabled && filterDatumDo ? `AND date_norm::DATE < '${filterDatumDo}'` : ''}
)
GROUP BY date_hr, hr
)
)
)
ORDER BY date_hr`;
			switch (SIPXAgg) {
				case 'daily':
					sql = `SELECT date_hr::DATE::TEXT "date", SUM(energy_sum) energy_sum, SUM(total_eur) total_eur FROM(
${sql}
)
GROUP BY "date"`;
					break;
				case 'montly':
					sql = `SELECT strftime(date_hr::DATE, '%Y-%m') "month", SUM(energy_sum) energy_sum, SUM(total_eur) total_eur FROM(
${sql}
)
GROUP BY "month"`;
					break;
					case 'yearly':
					sql = `SELECT strftime(date_hr::DATE, '%Y') "year", SUM(energy_sum) energy_sum, SUM(total_eur) total_eur FROM(
${sql}
)
GROUP BY "year"`;
					break;
			}
			return sql;
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
				filters.push(`"Časovna značka" < '${filterDatumDo}T00:00'`);
			}
		}
		currentQuery = name;
		query = getQuery(name, filters.join(' AND '));
		execute(query);
	}

	async function naloziBSP() {
		const res = await fetch('/omreznina/SIPX.csv');
		if (res.ok) {
			await createTableFromBlob('SIPX.csv', await res.blob());
			SIPXLoaded = true;
		}
	}

	status = '';

	$: if (files) {
		for (const file of files) {
			const { name } = file;
			if (name.slice(-4) === '.csv') {
				createTableFromBlob(name, file);
			} else {
				alert(`Nepravilna datoteka ${name}`);
			}
		}
	}

	$: if (tables) {
		const tbl = tables.rows.find(x => x.name.slice(0, 4) != 'SIPX');
		if (tbl && tbl.name != currentTable) {
			currentTable = tbl.name;
			executeNamed(currentQuery);
			fillMeta();
		}
	}

	// auto update on filter change
	$: {
		if (filterBlokEnabled || filterBlok || filterMocEnabled || filterMoc || filterDatumEnabled || filterDatumOd || filterDatumDo || SIPXAgg) {
			executeNamed(currentQuery);
		}
	};
</script>

<svelte:head>
	<title>Omrežnina</title>
</svelte:head>

<section>
	<h1>MojElektro.si 15min analiza</h1>

	<a href="https://mojelektro.si/" target="_blank">mojelektro.si</a> <a href="https://plesko.si/elektr/omreznina/csv/mojelektro/" target="_blank">15 minutna CSV datoteka</a>
	<input
		bind:files
		type="file"
		accept=".csv"
		title="Mojelektro 15 minutni podatki"
	/>
	{#if !SIPXLoaded && currentTable}	<p>
		Opcijsko <button on:click={naloziBSP}>naloži BSP (SIPX) podatke</button> <a href="https://www.bsp-southpool.com/slovenski-borzni-indeks-podatki.html" target="_blank">(vir)</a> in izračunaj možne prihranke z dinamičnimi cenami.
	</p>
	{/if}

	{#if currentTable}
	<p>Status: {status}</p>
	{/if}
	<!-- {#if tables}
	<p>Datoteke na voljo:</p>
	<ul>
		{#each tables.rows as t}
			<li>'{t.name}'</li>
		{/each}
	</ul>
	{/if} -->

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
	<br><br>
	Mojelektro:
	<button on:click={()=>{executeNamed('count')}}>Število zapisov</button>
	<button on:click={()=>{executeNamed('konice')}}>Izpis konic</button>
	<button on:click={()=>{executeNamed('luknje')}}>Luknje v podatkih</button>
	{#if SIPXLoaded}
	<br>
	SIPX:
	<button on:click={()=>{executeNamed('sipx')}}>Izračunaj SIPX zneske</button>
	<br>
	Agregacija:
	<select bind:value={SIPXAgg}>
		<option value="hourly">urno</option>
		<option value="daily">dnevno</option>
		<option value="montly">mesečno</option>
		<option value="yearly">letno</option>
	</select>
	<br>
	{/if}
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
