const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');
const cron = require('node-cron');
const unzipper = require('unzipper');
const { createClient } = require('@clickhouse/client');

// Step 1: Connect to default DB just for setup
const rawClickhouse = createClient({
  url: 'http://localhost:8123',
  username: 'default',
  password: '',
  database: 'default',
});

let clickhouse; // Will be initialized after DB is created

// Create database and table if not exists
async function setupDatabase() {
  // Create the 'domain_downloads' database
  await rawClickhouse.query({
    query: `CREATE DATABASE IF NOT EXISTS domain_downloads`,
    format: 'JSONEachRow',
  });

  // Step 2: Now connect to the target DB
  clickhouse = createClient({
    url: 'http://localhost:8123',
    username: 'default',
    password: '',
    database: 'domain_downloads', // Now it exists
  });

  // Create the 'downloads' table
  await clickhouse.query({
    query: `
      CREATE TABLE IF NOT EXISTS downloads (
        extracted_date Date,
        file_data String
      ) ENGINE = MergeTree()
      ORDER BY extracted_date
    `,
    format: 'JSONEachRow',
  });

  console.log("ClickHouse database and table are ready.");
}

// Save data into ClickHouse
async function saveDownloadData(fileLines) {
  const today = new Date().toISOString().split('T')[0];

  const rows = fileLines.map(line => ({
    extracted_date: today,
    file_data: line
  }));

  await clickhouse.insert({
    table: 'downloads',
    values: rows,
    format: 'JSONEachRow'
  });

  console.log(`✅ Saved ${fileLines.length} rows with extracted_date = ${today} to ClickHouse.`);
}

// Get most recently modified file
async function getLatestFile(directory) {
  const files = await fs.promises.readdir(directory);
  if (files.length === 0) return null;

  let latestFile = files[0];
  let latestTime = (await fs.promises.stat(path.join(directory, latestFile))).mtime;

  for (const file of files) {
    const filePath = path.join(directory, file);
    const stats = await fs.promises.stat(filePath);
    if (stats.mtime > latestTime) {
      latestTime = stats.mtime;
      latestFile = file;
    }
  }
  return path.join(directory, latestFile);
}

// Extract text from zip file and return an array of lines
async function extractTextFromZip(zipPath) {
  const directory = await unzipper.Open.file(zipPath);
  const textFiles = directory.files.filter(file => file.path.endsWith('.txt'));

  if (textFiles.length === 0) {
    throw new Error('No .txt files found in the ZIP.');
  }

  const file = textFiles[0];
  const content = await file.buffer(); // Buffer object

  const lines = content
    .toString('utf8')
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(line => line.length > 0);

  return lines;
}

// Scraper + Downloader + Save extracted text
async function runScraper() {
  await setupDatabase();

  const today = new Date().toISOString().split('T')[0];
  const browser = await puppeteer.launch({ headless: false });
  const page = await browser.newPage();
  const downloadPath = path.resolve('./downloads');
  fs.mkdirSync(downloadPath, { recursive: true });

  const client = await page.target().createCDPSession();
  await client.send('Page.setDownloadBehavior', {
    behavior: 'allow',
    downloadPath: downloadPath,
  });

  await page.goto('https://www.whoisdownload.com/newly-registered-domains', {
    waitUntil: 'networkidle2'
  });
  await page.waitForSelector('table');

  const downloadLink = await page.$$eval('table tbody tr', (rows, today) => {
    for (let row of rows) {
      const cells = row.querySelectorAll('td');
      if (cells.length > 2 && cells[2].innerText.trim() === today) {
        const button = row.querySelector('a.btn-success');
        if (button) {
          button.click();
          return button.href;
        }
      }
    }
    return null;
  }, today);

  if (downloadLink) {
    console.log('Download triggered for today:', today);
    await new Promise(resolve => setTimeout(resolve, 10000));

    const latestFilePath = await getLatestFile(downloadPath);
    if (latestFilePath && latestFilePath.endsWith('.zip')) {
      console.log('Latest file downloaded:', latestFilePath);

      try {
        const extractedText = await extractTextFromZip(latestFilePath);
        await saveDownloadData(extractedText);
      } catch (err) {
        console.error('Error extracting or saving ZIP content:', err);
      }

    } else {
      console.log('Downloaded file not found or is not a ZIP.');
    }
  } else {
    console.log('No download link found for today:', today);
  }

  console.log("Script executed successfully");
  await browser.close();
}

// Schedule the scraper (every minute for testing – change as needed)
cron.schedule('0 12 * * *', () => {
  console.log('Scheduled task started at', new Date().toLocaleString());
  runScraper().catch(error => console.error('Error running scraper:', error));
});

console.log('Scraper is scheduled to run daily at 12:00 PM.');
