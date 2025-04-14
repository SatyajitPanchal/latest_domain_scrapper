const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');
const cron = require('node-cron');
const mysql = require('mysql2/promise');
const unzipper = require('unzipper');

// MySQL credentials
const dbConfig = {
  host: 'localhost',
  user: 'root',
  password: 'root123',
  database: 'domain_downloads'
};

// Automatically create the database and table if not exist
async function setupDatabase() {
  const connection = await mysql.createConnection({
    host: dbConfig.host,
    user: dbConfig.user,
    password: dbConfig.password
  });

  await connection.query(`CREATE DATABASE IF NOT EXISTS \`${dbConfig.database}\`;`);
  console.log(`Database '${dbConfig.database}' is ready.`);

  await connection.changeUser({ database: dbConfig.database });

  const createTableSQL = `
    CREATE TABLE IF NOT EXISTS downloads (
      id INT AUTO_INCREMENT PRIMARY KEY,
      download_date DATE NOT NULL,
      file_data LONGTEXT NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `;
  await connection.execute(createTableSQL);
  console.log("Table 'downloads' is ready.");

  await connection.end();
}

// Save extracted text data into MySQL
async function saveDownloadData(fileText) {
  const connection = await mysql.createConnection(dbConfig);
  const today = new Date().toISOString().split('T')[0];
  const sql = `INSERT INTO downloads (download_date, file_data) VALUES (?, ?)`;
  await connection.execute(sql, [today, fileText]);
  console.log(`Saved extracted text for ${today}`);
  await connection.end();
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

// Extract text from zip file
async function extractTextFromZip(zipPath) {
  const directory = await unzipper.Open.file(zipPath);
  const textFiles = directory.files.filter(file => file.path.endsWith('.txt'));

  if (textFiles.length === 0) {
    throw new Error('No .txt files found in the ZIP.');
  }

  const file = textFiles[0];
  const content = await file.buffer(); // Buffer object
  return content.toString('utf8');     // Convert buffer to string
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

// Schedule the scraper (daily at 12:00 PM)
cron.schedule('0 12 * * *', () => {
  console.log('Scheduled task started at', new Date().toLocaleString());
  runScraper().catch(error => console.error('Error running scraper:', error));
});

console.log('Scraper is scheduled to run daily at 12:00 PM.');
