const puppeteer = require('puppeteer');
const { parse } = require('json2csv');
const winston = require('winston');
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

const LOCATION = 'us';
const MAX_RETRIES = 2;
const MAX_THREADS = 2;
const MAX_PAGES = 2;
const keywords = ['airpods', 'candles'];

const { api_key: API_KEY } = JSON.parse(fs.readFileSync('config.json', 'utf8'));

function getScrapeOpsUrl(url, location = LOCATION) {
    const params = new URLSearchParams({
        api_key: API_KEY,
        url,
        country: location,
        wait: 5000,
    });
    return `https://proxy.scrapeops.io/v1/?${params.toString()}`;
}

const logger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.printf(({ timestamp, level, message }) => {
            return `${timestamp} [${level.toUpperCase()}]: ${message}`;
        })
    ),
    transports: [
        new winston.transports.Console(),
        new winston.transports.File({ filename: 'walmart-scraper.log' })
    ]
});

class ProductURL {
    constructor(url) {
        this.url = this.validateUrl(url);
    }

    validateUrl(url) {
        if (typeof url === 'string' && url.trim().startsWith('https://www.walmart.com')) {
            return url.trim();
        }
        return 'Invalid URL';
    }
}

class ProductData {
    constructor(data = {}) {
        this.id = data.id || "Unknown ID";
        this.type = data.type || "Unknown Type";
        this.name = this.validateString(data.name, "No Name");
        this.brand = this.validateString(data.brand, "No Brand");
        this.averageRating = data.averageRating || 0;
        this.shortDescription = this.validateString(data.shortDescription, "No Description");
        this.thumbnailUrl = data.thumbnailUrl || "No Thumbnail URL";
        this.price = data.price || 0;
        this.currencyUnit = data.currencyUnit || "USD";
    }

    validateString(value, fallback) {
        if (typeof value === 'string' && value.trim() !== '') {
            return value.trim();
        }
        return fallback;
    }
}

class DataPipeline {
    constructor(csvFilename, storageQueueLimit = 50) {
        this.namesSeen = new Set();
        this.storageQueue = [];
        this.storageQueueLimit = storageQueueLimit;
        this.csvFilename = csvFilename;
    }

    async saveToCsv() {
        const filePath = path.resolve(this.csvFilename);
        const dataToSave = this.storageQueue.splice(0, this.storageQueue.length);
        if (dataToSave.length === 0) return;

        const csvData = parse(dataToSave, { header: !fs.existsSync(filePath) });
        fs.appendFileSync(filePath, csvData + '\n', 'utf8');
    }

    isDuplicate(title) {
        if (this.namesSeen.has(title)) {
            logger.warn(`Duplicate item found: ${title}. Item dropped.`);
            return true;
        }
        this.namesSeen.add(title);
        return false;
    }

    async addData(data) {
        if (!this.isDuplicate(data.title)) {
            this.storageQueue.push(data);
            if (this.storageQueue.length >= this.storageQueueLimit) {
                await this.saveToCsv();
            }
        }
    }

    async closePipeline() {
        if (this.storageQueue.length > 0) await this.saveToCsv();
    }
}

async function scrapeProductUrls(dataPipeline, keyword, maxPages = MAX_PAGES, retries = MAX_RETRIES) {
    const browser = await puppeteer.launch({ headless: true });
    const page = await browser.newPage();

    while (retries > 0) {
        try {
            for (let pageNum = 1; pageNum <= maxPages; pageNum++) {
                const url = `https://www.walmart.com/search?q=${encodeURIComponent(keyword)}&sort=best_seller&page=${pageNum}&affinityOverride=default`;
                logger.info(`Fetching: ${url}`);

                await page.goto(getScrapeOpsUrl(url), { waitUntil: 'domcontentloaded' });

                const data = await page.evaluate(() => {
                    const scriptTag = document.querySelector('#__NEXT_DATA__');
                    if (scriptTag) {
                        return JSON.parse(scriptTag.textContent);
                    }
                    return null;
                });

                if (data) {
                    const items = data.props?.pageProps?.initialData?.searchResult?.itemStacks?.[0]?.items || [];
                    for (const item of items) {
                        const url = new ProductURL('https://www.walmart.com' + (item.canonicalUrl || '').split('?')[0]);
                        await dataPipeline.addData({ title: item.name, url: url.url });
                    }

                    // Break the loop if no products are found
                    if (items.length === 0) {
                        logger.info('No more products found.');
                        break;
                    }
                } else {
                    logger.warn('No data found on this page.');
                }
            }
        } catch (error) {
            logger.error(`Error during scraping: ${error.message}`);
            retries--;

            if (retries > 0) {
                logger.info('Retrying...');
                await new Promise(resolve => setTimeout(resolve, 2000));
            }
        } finally {
            await browser.close();
            await dataPipeline.closePipeline();
        }
    }
}

async function scrapeProductData(dataPipeline, url, retries = MAX_RETRIES) {
    const browser = await puppeteer.launch({ headless: true });
    const page = await browser.newPage();

    while (retries > 0) {
        try {
            logger.info(`Fetching: ${url}`);
            await page.goto(getScrapeOpsUrl(url), { waitUntil: 'domcontentloaded' });

            const productData = await page.evaluate(() => {
                const scriptTag = document.querySelector('#__NEXT_DATA__');
                if (scriptTag) {
                    const jsonBlob = JSON.parse(scriptTag.textContent);
                    const rawProductData = jsonBlob.props?.pageProps?.initialData?.data?.product;

                    if (rawProductData) {
                        return {
                            id: rawProductData.id,
                            type: rawProductData.type,
                            name: rawProductData.name,
                            brand: rawProductData.brand,
                            averageRating: rawProductData.averageRating,
                            shortDescription: rawProductData.shortDescription,
                            thumbnailUrl: rawProductData.imageInfo?.thumbnailUrl,
                            price: rawProductData.priceInfo?.currentPrice?.price,
                            currencyUnit: rawProductData.priceInfo?.currentPrice?.currencyUnit,
                        };
                    }
                }
                return null;
            });

            if (productData) {
                await dataPipeline.addData(new ProductData(productData));
            } else {
                logger.info(`No product data found for ${url}`);
            }
            break;
        } catch (error) {
            logger.error(`Error scraping ${url}. Retries left: ${retries}`, error);
            retries--;

            if (retries > 0) {
                logger.info('Retrying...');
                await new Promise(resolve => setTimeout(resolve, 2000));
            }
        }
    }

    await browser.close();
}

async function readCsvAndGetUrls(csvFilename) {
    return new Promise((resolve, reject) => {
        const urls = [];
        fs.createReadStream(csvFilename)
            .pipe(csv())
            .on('data', (row) => {
                if (row.url) urls.push(row.url);
            })
            .on('end', () => resolve(urls))
            .on('error', reject);
    });
}

const scrapeConcurrently = async (tasks, maxConcurrency) => {
    const results = [];
    const executing = new Set();

    for (const task of tasks) {
        const promise = task().then(result => {
            executing.delete(promise);
            return result;
        });
        executing.add(promise);
        results.push(promise);

        if (executing.size >= maxConcurrency) {
            await Promise.race(executing);
        }
    }

    return Promise.all(results);
};

async function getAllUrlsFromFiles(files) {
    const urls = [];
    for (const file of files) {
        urls.push({ filename: file, urls: await readCsvAndGetUrls(file) });
    }
    return urls;
}

(async () => {
    logger.info("Started Scraping Search Results")
    const aggregateFiles = [];

    const scrapeProductUrlsTasks = keywords.map(keyword => async () => {
        const csvFilename = `${keyword.replace(/\s+/g, '-').toLowerCase()}-urls.csv`;
        const dataPipeline = new DataPipeline(csvFilename);
        await scrapeProductUrls(dataPipeline, keyword);
        await dataPipeline.closePipeline();
        aggregateFiles.push(csvFilename);
    });

    await scrapeConcurrently(scrapeProductUrlsTasks, MAX_THREADS);

    const urls = await getAllUrlsFromFiles(aggregateFiles);

    const scrapeProductDataTasks = urls.flatMap(data =>
        data.urls.map(url => async () => {
            const filename = data.filename.replace('urls', 'product-data');
            const dataPipeline = new DataPipeline(filename);
            await scrapeProductData(dataPipeline, url);
            await dataPipeline.closePipeline();
        })
    );

    await scrapeConcurrently(scrapeProductDataTasks, MAX_THREADS);

    logger.info('Scraping completed.');
})();
