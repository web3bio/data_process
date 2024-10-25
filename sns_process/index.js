import { Connection, clusterApiUrl, PublicKey } from '@solana/web3.js';
import { getAllDomains,
    getDomainKeysWithReverses,
    getAllRegisteredDomains,
    NameRegistryState,
    getRecordV2Key,
    Record,
    getRecords,
    getDomainKeySync,
    getMultiplePrimaryDomains
  } from '@bonfida/spl-name-service';
import pgp from 'pg-promise';
import dotenv from 'dotenv';
import fs from 'fs';
import readline from 'readline';
import Bottleneck from 'bottleneck';
import dayjs from 'dayjs';

// Load environment variables from the .env file
dotenv.config();


const pg = pgp();

// PostgreSQL connection using DSN
const db = pg({
    connectionString: process.env.PG_DSN,
    ssl: {
        rejectUnauthorized: false, // Accept self-signed certificates
    },
});

const SOLANA_MAIN_CLIENT = new Connection(process.env.QUICKNODE_RPC);

// Set up bottleneck limiter to control request rate (2 requests per second, 30 requests per minute)
// const limiter = new Bottleneck({
//     minTime: 3000, // 1 request every 3 seconds
//     maxConcurrent: 1, // Only allow 1 concurrent request at a time
//     reservoir: 30,  // Limit to 30 requests per minute
//     reservoirRefreshAmount: 30, // Refill 30 tokens
//     reservoirRefreshInterval: 60 * 1000 // Refresh every minute (60000 ms)
// });

// const limiter = new Bottleneck({
//     minTime: 100, // 1 request every 100 milliseconds (10 requests per second)
//     maxConcurrent: 1, // Only allow 1 concurrent request at a time
//     reservoir: 10,  // Limit to 10 requests per second
//     reservoirRefreshAmount: 10, // Refill 10 tokens (requests)
//     reservoirRefreshInterval: 1000 // Refresh every 1 second (1000 ms)
// });

const limiter = new Bottleneck({
    minTime: 200, // 1 request every 200 milliseconds (5 requests per second)
    maxConcurrent: 1, // Only allow 1 concurrent request at a time
    reservoir: 5,  // Limit to 5 requests per second
    reservoirRefreshAmount: 5, // Refill 5 tokens (requests)
    reservoirRefreshInterval: 1000 // Refresh every 1 second (1000 ms)
});

const SOL_TLD = new PublicKey("58PwtjSDuFHuUkYjH9BYnnQKHfwo9reZhC2zMJv9JPkx"); // .sol TLD
const NAME_PROGRAM_ID = new PublicKey("namesLPneVptA9Z5rqUDD9tMTWEJwofgaYwp8cawRkX");
const solanaZeroAddress = "11111111111111111111111111111111";


// fetchAllDomains
// dumps all the domains namenode
// Fetch all registered .sol domains
const fetchAllDomains = async () => {
    try {
        console.log("Fetching all registered .sol domains...");
        const connection = new Connection(clusterApiUrl('mainnet-beta'), 'confirmed');
        const registeredDomains = await getAllRegisteredDomains(connection);
        console.log("Total domains fetched:", registeredDomains.length);
        const domainsList = registeredDomains.map(domain => domain.pubkey);
        const filePath = "./data/domains.csv"; 
        await fs.promises.writeFile(filePath, domainsList.join("\n"));
        console.log(`Domains successfully saved to ${filePath}`);
    } catch (error) {
        console.error("Error fetching domains:", error);
    }
};

// fetchDomainsAndUpsert
// fetch all namenode(sns.id pubkey) owner/nft_owner/content
// this pipeline can not get name
async function fetchDomainsAndUpsert() {
    try {
        console.log("Connected to PostgreSQL");

        const batchSize = 1000;
        let allCount = 0;
        let batchCount = 0;
        let lastId = 0;
        let hasMoreRows = true;

        while (hasMoreRows) {
            // Fetch 1000 rows from the database where owner is NULL and sorted by id
            const query = `
                SELECT id, namenode 
                FROM sns_profile 
                WHERE owner IS NULL 
                AND id > $1
                ORDER BY id ASC 
                LIMIT $2`;

            const batch = await db.any(query, [lastId, batchSize]);

            if (batch.length > 0) {
                for (const row of batch) {
                    // Call getDomainInfo and fetch domain details
                    const domainInfo = await retryGetDomainInfo(row.namenode);
                    if (domainInfo) {
                        allCount++;
                        // Prepare the SQL upsert query with the fetched domain details
                        const insertQuery = `
                            INSERT INTO sns_profile (namenode, nft_owner, is_tokenized, parent_node, expire_time, owner, resolver, resolved_address, contenthash)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                            ON CONFLICT (namenode) DO UPDATE
                            SET nft_owner = EXCLUDED.nft_owner,
                                is_tokenized = EXCLUDED.is_tokenized,
                                parent_node = EXCLUDED.parent_node,
                                expire_time = EXCLUDED.expire_time,
                                owner = EXCLUDED.owner,
                                resolver = EXCLUDED.resolver,
                                resolved_address = EXCLUDED.resolved_address,
                                contenthash = EXCLUDED.contenthash;`;
        
                        // Perform the upsert operation
                        await db.none(insertQuery, [
                            domainInfo.namenode,
                            domainInfo.nft_owner,
                            domainInfo.is_tokenized,
                            domainInfo.parent_node,
                            domainInfo.expire_time,
                            domainInfo.owner,
                            domainInfo.resolver,
                            domainInfo.resolved_address,
                            domainInfo.contenthash
                        ]);
                    
                        console.log(`${allCount} upserted.`);
                        console.log(`Upserted domain: ${domainInfo.namenode}`);
                    } else {
                        console.log(`Failed to fetch domain info for ${row.namenode}. Skipping.`);
                    }
                }

                // Update the last processed ID
                lastId = batch[batch.length - 1].id;

                batchCount++;
                console.log(`Batch ${batchCount} upserted.`);
            }

            // Check if we fetched less than batchSize, which means no more rows left
            if (batch.length < batchSize) {
                hasMoreRows = false;
            }
        }

        console.log(`All batches completed. Batch count: ${batchCount}`);
    } catch (error) {
        console.error('Error:', error);
    } finally {
        console.log("Disconnected from PostgreSQL");
    }
}

async function fetchDomainsByOwnersAndUpsert() {
    try {
        console.log("Connected to PostgreSQL");

        const batchSize = 1000;
        let allCount = 0;
        let batchCount = 0;
        let lastId = 0;
        let hasMoreRows = true;
        // Use a Set to filter out duplicate owners
        const processedOwners = new Set();

        while (hasMoreRows) {
            // Fetch 1000 rows from the database where name is NULL and sorted by id
            // owner != '11111111111111111111111111111111'
            const query = `
                SELECT id, owner 
                FROM sns_profile 
                WHERE name IS NULL AND owner != '11111111111111111111111111111111'
                AND id > $1
                ORDER BY id ASC 
                LIMIT $2`;

            const batch = await db.any(query, [lastId, batchSize]);
            if (batch.length > 0) {
                for (const row of batch) {
                    const owner = row.owner;
                    if (processedOwners.has(owner)) {
                        console.log(`processedOwners Ignore Owner(${owner}) has already been fetched`);
                        continue;
                    }
                    // Call getDomainsWithWallet and fetch domain name details
                    const domainDetails = await retryGetDomainsWithWallet(owner);
                    processedOwners.add(owner);
                    if (domainDetails.length > 0) {
                        allCount += domainDetails.length
                        const insertQuery = `
                            INSERT INTO sns_profile (namenode, name, label_name, parent_node, expire_time, owner, resolver, resolved_address, update_time)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                            ON CONFLICT (namenode) DO UPDATE
                            SET name = EXCLUDED.name,
                                label_name = EXCLUDED.label_name,
                                parent_node = EXCLUDED.parent_node,
                                expire_time = EXCLUDED.expire_time,
                                owner = EXCLUDED.owner,
                                resolver = EXCLUDED.resolver,
                                resolved_address = EXCLUDED.resolved_address,
                                update_time = EXCLUDED.update_time;`;
                        
                        for (const domainDetail of domainDetails) {
                            await db.none(insertQuery, [
                                domainDetail.namenode,
                                domainDetail.name,
                                domainDetail.label_name,
                                domainDetail.parent_node,
                                domainDetail.expire_time,
                                domainDetail.owner,
                                domainDetail.resolver,
                                domainDetail.resolved_address,
                                domainDetail.update_time
                            ]);
                        }
                        console.log(`${allCount} upserted.`);
                        console.log(`Upserted owner domains: ${row.owner}, ${domainDetails.length} records`);
                    } else {
                        console.log(`Failed to fetch owner domains for ${row.owner}. Skipping.`);
                    }
                }
                // Update the last processed ID
                lastId = batch[batch.length - 1].id;

                batchCount++;
                console.log(`Batch ${batchCount} upserted.`);
            }
            // Check if we fetched less than batchSize, which means no more rows left
            if (batch.length < batchSize) {
                hasMoreRows = false;
            }
        }

    } catch (error) {
        console.error('Error:', error);
    } finally {
        console.log("Disconnected from PostgreSQL");
    }
}


async function processAndUpsertBatchForloop(batch) {
    try {
        for (const domain_pubkey of batch) {
            // Call getDomainInfo and fetch domain details
            const domainInfo = await getDomainInfo(domain_pubkey);

            // Prepare the SQL upsert query with the fetched domain details
            const insertQuery = `
                INSERT INTO sns_profile (namenode, nft_owner, is_tokenized, parent_node, expire_time, owner, resolver, resolved_address, contenthash)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (namenode) DO UPDATE
                SET nft_owner = EXCLUDED.nft_owner,
                    is_tokenized = EXCLUDED.is_tokenized,
                    parent_node = EXCLUDED.parent_node,
                    expire_time = EXCLUDED.expire_time,
                    owner = EXCLUDED.owner,
                    resolver = EXCLUDED.resolver,
                    resolved_address = EXCLUDED.resolved_address,
                    contenthash = EXCLUDED.contenthash;`;

            // Perform the upsert operation
            await db.none(insertQuery, [
                domainInfo.namenode,
                domainInfo.nft_owner,
                domainInfo.is_tokenized,
                domainInfo.parent_node,
                domainInfo.expire_time,
                domainInfo.owner,
                domainInfo.resolver,
                domainInfo.resolved_address,
                domainInfo.contenthash
            ]);

            console.log(`Upserted domain: ${domainInfo.namenode}`);
        }
        console.log('Batch processed successfully.');
    } catch (error) {
        console.error('Error in processAndUpsertBatch:', error);
    }
}

async function readDomainsAndUpsert() {
    try {
        console.log("Connected to PostgreSQL");

        const filePath = './data/domains.csv';
        const fileStream = fs.createReadStream(filePath);
        
        const rl = readline.createInterface({
            input: fileStream,
            crlfDelay: Infinity // Recognize CR LF sequences
        });

        const batchSize = 1000;
        let batch = [];
        let batchCount = 0;

        for await (const line of rl) {
            if (line.trim() !== '') {
                batch.push(line.trim());
                if (batch.length === batchSize) {
                    await processAndUpsertBatchForloop(batch);
                    batchCount++;
                    console.log(`Batch ${batchCount} upserted.`);
                    batch = [];
                }
            }
        }

        // remaining lines
        if (batch.length > 0) {
            await processAndUpsertBatchForloop(batch);
            batchCount++;
            console.log(`Batch ${batchCount} upserted.`);
        }

        console.log(`All batches completed. Batch count: ${batchCount}`);
    } catch (error) {
        console.error('Error:', error);
    } finally {
        console.log("Disconnected from PostgreSQL");
    }
}

// Function to upsert a batch of domains using pg-promise
async function upsertBatch_Old(batch) {
    const insertQuery = `
        INSERT INTO sns_profile (namenode)
        VALUES ${batch.map((_, index) => `($${index + 1})`).join(', ')}
        ON CONFLICT (namenode) DO UPDATE
        SET namenode = EXCLUDED.namenode;`;

    await db.none(insertQuery, batch);
}

async function processAndUpsertBatchConcurrent(batch) {
    const processedBatch = await Promise.all(batch.map(async (domain_pubkey) => {
        const domainInfo = await getDomainInfo(domain_pubkey);
        if (domainInfo) {
            return [
                domainInfo.namenode,
                domainInfo.nft_owner,
                domainInfo.is_tokenized,
                domainInfo.parent_node,
                domainInfo.expire_time,
                domainInfo.owner,
                domainInfo.resolver,
                domainInfo.resolved_address,
                domainInfo.contenthash
            ];
        } else {
            return null;
        }
    }));

    const validBatch = processedBatch.filter(Boolean); // Filter out null entries

    if (validBatch.length > 0) {
        await upsertBatch(validBatch);
    }
}

async function upsertBatch(batch) {
    const insertQuery = `
        INSERT INTO sns_profile 
        (namenode, nft_owner, is_tokenized, parent_node, expire_time, owner, resolver, resolved_address, contenthash)
        VALUES ${batch.map((_, index) => `($${index * 8 + 1}, $${index * 8 + 2}, $${index * 8 + 3}, $${index * 8 + 4}, $${index * 8 + 5}, $${index * 8 + 6}, $${index * 8 + 7}, $${index * 8 + 8}, $${index * 8 + 9})`).join(', ')}
        ON CONFLICT (namenode) DO UPDATE
        SET nft_owner = EXCLUDED.nft_owner,
            is_tokenized = EXCLUDED.is_tokenized,
            parent_node = EXCLUDED.parent_node,
            expire_time = EXCLUDED.expire_time,
            owner = EXCLUDED.owner,
            resolver = EXCLUDED.resolver,
            resolved_address = EXCLUDED.resolved_address,
            contenthash = EXCLUDED.contenthash;
    `;

    const flattenedValues = batch.flat(); // Flatten batch array for parameterized query
    await db.none(insertQuery, flattenedValues);
}

async function retryGetDomainInfo(domain_pubkey, retries = 3) {
    for (let attempt = 0; attempt < retries; attempt++) {
        try {
            return await getDomainInfo(domain_pubkey);
        } catch (error) {
            console.error(`Attempt ${attempt + 1} failed for ${domain_pubkey}:`, error);
        }

        // Wait for 3 seconds before retrying
        await new Promise(resolve => setTimeout(resolve, 3000));
    }

    // If all retries fail, return the fallback solanaZeroAddress info
    return {
        namenode: domain_pubkey,
        nft_owner: null,
        is_tokenized: false,
        parent_node: SOL_TLD.toBase58(), // Default parent_node
        expire_time: null,
        owner: solanaZeroAddress, // Solana zero address
        resolver: null,
        resolved_address: null,
        contenthash: null,
    };
}


const getDomainInfo = limiter.wrap(async (domain_pubkey) => {
    try {
        const pubkey = new PublicKey(domain_pubkey);
        const { registry, nftOwner } = await NameRegistryState.retrieve(SOLANA_MAIN_CLIENT, pubkey);
        let contenthash = registry.data.toString('utf-8').trim();
        contenthash = contenthash.replace(/\x00+$/, '');

        // Further clean up invalid or problematic characters if necessary
        if (contenthash === '' || /^[\x00]+$/.test(contenthash)) {
            contenthash = null;
        } else {
            contenthash = contenthash.replace(/[\0\x00]+/g, ''); // Remove any lingering null bytes
        }

        return {
            namenode: pubkey.toBase58(),
            nft_owner: nftOwner ? nftOwner.toBase58() : null,
            is_tokenized: !!nftOwner,
            parent_node: registry.parentName.toBase58(),
            expire_time: "2116-09-24 09:30:00",
            owner: registry.owner.toBase58(),
            resolver: NAME_PROGRAM_ID.toBase58(),
            resolved_address: registry.owner.toBase58(),
            contenthash: contenthash,
        };
    } catch (error) {
        console.error(`Error fetching domain info for ${domain_pubkey}:`, error);
        throw error;
    }
});


async function retryGetDomainsWithWallet(wallet, retries = 3) {
    for (let attempt = 0; attempt < retries; attempt++) {
        try {
            return await getDomainsWithWallet(wallet);
        } catch (error) {
            console.error(`Attempt ${attempt + 1} failed for wallet ${wallet}:`, error);
        }

        // Wait for 3 seconds before retrying
        await new Promise(resolve => setTimeout(resolve, 3000));
    }

    // If all retries fail, return []
    return [];
}

const getDomainsWithWallet = limiter.wrap(async (wallet) => {
    try {
        const walletPubkey = new PublicKey(wallet);
        const domainsWithReverses = await getDomainKeysWithReverses(SOLANA_MAIN_CLIENT, walletPubkey);
        let domains = [];

        domainsWithReverses.forEach((domain) => {
            console.log(`Domain: ${domain.domain}, Public Key: ${domain.pubKey}`);

            const formattedNow = dayjs().format('YYYY-MM-DD HH:mm:ss');

            domains.push({
                namenode: domain.pubKey.toBase58(),
                name: domain.domain + ".sol",
                label_name: domain.domain,
                parent_node: SOL_TLD.toBase58(), // Ensure SOL_TLD is defined
                expire_time: "2116-09-24 09:30:00", // Placeholder expire time
                owner: walletPubkey.toBase58(),
                resolver: NAME_PROGRAM_ID.toBase58(), // Ensure NAME_PROGRAM_ID is defined
                resolved_address: walletPubkey.toBase58(),
                update_time: formattedNow, // Current time in 'yyyy-MM-dd HH:mm:ss' format
            });
        });

        return domains;
    } catch (error) {
        console.error(`Error fetching domains with wallet ${wallet}:`, error);
        throw error;
    }
});

const run = async () => {
    // await fetchAllDomains();
    // await readDomainsAndUpsert();
    // await fetchDomainsAndUpsert();
    await fetchDomainsByOwnersAndUpsert();
};

// Execute the run function
run().catch(console.error);

// const domains = await retryGetDomainsWithWallet("HKKp49qGWXd639QsuH7JiLijfVW5UtCVY4s1n2HANwEA")
// console.log(domains);

// const result = await retryGetDomainInfo("7bPjsXHCTfpxE7mD7UPcWepk5LrubLo9ctMV5HfJNY1e")
// console.log(result.owner);
// console.log(result.contenthash);