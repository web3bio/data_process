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
import fs from "fs/promises";
import Bottleneck from 'bottleneck';


// Create a connection to the Solana devnet or mainnet
const SOLANA_MAIN_CLIENT = new Connection(clusterApiUrl('mainnet-beta'), 'confirmed');

// Set up bottleneck limiter to control request rate (2 requests per second, 30 requests per minute)
const limiter = new Bottleneck({
  minTime: 3000, // 1 request every 3 seconds
  maxConcurrent: 1, // Only allow 1 concurrent request at a time
  reservoir: 30,  // Limit to 30 requests per minute
  reservoirRefreshAmount: 30, // Refill 30 tokens
  reservoirRefreshInterval: 60 * 1000 // Refresh every minute (60000 ms)
});


const SOL_TLD = new PublicKey("58PwtjSDuFHuUkYjH9BYnnQKHfwo9reZhC2zMJv9JPkx"); // .sol TLD
const NAME_PROGRAM_ID = new PublicKey("namesLPneVptA9Z5rqUDD9tMTWEJwofgaYwp8cawRkX");


// fetchAllDomains
// dumps all the domains namenode
const fetchAllDomains = async () => {
  try {
    console.log("Fetching all registered .sol domains...");

    // Fetch all registered .sol domains using Bonfida's spl-name-service
    const registeredDomains = await getAllRegisteredDomains(SOLANA_MAIN_CLIENT);
  
    // Log the number of domains fetched
    console.log("Total domains fetched:", registeredDomains.length);

    // Prepare to save base58 addresses and corresponding names to domains.txt
    const domainsList = registeredDomains.map(domain => {
      const namenode = domain.pubkey; // The domain name base58 format
      return `${namenode}`;
    });

    // Save to domains.csv
    const filePath = "./data/domains.csv"; 
    await fs.writeFile(filePath, domainsList.join("\n"));
    console.log(`Domains successfully saved to ${filePath}`);

  } catch (error) {
    console.error("Error fetching domains:", error);
  }
};

// Dumps the domains list to domain
// fetchAllDomains();

const getDomainInfo = limiter.wrap(async (input) => {
  try {
    // domain key
    const { pubkey } = getDomainKeySync(input);
    // const pubkey = new PublicKey(input)

    const { registry, nftOwner } = await NameRegistryState.retrieve(SOLANA_MAIN_CLIENT, pubkey);
    
    const namenode = pubkey.toBase58();
    const nft_owner = nftOwner ? nftOwner.toBase58() : null;
    const is_tokenized = nftOwner ? true : false;
    const parent_node = registry.parentName.toBase58();
    const owner = registry.owner.toBase58();
    const resolver = NAME_PROGRAM_ID.toBase58();
    const resolved_address = registry.owner.toBase58();
    const contenthash = registry.data.toString();

    console.log("namenode:", namenode);
    console.log("is_tokenized:", is_tokenized);
    console.log("nft_owner:", nft_owner);
    console.log("parent_node:", parent_node);
    console.log("owner:", owner);
    console.log("resolver:", resolver);
    console.log("resolved_address:", resolved_address);
    console.log("contenthash:", contenthash);
  } catch (error) {
    console.error(`Error fetching domain error:`, error);
  }
});

const getTexts = limiter.wrap(async (domainName) => {
  try {
    const record_keys = [
      Record.IPNS,
      Record.IPFS,
      Record.ARWV,
      Record.SOL,
      Record.BTC,
      Record.LTC,
      Record.DOGE,
      Record.BSC,
      Record.Email,
      Record.Url,
      Record.Discord,
      Record.Reddit,
      Record.Twitter,
      Record.Telegram,
      Record.Pic,
      Record.SHDW,
      Record.POINT,
      Record.Injective,
      Record.Backpack
    ];

    const texts = {}; // json object
    const records = await getRecords(SOLANA_MAIN_CLIENT, domainName, record_keys, true);
    for (let i = 0; i < records.length; i++) {
      if (records[i] !== undefined) {
          if (record_keys[i]) {
              texts[record_keys[i].toLowerCase()] = records[i];
          }
      }
    }
    console.log("texts:", texts);

  } catch (error) {
    console.error(`Error fetching texts error:`, error);
  }
});

const getDomainKeysWithNames = limiter.wrap(async (wallet) => {
  try {
    const domainsWithReverses = await getDomainKeysWithReverses(connection, wallet);
    domainsWithReverses.forEach((domain) => {
      console.log(`Domain: ${domain.domain}, Public Key: ${domain.pubKey}`);
    });
  } catch (error) {
    console.error('Error fetching domain keys with names:', error);
  }
});

const getPrimaryDomains = limiter.wrap(async (wallets) => {
  try {
    const primaryDomains = await getMultiplePrimaryDomains(SOLANA_MAIN_CLIENT, wallets);
    primaryDomains.forEach((primary_name) => {
      console.log(`primaryDomain: ${primary_name}`);
    });
  } catch (error) {
    console.error('Error fetching primary domains:', error);
  }
});

const getPublicKeyFromSolDomain = limiter.wrap(async (domain) => {
  const { pubkey } = getDomainKeySync(domain);
  const owner = (await NameRegistryState.retrieve(SOLANA_MAIN_CLIENT, pubkey)).registry.owner.toBase58();
  console.log(`The owner of SNS Domain: ${domain} is: `,owner);
  return owner;
});

// const DOMAIN_TO_SEARCH = 'bonfida';
// getPublicKeyFromSolDomain(DOMAIN_TO_SEARCH);


const wallets = [
  new PublicKey("5k8SRiitUFPcUPLNB4eWwafXfYBP76iTx2P16xc99QYd"),
  new PublicKey("HKKp49qGWXd639QsuH7JiLijfVW5UtCVY4s1n2HANwEA"),
  new PublicKey("5WqqCfzNHhgLSkaR9QDbQnearFo616MP1EyxfZ69eED"),
  new PublicKey("1DWMUS1qEWrNWnCAiybqg1sp9m1YqTTnJTYXfQpvSgs"),
  new PublicKey("13L7R82JFtTpHeGKMs4Pnrh7ukYdPAPDUkSH7nZHL9A"),
  // Public Keys of all the wallet addresses you're looking up a primary domain for (up to 100)
];

getPrimaryDomains(wallets)
// getDomainInfo("5xQahoFAt4wF5iC1EDYtMwC6tpy9fih4uWfKSDwexxsR")
// getDomainInfo("9i8Q8GWK6s1XUkczHgit7P3hakfxTT6bppjbNSjdLuNC")
// getDomainInfo("Cfn1chAwJcL1F4tfqDMJRb4d1pSZCC5jMaQMpBK8cd7G")
// getDomainInfo("cuba")
// getDomainInfo("vaxa")
// getTexts("bonfida")
// getTexts("vaxa")