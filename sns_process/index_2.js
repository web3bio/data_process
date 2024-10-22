import { Connection, clusterApiUrl, PublicKey} from '@solana/web3.js';
import { getDomainKeySync, getIpfsRecord, getTwitterRecord, getRecords, getRecordV2Key, Record, NameRegistryState } from '@bonfida/spl-name-service';

// // Define your domain and subdomain here
// const domainName = 'bonfida'; // Without the .sol at the end
// const subDomain = 'dex.bonfida'; // Without the .sol at the end
// const record = 'IPFS.bonfida'; // Without the .sol at the end

const domainName = 'yoyodyne'; // Without the .sol at the end
const subDomain = 'dex.bonfida'; // Without the .sol at the end
// const record = 'IPFS.bonfida'; // Without the .sol at the end
const record = Record.Twitter + '.' + domainName

const connection = new Connection(clusterApiUrl('mainnet-beta'), 'confirmed');

// Function to retrieve domain information
async function getDomainInfo(domain) {
  try {
    // Step 1: Get the domain key
    const { pubkey } = getDomainKeySync(domain);
    // const pubkey = new PublicKey('DuG7m1M7rnyDP3ErUmtHrvcA8CfaKNT6HEc7Py9StVBU')

    // Step 2: Retrieve the domain registry information
    const { registry, nftOwner } = await NameRegistryState.retrieve(connection, pubkey);
    
    // Log the retrieved information
    console.log(`Domain: ${registry.name}`);
    console.log(`Public Key: ${pubkey.toBase58()}`);
    console.log(`NFT Owner Origin: ${nftOwner ? nftOwner : 'None'}`);
    console.log(`NFT Owner: ${nftOwner ? nftOwner.toBase58() : 'None'}`);
    console.log(`Registry:`, registry);
    console.log(`parentName:`, registry.parentName.toBase58());
    console.log(`Owner:`, registry.owner.toBase58());
    console.log(`class:`, registry.class.toBase58());
    console.log(`data:`, registry.data.toString());

  } catch (error) {
    console.error(`Error fetching domain info for ${domain}:`, error);
  }
}

// Function to retrieve subdomain and record information
async function getSubdomainAndRecordInfo(subDomain, record) {
  try {
    // Get subdomain key
    const { pubkey: subKey } = getDomainKeySync(subDomain);
    console.log(`Subdomain: ${subDomain}`);
    console.log(`Subdomain Public Key: ${subKey.toBase58()}`);

    // Get record key
    const records = await getRecords(connection, domainName, [Record.Twitter, Record.Telegram], true)
    records.forEach((item) => {
      console.log(`Record: ${item}`);
    });

  } catch (error) {
    console.error(`Error fetching subdomain or record info:`, error);
  }
}

// Main function to run the script
async function main() {
  await getDomainInfo(domainName);
  await getSubdomainAndRecordInfo(subDomain, record);
}

// Execute the main function
main().catch(error => console.error('Error in main execution:', error));
