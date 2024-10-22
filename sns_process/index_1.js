// import pkg from "@solana/web3.js";
// const { Connection, clusterApiUrl, PublicKey } = pkg;

// // Log the PublicKey input to verify it is correct
// const SOL_TLD = new PublicKey("58PwtjSDuFHuUkYjH9BYnnQKHfwo9reZhC2zMJv9JPkx"); // .sol TLD
// console.log(`Root Domain PublicKey: ${SOL_TLD}`);
// const NAME_PROGRAM_ID = new PublicKey("namesLPneVptA9Z5rqUDD9tMTWEJwofgaYwp8cawRkX");


// // Add a delay to avoid overloading the call stack
// const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// // Function to get slices of registered domains
// const getAllDomainSlices = async (connection, offset, length) => {
//   const filters = [
//     {
//       memcmp: {
//         offset: 0,
//         bytes: SOL_TLD.toBase58(),
//       },
//     },
//   ];

//   const dataSlice = { offset, length };

//   const accounts = await connection.getProgramAccounts(NAME_PROGRAM_ID, {
//     dataSlice,
//     filters,
//   });

//   return accounts;
// };

// // Fetch all domains by slicing data in chunks with proper flow control
// const fetchAllDomains = async () => {
//   const connection = new Connection(clusterApiUrl("mainnet-beta"));
//   const sliceSize = 1; // Size of each slice
//   const maxDataLength = 32; // Estimated max length of domain data
//   const delayBetweenRequests = 100; // Delay in milliseconds to avoid overload

//   let allDomains = [];

//   try {
//     for (let i = 0; i < maxDataLength; i += sliceSize) {
//       console.log(`Fetching slice from offset: ${i} to ${i + sliceSize}`);

//       const domainSlice = await getAllDomainSlices(connection, i, sliceSize);

//       // If no more data, stop fetching
//       if (domainSlice.length === 0) {
//         console.log("No more domains to fetch, stopping.");
//         break;
//       }

//       allDomains.push(...domainSlice);

//       // Add delay to prevent overwhelming the system
//       await delay(delayBetweenRequests);
//     }

//     console.log("Total domains fetched:", allDomains.length);
//     console.log("Domains:", allDomains);
//   } catch (error) {
//     console.error("Error fetching domains:", error);
//   }
// };

// // Run the fetch function
// fetchAllDomains();

import { Connection, clusterApiUrl, PublicKey} from '@solana/web3.js';
import { getAllRegisteredDomains } from '@bonfida/spl-name-service';


// Given uint8 array
const uint8Array = [
  0,
  6,
  3,
  37,
  156,
  186,
  47,
  178,
  159,
  88,
  193,
  124,
  39,
  135,
  173,
  49,
  227,
  251,
  240,
  125,
  235,
  84,
  233,
  214,
  199,
  28,
  27,
  116,
  220,
  40,
  157,
  36
];

// Convert to string using TextDecoder
const decoder = new TextDecoder('utf-8'); // Specify the encoding if needed
const byteArray = new Uint8Array(uint8Array); // Create a Uint8Array
const decodedString = decoder.decode(byteArray); // Decode the byte array

console.log(decodedString);

const fetchAllDomains = async () => {
    const connection = new Connection(clusterApiUrl("mainnet-beta")); // Solana mainnet connection

    try {
      console.log("Fetching all registered .sol domains...");

      // Fetch all registered .sol domains using Bonfida's spl-name-service
      const registeredDomains = await getAllRegisteredDomains(connection);
    
      // Log the number of domains fetched
      console.log("Total domains fetched:", registeredDomains.length);

      // Prepare to save base58 addresses and corresponding names to domains.txt
      const domainsList = registeredDomains.map(domain => {
        const pubkey = domain.pubkey; // The base58 address
        const dataBuffer = domain.account.data; // The data buffer
      
        // Convert the data buffer to a string (you can customize this conversion)
        const name = bufferToString(dataBuffer);

        return `${pubkey}, ${name}`;
      });

      // Save to domains.txt
      const filePath = "./data/domains.csv"; 
      await fs.writeFile(filePath, domainsList.join("\n"));
      console.log(`Domains successfully saved to ${filePath}`);

    } catch (error) {
      console.error("Error fetching domains:", error);
    }
  };

  // Example function to convert a Buffer to a name string
  const bufferToString = (dataBuffer) => {
    return dataBuffer.toString('utf-8').replace(/\0/g, ''); // Convert to string and remove null characters
  };

  // Run the fetch function
  fetchAllDomains();